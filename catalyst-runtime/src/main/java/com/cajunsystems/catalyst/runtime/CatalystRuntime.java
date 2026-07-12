package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionInfo;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.ReplayMode;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.Cost;
import com.cajunsystems.catalyst.engine.BranchSpec;
import com.cajunsystems.catalyst.engine.CostModel;
import com.cajunsystems.catalyst.engine.ExecutionFailedException;
import com.cajunsystems.catalyst.engine.ExecutionPausedSignal;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.Hashing;
import com.cajunsystems.catalyst.engine.InDoubtPolicy;
import com.cajunsystems.catalyst.engine.NonDeterministicReplayException;
import com.cajunsystems.catalyst.engine.PayloadCodec;
import com.cajunsystems.catalyst.engine.Reducer;
import com.cajunsystems.catalyst.engine.ReplayingContext;
import com.cajunsystems.catalyst.engine.TimelineStep;
import com.cajunsystems.catalyst.engine.Trajectory;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.EventJson;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.model.Model;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The Catalyst runtime: owns scheduling (virtual threads), event persistence, resume and lifecycle
 * (spec §4, §7). It is thin — durability lives in the {@link EventLog} and the record/substitute
 * semantics live in {@link ReplayingContext}. This class wires them together and manages the
 * lifecycle events around a task run.
 */
public final class CatalystRuntime implements AutoCloseable {

    private final EventLog log;
    private final Model defaultModel;
    private final Clock clock;
    private final ReplayMode replayMode;
    private final InDoubtPolicy inDoubtPolicy;
    private final CostModel costModel;
    private final ObjectMapper eventMapper;
    private final PayloadCodec payloads;
    private final String nodeId;
    private final ExecutorService executor;

    /**
     * Attempts currently running in this process, keyed by execution. Guards against scheduling a
     * second concurrent attempt for the same execution (which would interleave events and corrupt the
     * stream): a re-submission of a still-running execution attaches to the in-flight attempt instead.
     */
    private final ConcurrentHashMap<ExecutionId, CompletableFuture<?>> inFlight = new ConcurrentHashMap<>();

    private enum Mode { FRESH, RESUME, REPLAY_TERMINAL }

    private CatalystRuntime(Builder b) {
        this.log = b.log;
        this.defaultModel = b.model;
        this.clock = b.clock;
        this.replayMode = b.replayMode;
        this.inDoubtPolicy = b.inDoubtPolicy;
        this.costModel = b.costModel;
        this.eventMapper = b.eventMapper;
        this.payloads = new PayloadCodec();
        this.nodeId = b.nodeId;
        this.executor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("catalyst-exec-", 0).factory());
    }

    public static Builder builder() {
        return new Builder();
    }

    // ── Runtime API (spec §7) ────────────────────────────────────────────────

    /** Starts a new execution, or attaches/resumes an existing one when an idempotency key matches. */
    public synchronized <R> ExecutionHandle<R> execute(Task<R> task, ExecutionOptions opts) {
        String taskType = task.getClass().getName();
        Optional<String> key = opts.idempotencyKey();

        ExecutionId id;
        Mode mode;
        if (key.isPresent()) {
            Optional<ExecutionId> existing = log.findByKey(key.get());
            if (existing.isPresent()) {
                id = existing.get();
                // If an attempt for this execution is already running in this process, attach to it
                // rather than scheduling a duplicate attempt that would corrupt the event stream.
                CompletableFuture<?> running = inFlight.get(id);
                if (running != null) {
                    return new FutureExecutionHandle<>(id, cast(running));
                }
                ExecutionState state = Reducer.fold(id, log.read(id));
                mode = state.isTerminal() ? Mode.REPLAY_TERMINAL : Mode.RESUME;
            } else {
                id = ExecutionId.random();
                log.putKey(key.get(), id);
                createExecution(id, taskType, key.get(), opts);
                mode = Mode.FRESH;
            }
        } else {
            id = ExecutionId.random();
            createExecution(id, taskType, "", opts);
            mode = Mode.FRESH;
        }

        final ExecutionId execId = id;
        final Mode runMode = mode;
        CompletableFuture<R> future = new CompletableFuture<>();
        inFlight.put(execId, future);
        future.whenComplete((r, t) -> inFlight.remove(execId, future));
        executor.execute(() -> runAttempt(task, execId, taskType, opts, runMode, future));
        return new FutureExecutionHandle<>(execId, future);
    }

    @SuppressWarnings("unchecked")
    private static <R> CompletableFuture<R> cast(CompletableFuture<?> future) {
        return (CompletableFuture<R>) future;
    }

    public <R> ExecutionHandle<R> execute(Task<R> task) {
        return execute(task, ExecutionOptions.none());
    }

    /** Synchronous sugar for scripts. */
    public <R> R executeAndWait(Task<R> task) {
        return execute(task, ExecutionOptions.none()).result();
    }

    public <R> R executeAndWait(Task<R> task, ExecutionOptions opts) {
        return execute(task, opts).result();
    }

    /** Folded state plus the typed timeline for an execution. */
    public ExecutionState inspect(ExecutionId id) {
        return Reducer.fold(id, log.read(id));
    }

    /**
     * Exact re-fold of an execution's events (spec §7): a pure function of the log, zero external
     * calls. Use {@link #replay(ExecutionId, Task)} to also re-run the task and verify determinism.
     */
    public ExecutionState replay(ExecutionId id) {
        return inspect(id);
    }

    /**
     * Exact replay with substitution (spec §7): re-runs {@code task} against the recorded log with
     * every boundary substituted and <strong>zero external calls</strong> (any attempt to execute a
     * boundary not covered by the log fails). Under {@link ReplayMode#STRICT} a boundary whose
     * canonical hash/identity differs from the record raises {@link NonDeterministicReplayException}.
     * Appends nothing; returns the folded {@link ExecutionState}.
     *
     * @throws NonDeterministicReplayException if the task diverges from the recorded run
     */
    public <R> ExecutionState replay(ExecutionId id, Task<R> task) {
        return replay(id, task, ExecutionOptions.none());
    }

    /**
     * As {@link #replay(ExecutionId, Task)}, but with the task input variables the original execution
     * ran with. Vars are not stored in the log (only their {@code argsHash} is), so a task that
     * branches on {@code ctx.var(...)} must be replayed with the same {@code opts} it was executed
     * with; otherwise it takes a different path and (correctly) trips
     * {@link NonDeterministicReplayException}.
     */
    public <R> ExecutionState replay(ExecutionId id, Task<R> task, ExecutionOptions opts) {
        List<SequencedEvent> recorded = log.read(id);
        ExecutionState state = Reducer.fold(id, recorded);
        ExecutionInfo info = new ExecutionInfo(id, state.attempt(),
                state.taskType() != null ? state.taskType() : task.getClass().getName(), opts.metadata());
        Logger logger = LoggerFactory.getLogger("catalyst.replay." + id.value());
        ReplayingContext ctx = new ReplayingContext(id, log, defaultModel, info, opts.vars(),
                eventMapper, payloads, inDoubtPolicy, costModel, replayMode, null, clock, logger, recorded, /* appendEnabled */ false);
        try {
            task.execute(ctx);
        } catch (RuntimeException e) {
            throw e; // includes NonDeterministicReplayException
        } catch (Exception e) {
            throw new RuntimeException("Replay of " + id + " failed", e);
        }
        return inspect(id);
    }

    /**
     * Recovers an execution after a crash or pause. In M0 a runtime cannot reconstruct the original
     * {@link Task} instance, so resume is driven by re-submitting the task with the same idempotency
     * key: {@code execute(task, ExecutionOptions.withKey(k))}. A task registry that makes
     * {@code resume(id)} standalone is a thin follow-up.
     */
    public ExecutionHandle<?> resume(ExecutionId id) {
        throw new UnsupportedOperationException(
                "M0 resumes by re-submitting the task with the same idempotency key: "
                        + "execute(task, ExecutionOptions.withKey(key)). Standalone resume(id) needs a task registry.");
    }

    /**
     * Forks a recorded execution at {@code atSeq} to explore an alternative (spec §7): the returned
     * {@link BranchBuilder} lets you swap the model, substitute counterfactual tool results, or
     * supply vars, then {@code run(task)} it forward into a new child execution and get its
     * {@link Trajectory}.
     */
    public BranchBuilder branch(ExecutionId id, long atSeq) {
        return new BranchBuilder(this, id, atSeq);
    }

    /**
     * Runs a branch: substitute the parent's recorded prefix up to {@code atSeq} (swapping in any
     * counterfactual tool results), fork, and run forward live with the override model. Records a
     * new child execution beginning with {@code ExecutionBranched} and returns the fork's effective
     * trajectory (parent prefix + child's forward steps).
     */
    <R> Trajectory runBranch(ExecutionId parentId, long atSeq, Model overrideModel,
                             Map<String, Object> toolOverrideObjects, Map<String, Object> vars,
                             String changedComponents, Task<R> task) {
        List<SequencedEvent> parentEvents = log.read(parentId);
        if (parentEvents.isEmpty()) {
            throw new IllegalArgumentException("Cannot branch unknown execution: " + parentId);
        }
        ExecutionState parentState = Reducer.fold(parentId, parentEvents);

        Map<String, JsonNode> overrides = new HashMap<>();
        toolOverrideObjects.forEach((name, value) -> overrides.put(name, payloads.toTree(value)));

        ExecutionId childId = ExecutionId.random();
        String components = changedComponents == null || changedComponents.isBlank()
                ? "branch@" + atSeq : changedComponents;
        BranchSpec spec = new BranchSpec(atSeq, parentId.value(), overrides, components);

        // The child log begins with the branch marker + started (the fork itself won't re-record it).
        log.append(childId, new CatalystEvent.ExecutionBranched(now(), parentId.value(), atSeq, components));
        log.append(childId, new CatalystEvent.ExecutionStarted(now(), 1, nodeId));

        String taskType = parentState.taskType() != null ? parentState.taskType() : task.getClass().getName();
        ExecutionInfo info = new ExecutionInfo(childId, 1, taskType, Map.of());
        Logger logger = LoggerFactory.getLogger("catalyst.branch." + childId.value());
        Model model = overrideModel != null ? overrideModel : defaultModel;

        ReplayingContext ctx = new ReplayingContext(childId, log, model, info, vars,
                eventMapper, payloads, inDoubtPolicy, costModel, ReplayMode.BRANCH, spec, clock, logger,
                parentEvents, /* appendEnabled */ true);
        try {
            R result = task.execute(ctx);
            log.append(childId, new CatalystEvent.ExecutionCompleted(now(), payloads.toTree(result)));
        } catch (RuntimeException e) {
            log.append(childId, new CatalystEvent.ExecutionFailed(now(), String.valueOf(e), log.latestSeq(childId)));
            throw e;
        } catch (Exception e) {
            log.append(childId, new CatalystEvent.ExecutionFailed(now(), String.valueOf(e), log.latestSeq(childId)));
            throw new RuntimeException("Branch of " + parentId + " failed", e);
        }

        ExecutionState childState = inspect(childId);
        // Effective trajectory of the fork: parent prefix (seq ≤ atSeq) + the child's forward steps.
        List<TimelineStep> forkSteps = new ArrayList<>();
        for (TimelineStep s : parentState.trajectory()) {
            if (s.seq() <= atSeq) forkSteps.add(s);
        }
        forkSteps.addAll(childState.trajectory());

        List<SequencedEvent> prefix = new ArrayList<>();
        for (SequencedEvent se : parentEvents) {
            if (se.seq() <= atSeq) prefix.add(se);
        }
        Cost prefixCost = Reducer.fold(parentId, prefix).cost();
        Cost childCost = childState.cost();
        Cost combined = prefixCost.plus(childCost.promptTokens(), childCost.completionTokens(), childCost.usd());
        return Trajectory.of(childId, childState.status(), forkSteps, combined);
    }

    /**
     * Pauses an execution. A no-op on an already-terminal execution (never reopens a closed log).
     * {@code synchronized} + the in-flight guard avoid a race: an execution running in this process
     * appends its own events on a worker thread, so injecting a pause event concurrently could land
     * after {@code ExecutionCompleted}. Refuse while it is in flight here. (v0.1 has no live
     * pause/cancel of a running task — that lands with the reserved WAITING/signal APIs in v1.)
     */
    public synchronized void pause(ExecutionId id) {
        if (inspect(id).isTerminal()) return;      // terminal (incl. just-completed) → no-op
        if (inFlight.containsKey(id)) {
            throw new IllegalStateException("Cannot pause an execution running in this process: " + id);
        }
        log.append(id, new CatalystEvent.ExecutionPaused(now(), "paused by request"));
    }

    /**
     * Requests cancellation. A no-op on an already-terminal execution, and refused while the
     * execution is in flight in this process (see {@link #pause}). Note: the v0 event schema (spec §5)
     * has no dedicated cancellation event, so this records a failure marked "cancelled" (open
     * question §13). Folds to FAILED.
     */
    public synchronized void cancel(ExecutionId id) {
        if (inspect(id).isTerminal()) return;      // terminal (incl. just-completed) → no-op
        if (inFlight.containsKey(id)) {
            throw new IllegalStateException("Cannot cancel an execution running in this process: " + id);
        }
        log.append(id, new CatalystEvent.ExecutionFailed(now(), "cancelled by request", log.latestSeq(id)));
    }

    public EventLog log() {
        return log;
    }

    // ── Internals ────────────────────────────────────────────────────────────

    private void createExecution(ExecutionId id, String taskType, String key, ExecutionOptions opts) {
        String configFingerprint = "replayMode=" + replayMode + ";inDoubt=" + inDoubtPolicy;
        log.append(id, new CatalystEvent.ExecutionCreated(now(), taskType, argsHash(opts.vars()), configFingerprint, key));
    }

    /**
     * A stable content fingerprint of the task vars. Uses canonical JSON (keys are ordered by the
     * shared mapper) hashed with SHA-256 — unlike {@code Map.hashCode()}, this is stable across JVMs
     * and process restarts. Best-effort: unserializable vars fall back to a marker.
     */
    private String argsHash(Map<String, Object> vars) {
        if (vars.isEmpty()) return "novars";
        try {
            return Hashing.sha256(eventMapper.valueToTree(vars).toString());
        } catch (RuntimeException e) {
            return "unhashable";
        }
    }

    private <R> void runAttempt(Task<R> task, ExecutionId id, String taskType,
                                ExecutionOptions opts, Mode mode, CompletableFuture<R> future) {
        // Everything is inside a try that always completes the future — including the setup below
        // (log I/O, fold, lifecycle appends, context construction). If any of that throws, the caller
        // must see the failure rather than block forever on handle.result().
        try {
            List<SequencedEvent> recorded = log.read(id);
            ExecutionState state = Reducer.fold(id, recorded);

            // Attaching to an already-terminal execution returns its recorded outcome directly —
            // never re-runs the task. Re-execution could surface a different (wrong) exception, and
            // re-running a completed task is wasteful. (replay(id, task) is the explicit re-run path.)
            if (mode == Mode.REPLAY_TERMINAL) {
                completeFromTerminalState(id, state, future);
                return;
            }

            int attempt = state.attempt() + 1;
            switch (mode) {
                case FRESH -> log.append(id, new CatalystEvent.ExecutionStarted(now(), attempt, nodeId));
                case RESUME -> log.append(id, new CatalystEvent.ExecutionResumed(now(), attempt));
                case REPLAY_TERMINAL -> { /* handled above */ }
            }

            String effectiveTaskType = state.taskType() != null ? state.taskType() : taskType;
            ExecutionInfo info = new ExecutionInfo(id, attempt, effectiveTaskType, opts.metadata());
            Logger logger = LoggerFactory.getLogger("catalyst.exec." + id.value());

            ReplayingContext ctx = new ReplayingContext(id, log, defaultModel, info, opts.vars(),
                    eventMapper, payloads, inDoubtPolicy, costModel, replayMode, null, clock, logger, recorded, /* appendEnabled */ true);

            try {
                R result = task.execute(ctx);
                log.append(id, new CatalystEvent.ExecutionCompleted(now(), payloads.toTree(result)));
                future.complete(result);
            } catch (ExecutionPausedSignal pause) {
                // ExecutionPaused already recorded by the context; leave the execution paused.
                future.completeExceptionally(pause);
            } catch (Throwable t) {
                try {
                    log.append(id, new CatalystEvent.ExecutionFailed(now(), String.valueOf(t), log.latestSeq(id)));
                } catch (RuntimeException ignored) {
                    // best-effort failure record; the future still completes exceptionally below
                }
                future.completeExceptionally(t);
            }
        } catch (Throwable setupFailure) {
            future.completeExceptionally(setupFailure);
        }
    }

    /** Completes a handle from an already-terminal execution's recorded outcome, without re-running. */
    @SuppressWarnings("unchecked")
    private <R> void completeFromTerminalState(ExecutionId id, ExecutionState state, CompletableFuture<R> future) {
        switch (state.status()) {
            case COMPLETED -> future.complete((R) payloads.fromTree(state.result()));
            case FAILED, CANCELLED -> future.completeExceptionally(new ExecutionFailedException(id, state.error()));
            default -> future.completeExceptionally(
                    new IllegalStateException("Execution " + id + " is not terminal: " + state.status()));
        }
    }

    private Instant now() {
        return clock.instant();
    }

    @Override
    public void close() {
        executor.close();
        log.close();
    }

    // ── Builder ──────────────────────────────────────────────────────────────

    public static final class Builder {
        private EventLog log;
        private Model model;
        private Clock clock = Clock.systemUTC();
        private ReplayMode replayMode = ReplayMode.STRICT;
        private InDoubtPolicy inDoubtPolicy = InDoubtPolicy.FAIL;
        private CostModel costModel = CostModel.free();
        private ObjectMapper eventMapper = EventJson.shared();
        private String nodeId = "node-0";

        public Builder log(EventLog log) {
            this.log = log;
            return this;
        }

        public Builder model(Model model) {
            this.model = model;
            return this;
        }

        public Builder clock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder replayMode(ReplayMode replayMode) {
            this.replayMode = replayMode;
            return this;
        }

        public Builder inDoubtPolicy(InDoubtPolicy inDoubtPolicy) {
            this.inDoubtPolicy = inDoubtPolicy;
            return this;
        }

        /** Prices completions so cost folds into the execution (default {@link CostModel#free()}). */
        public Builder costModel(CostModel costModel) {
            this.costModel = costModel;
            return this;
        }

        public Builder serializer(ObjectMapper eventMapper) {
            this.eventMapper = eventMapper;
            return this;
        }

        public Builder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public CatalystRuntime build() {
            if (log == null) {
                throw new IllegalStateException("An EventLog is required (e.g. EventLogs.inMemory() or GumboEventLog.at(path))");
            }
            return new CatalystRuntime(this);
        }
    }
}
