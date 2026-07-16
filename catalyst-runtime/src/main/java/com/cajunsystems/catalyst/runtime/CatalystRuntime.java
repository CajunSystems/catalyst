package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionInfo;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.ReplayMode;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.TaskFactory;
import com.cajunsystems.catalyst.TaskRegistry;
import com.cajunsystems.catalyst.Cost;
import com.cajunsystems.catalyst.engine.BranchSpec;
import com.cajunsystems.catalyst.engine.CancellationSignal;
import com.cajunsystems.catalyst.engine.CancellationToken;
import com.cajunsystems.catalyst.engine.CostModel;
import com.cajunsystems.catalyst.engine.ExecutionFailedException;
import com.cajunsystems.catalyst.engine.ExecutionPausedSignal;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.Hashing;
import com.cajunsystems.catalyst.engine.InDoubtPolicy;
import com.cajunsystems.catalyst.engine.NonDeterministicReplayException;
import com.cajunsystems.catalyst.engine.PayloadCodec;
import com.cajunsystems.catalyst.engine.Reducer;
import com.cajunsystems.catalyst.engine.ReducerState;
import com.cajunsystems.catalyst.engine.ReplayingContext;
import com.cajunsystems.catalyst.engine.TimelineStep;
import com.cajunsystems.catalyst.engine.Trajectory;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.EventJson;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.log.Snapshot;
import com.cajunsystems.catalyst.model.Model;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import java.util.concurrent.CancellationException;
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
    private final long snapshotInterval;
    private final TaskRegistry registry;
    private final ExecutorService executor;

    /**
     * Attempts currently running in this process, keyed by execution. Guards against scheduling a
     * second concurrent attempt for the same execution (which would interleave events and corrupt the
     * stream): a re-submission of a still-running execution attaches to the in-flight attempt instead.
     */
    private final ConcurrentHashMap<ExecutionId, CompletableFuture<?>> inFlight = new ConcurrentHashMap<>();

    /**
     * Cancellation handles for attempts running in this process. {@code cancel(id)} trips the token and
     * interrupts the worker; the worker observes it at its next live boundary (or on interrupt) and
     * records {@link CatalystEvent.ExecutionCancelled} itself — so no other thread ever appends to a
     * running execution's stream (the same invariant the in-flight guard protects for pause).
     */
    private final ConcurrentHashMap<ExecutionId, RunningAttempt> running = new ConcurrentHashMap<>();

    /**
     * Per-execution mutual exclusion. Every section that decides <em>whether to schedule an attempt</em>
     * — {@code execute}, {@code resume}, {@code pause}, {@code cancel} — reads the log, folds state and
     * then acts on it, so it must be atomic against another caller doing the same for the same
     * execution. Scoping that to the id (rather than the whole runtime, as the coarse
     * {@code synchronized} used to) means unrelated executions no longer serialize on each other's log
     * I/O and folds.
     *
     * <p>Only the scheduling decision is held under this lock; the task itself runs on its own virtual
     * thread with no lock held.
     */
    private final KeyedLock<ExecutionId> byExecution = new KeyedLock<>();

    /**
     * Per-idempotency-key mutual exclusion, guarding the {@code findByKey} → {@code putKey} →
     * {@code createExecution} window: without it two concurrent submissions of the same key could both
     * miss the lookup and create two executions for one key. A separate domain from
     * {@link #byExecution} because the id is not yet known when the window opens — that is the whole
     * reason the key must be locked.
     *
     * <p><strong>Lock order: key → id, never the reverse.</strong> {@code execute} is the only path that
     * takes both, and it always takes the key lock first; {@code resume}/{@code pause}/{@code cancel}
     * take only the id lock. No cycle, so no deadlock.
     */
    private final KeyedLock<String> byIdempotencyKey = new KeyedLock<>();

    /** A running attempt's cancellation token plus its worker thread (set once the worker starts). */
    private static final class RunningAttempt {
        final CancellationToken token = new CancellationToken();
        volatile Thread thread;
    }

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
        this.snapshotInterval = b.snapshotInterval;
        this.registry = b.registry;
        this.executor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("catalyst-exec-", 0).factory());
    }

    public static Builder builder() {
        return new Builder();
    }

    // ── Runtime API (spec §7) ────────────────────────────────────────────────

    /** Starts a new execution, or attaches/resumes an existing one when an idempotency key matches. */
    public <R> ExecutionHandle<R> execute(Task<R> task, ExecutionOptions opts) {
        String taskType = task.getClass().getName();
        Optional<String> key = opts.idempotencyKey();

        // No key → a fresh random id nobody else can name yet, so only the id lock is needed.
        if (key.isEmpty()) {
            ExecutionId id = ExecutionId.random();
            createExecution(id, taskType, "", opts);
            return byExecution.withLock(id, () -> scheduleAttempt(task, id, taskType, opts, Mode.FRESH));
        }

        // Keyed: hold the key lock across the whole decision — resolving the key, creating the
        // execution and scheduling its attempt. Releasing it once the id is known would leave a window
        // where a same-key submission resolves to that id and schedules a second attempt for it before
        // this one has registered its future. Lock order is key → id (see byIdempotencyKey).
        return byIdempotencyKey.withLock(key.get(), () -> {
            Optional<ExecutionId> existing = log.findByKey(key.get());
            if (existing.isEmpty()) {
                ExecutionId id = ExecutionId.random();
                log.putKey(key.get(), id);
                createExecution(id, taskType, key.get(), opts);
                return byExecution.withLock(id, () -> scheduleAttempt(task, id, taskType, opts, Mode.FRESH));
            }
            ExecutionId id = existing.get();
            return byExecution.withLock(id, () -> {
                // If an attempt for this execution is already running in this process, attach to it
                // rather than scheduling a duplicate attempt that would corrupt the event stream.
                CompletableFuture<?> live = inFlight.get(id);
                if (live != null) {
                    return new FutureExecutionHandle<>(id, CatalystRuntime.<R>cast(live));
                }
                Mode mode = foldState(id).isTerminal() ? Mode.REPLAY_TERMINAL : Mode.RESUME;
                return scheduleAttempt(task, id, taskType, opts, mode);
            });
        });
    }

    /**
     * Schedules an attempt for {@code id} on a virtual thread and returns its handle. Registers the
     * in-flight future and cancellation handle while the caller holds this execution's lock (every
     * caller — {@code execute} / {@code resume} — takes {@code byExecution} first), so a second
     * concurrent attempt for the same execution is detected and attached to rather than scheduled.
     */
    private <R> ExecutionHandle<R> scheduleAttempt(Task<R> task, ExecutionId id, String taskType,
                                                   ExecutionOptions opts, Mode mode) {
        CompletableFuture<R> future = new CompletableFuture<>();
        RunningAttempt attempt = new RunningAttempt();
        inFlight.put(id, future);
        running.put(id, attempt);
        future.whenComplete((r, t) -> {
            inFlight.remove(id, future);
            running.remove(id, attempt);
        });
        executor.execute(() -> runAttempt(task, id, taskType, opts, mode, future, attempt));
        return new FutureExecutionHandle<>(id, future);
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

    /**
     * Folded state plus the typed timeline for an execution. Restores the latest snapshot (if any)
     * and folds only the events after it, so {@code inspect} on a long execution does not re-fold the
     * whole log (spec §8). Opportunistically writes a fresh snapshot once enough new events have
     * accumulated since the last one.
     */
    public ExecutionState inspect(ExecutionId id) {
        return foldState(id);
    }

    /**
     * The snapshot-aware fold used everywhere state is derived: read the latest checkpoint, fold the
     * log's tail from it, and checkpoint again if the interval has elapsed. Falls back to a full fold
     * when no snapshot exists or the log does not persist them.
     */
    private ExecutionState foldState(ExecutionId id) {
        ReducerState base = ReducerState.initial();
        long fromExclusive = -1;
        try {
            Optional<Snapshot> snap = log.readSnapshot(id);
            if (snap.isPresent()) {
                base = deserializeState(snap.get().state());
                fromExclusive = snap.get().throughSeq();
            }
        } catch (RuntimeException e) {
            // A snapshot is a pure optimization: a corrupt or schema-incompatible one must never make an
            // intact log inaccessible. Fall back to a full re-fold — exactly as if no snapshot existed —
            // symmetric with maybeSnapshot's best-effort write. maybeSnapshot below then overwrites the
            // bad checkpoint with a fresh, valid one, so the log self-heals.
            LoggerFactory.getLogger("catalyst.snapshot")
                    .warn("ignoring unreadable snapshot for {}, folding the full log: {}", id, e.toString());
            base = ReducerState.initial();
            fromExclusive = -1;
        }
        ReducerState folded = Reducer.foldFrom(base, log.readFrom(id, fromExclusive));
        maybeSnapshot(id, folded, fromExclusive);
        return folded.toExecutionState(id);
    }

    /**
     * Persists a fresh checkpoint when at least {@code snapshotInterval} events have been folded since
     * the checkpoint we restored from ({@code sinceSeqExclusive}). A no-op when snapshots are disabled
     * ({@code snapshotInterval <= 0}) or nothing new has accumulated. Best-effort: a failed write only
     * costs a future re-fold, so it never fails the caller.
     */
    private void maybeSnapshot(ExecutionId id, ReducerState folded, long sinceSeqExclusive) {
        if (snapshotInterval <= 0) return;
        if (folded.lastSeq() - sinceSeqExclusive < snapshotInterval) return;
        try {
            log.writeSnapshot(id, new Snapshot(folded.lastSeq(), serializeState(folded)));
        } catch (RuntimeException e) {
            LoggerFactory.getLogger("catalyst.snapshot").debug("snapshot write failed for {}: {}", id, e.toString());
        }
    }

    private byte[] serializeState(ReducerState state) {
        try {
            return eventMapper.writeValueAsBytes(state);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize reducer snapshot", e);
        }
    }

    private ReducerState deserializeState(byte[] bytes) {
        try {
            return eventMapper.readValue(bytes, ReducerState.class);
        } catch (java.io.IOException e) {
            throw new IllegalStateException("Failed to deserialize reducer snapshot", e);
        }
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

    /** Recovers an execution by id, with no task input vars. See {@link #resume(ExecutionId, ExecutionOptions)}. */
    public ExecutionHandle<?> resume(ExecutionId id) {
        return resume(id, ExecutionOptions.none());
    }

    /**
     * Recovers an execution after a crash or pause <em>from its id alone</em> (spec §7, v0.2 increment
     * ②). The runtime reconstructs the {@link Task} from its recorded task type via the configured
     * {@link TaskRegistry} — so the caller no longer has to re-submit the original task instance with
     * its idempotency key (the M0 recovery path, which still works). Register the task type up front
     * with {@code Catalyst.builder().task(...)}.
     *
     * <p>An execution already running in this process attaches to the in-flight attempt (no second
     * concurrent attempt). A non-terminal execution runs forward, recording {@code ExecutionResumed}
     * and substituting every recorded boundary from the log until the task passes the log tail — so
     * recovery performs no duplicate side effects. Resuming an already-terminal execution replays its
     * recorded outcome without re-running the task, exactly as attaching by key does.
     *
     * <p>Task input vars are not persisted (only their {@code argsHash} is), so a task that branches
     * on {@code ctx.var(...)} must be resumed with the same vars via {@code opts}; otherwise it takes a
     * different path and (correctly) trips {@link NonDeterministicReplayException} under strict replay.
     *
     * @throws IllegalArgumentException if no execution with this id exists
     * @throws IllegalStateException    if the execution is non-terminal and no task is registered for
     *                                  its recorded task type (a terminal execution needs no registration)
     */
    public ExecutionHandle<?> resume(ExecutionId id, ExecutionOptions opts) {
        return byExecution.withLock(id, () -> {
            // Already running here → attach to the in-flight attempt rather than scheduling a duplicate
            // that would interleave events and corrupt the stream (same guard as execute()).
            CompletableFuture<?> live = inFlight.get(id);
            if (live != null) {
                return new FutureExecutionHandle<>(id, live);
            }

            ExecutionState state = foldState(id);
            String taskType = state.taskType();
            if (taskType == null) {
                throw new IllegalArgumentException("Cannot resume unknown execution: " + id);
            }

            // A terminal execution replays its recorded outcome without ever re-running the task
            // (runAttempt short-circuits REPLAY_TERMINAL before touching it), so it needs no registered
            // factory — it is recoverable from the id alone. Only a live resume reconstructs the task.
            if (state.isTerminal()) {
                return scheduleAttempt(TERMINAL_REPLAY_TASK, id, taskType, opts, Mode.REPLAY_TERMINAL);
            }

            TaskFactory factory = registry.lookup(taskType).orElseThrow(() -> new IllegalStateException(
                    "No task registered for type '" + taskType + "' to resume " + id
                            + "; register it with Catalyst.builder().task(...), or recover by re-submitting the"
                            + " task with its idempotency key via execute(task, ExecutionOptions.withKey(key))."));

            return scheduleAttempt(factory.create(), id, taskType, opts, Mode.RESUME);
        });
    }

    /**
     * Stand-in task for the {@code REPLAY_TERMINAL} path, which completes a handle from the recorded
     * terminal outcome and never invokes the task. It exists only to satisfy {@code scheduleAttempt}'s
     * signature; if it were ever executed that would be a bug, so it fails loudly.
     */
    private static final Task<?> TERMINAL_REPLAY_TASK = ctx -> {
        throw new IllegalStateException("terminal replay must not run the task");
    };

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
        } catch (ExecutionPausedSignal pause) {
            // ExecutionPaused already recorded by the context (e.g. an ASK in-doubt tool); the child
            // is left PAUSED — do NOT append a spurious ExecutionFailed on top of it.
            throw pause;
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
     * This execution's lock + the in-flight guard avoid a race: an execution running in this process
     * appends its own events on a worker thread, so injecting a pause event concurrently could land
     * after {@code ExecutionCompleted}. Refuse while it is in flight here. (v0.1 has no live
     * pause/cancel of a running task — that lands with the reserved WAITING/signal APIs in v1.)
     */
    public void pause(ExecutionId id) {
        byExecution.withLock(id, () -> {
            if (inspect(id).isTerminal()) return;  // terminal (incl. just-completed) → no-op
            if (inFlight.containsKey(id)) {
                throw new IllegalStateException("Cannot pause an execution running in this process: " + id);
            }
            log.append(id, new CatalystEvent.ExecutionPaused(now(), "paused by request"));
        });
    }

    /**
     * Requests cancellation. A no-op on an already-terminal execution. Records
     * {@link CatalystEvent.ExecutionCancelled}, so the execution folds to {@code CANCELLED} (not
     * {@code FAILED}).
     *
     * <p>Cancellation is cooperative for a running task: when the execution is in flight in this
     * process this trips its {@link CancellationToken} and interrupts the worker rather than appending
     * from this thread (which would race the worker's own appends). The worker observes the token at
     * its next live boundary — a model/tool/effect/memory call — unwinds, and records the
     * {@code ExecutionCancelled} itself. A task making no further boundary calls cannot be unwound this
     * way and runs to completion; cancellation of an already-completed run is then a no-op. When the
     * execution is <em>not</em> running here (crashed, paused, or never started in this process) the
     * event is appended directly, since there is no concurrent writer.
     */
    public void cancel(ExecutionId id) {
        byExecution.withLock(id, () -> {
            if (inspect(id).isTerminal()) return;  // terminal (incl. just-completed) → no-op
            RunningAttempt attempt = running.get(id);
            if (attempt != null) {
                // Cooperative: the worker thread — the only writer for a running execution — records the
                // ExecutionCancelled when it observes the trip. Interrupt to unblock a task parked in I/O.
                attempt.token.cancel("cancelled by request");
                Thread worker = attempt.thread;
                if (worker != null) worker.interrupt();
                return;
            }
            log.append(id, new CatalystEvent.ExecutionCancelled(now(), "cancelled by request", log.latestSeq(id)));
        });
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
                                ExecutionOptions opts, Mode mode, CompletableFuture<R> future,
                                RunningAttempt runningAttempt) {
        runningAttempt.thread = Thread.currentThread();
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
                    eventMapper, payloads, inDoubtPolicy, costModel, replayMode, null, clock, logger,
                    recorded, /* appendEnabled */ true, runningAttempt.token);

            try {
                R result = task.execute(ctx);
                log.append(id, new CatalystEvent.ExecutionCompleted(now(), payloads.toTree(result)));
                future.complete(result);
            } catch (ExecutionPausedSignal pause) {
                // ExecutionPaused already recorded by the context; leave the execution paused.
                future.completeExceptionally(pause);
            } catch (Throwable t) {
                // A cancelled attempt folds to CANCELLED only via the cooperative unwind itself: the
                // CancellationSignal the context raises at the next live boundary once the token trips
                // (that is, the task acknowledging cancellation). Every other throwable after a cancel
                // request is preserved as ExecutionFailed — including a bare InterruptedException, which
                // is indistinguishable from a fresh interrupt failure a blocking call raised during
                // cleanup. The interrupt() we raise is only a best-effort nudge to reach that boundary
                // promptly; it never, by itself, reclassifies a failure as a clean cancellation.
                if (t instanceof CancellationSignal) {
                    String reason = runningAttempt.token.reason();
                    try {
                        log.append(id, new CatalystEvent.ExecutionCancelled(now(), reason, log.latestSeq(id)));
                    } catch (RuntimeException ignored) {
                        // best-effort cancellation record; the future still completes exceptionally below
                    }
                    future.completeExceptionally(new CancellationException(reason));
                } else {
                    try {
                        log.append(id, new CatalystEvent.ExecutionFailed(now(), String.valueOf(t), log.latestSeq(id)));
                    } catch (RuntimeException ignored) {
                        // best-effort failure record; the future still completes exceptionally below
                    }
                    future.completeExceptionally(t);
                }
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
            case CANCELLED -> future.completeExceptionally(new CancellationException(state.error()));
            case FAILED -> future.completeExceptionally(new ExecutionFailedException(id, state.error()));
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
        private long snapshotInterval = 100;
        private TaskRegistry registry = new TaskRegistry();

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

        /**
         * How many new events may accumulate before {@code inspect}/resume writes a fresh fold
         * checkpoint (spec §8). A checkpoint lets later folds skip the events before it. Default 100;
         * {@code 0} (or negative) disables snapshotting and always folds the full log.
         */
        public Builder snapshotInterval(long snapshotInterval) {
            this.snapshotInterval = snapshotInterval;
            return this;
        }

        /**
         * Registers a task instance under its class name so {@code runtime.resume(id)} can reconstruct
         * it from the recorded log (spec §7, v0.2 increment ②). Prefer a named {@code Task} class:
         * a lambda's class name is not stable across processes. A task is re-invoked on resume with its
         * boundaries substituted, so sharing one stateless instance is safe.
         */
        public Builder task(Task<?> task) {
            registry.register(task);
            return this;
        }

        /** Registers a factory that reconstructs a task for the given recorded task type. */
        public Builder task(String taskType, TaskFactory factory) {
            registry.register(taskType, factory);
            return this;
        }

        /** Registers a factory under {@code type.getName()} — the type a named {@code Task} class records. */
        public Builder task(Class<? extends Task<?>> type, TaskFactory factory) {
            registry.register(type, factory);
            return this;
        }

        /** Replaces the task registry wholesale (for callers assembling registrations elsewhere). */
        public Builder tasks(TaskRegistry registry) {
            this.registry = java.util.Objects.requireNonNull(registry, "registry");
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
