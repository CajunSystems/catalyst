package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionInfo;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.ReplayMode;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.TaskFactory;
import com.cajunsystems.catalyst.TaskRegistry;
import com.cajunsystems.catalyst.Cost;
import com.cajunsystems.catalyst.engine.BranchSpec;
import com.cajunsystems.catalyst.engine.CancellationSignal;
import com.cajunsystems.catalyst.engine.CancellationToken;
import com.cajunsystems.catalyst.engine.CostModel;
import com.cajunsystems.catalyst.engine.ExecutionAbandonedSignal;
import com.cajunsystems.catalyst.engine.ExecutionFailedException;
import com.cajunsystems.catalyst.engine.ExecutionPausedSignal;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.Hashing;
import com.cajunsystems.catalyst.engine.InDoubtException;
import com.cajunsystems.catalyst.engine.InDoubtPolicy;
import com.cajunsystems.catalyst.engine.NonDeterministicReplayException;
import com.cajunsystems.catalyst.engine.PayloadCodec;
import com.cajunsystems.catalyst.engine.Reducer;
import com.cajunsystems.catalyst.engine.ReducerState;
import com.cajunsystems.catalyst.capture.AutoCapture;
import com.cajunsystems.catalyst.engine.ReplayingContext;
import com.cajunsystems.catalyst.engine.RetryPolicy;
import com.cajunsystems.catalyst.engine.TimelineStep;
import com.cajunsystems.catalyst.engine.Trajectory;
import com.cajunsystems.catalyst.events.BlobStore;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.EventCodec;
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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The Catalyst runtime: owns scheduling (virtual threads), event persistence, resume and lifecycle
 * (spec §4, §7). It is thin — durability lives in the {@link EventLog} and the record/substitute
 * semantics live in {@link ReplayingContext}. This class wires them together and manages the
 * lifecycle events around a task run.
 */
public final class CatalystRuntime implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CatalystRuntime.class);
    private static final Duration DEFAULT_DRAIN_TIMEOUT = Duration.ofSeconds(10);
    /** How often a retry backoff checks whether it was cancelled or abandoned. */
    private static final long BACKOFF_POLL_MILLIS = 50;
    /** How many still-running ids a drain-timeout warning names before summarising the rest. */
    private static final int DRAIN_WARNING_SAMPLE = 10;

    private final EventLog log;
    private final Model defaultModel;
    private final Clock clock;
    private final ReplayMode replayMode;
    private final InDoubtPolicy inDoubtPolicy;
    private final CostModel costModel;
    private final RetryPolicy retryPolicy;
    private final ObjectMapper eventMapper;
    private final PayloadCodec payloads;
    private final String nodeId;
    private final long snapshotInterval;
    private final TaskRegistry registry;
    private final ExecutorService executor;
    /** Set once by {@code build()} when {@code recoverOnStart} is on, before the runtime is handed out. */
    private RecoveryReport startupRecovery;
    private final Duration drainTimeout;

    /**
     * True once {@link #close()} has begun. Every attempt's token reads it as its abandon flag, so this
     * one write unwinds all running attempts at their next live boundary — and any attempt scheduled
     * concurrently with the shutdown, which a per-attempt trip could miss. New submissions are refused.
     */
    private volatile boolean closing;
    private final AtomicBoolean closed = new AtomicBoolean();
    /** Released when the first close() finishes, so a concurrent close() returns only once the log is closed. */
    private final CountDownLatch closeFinished = new CountDownLatch(1);

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
    private final class RunningAttempt {
        final CancellationToken token = new CancellationToken(() -> closing);
        volatile Thread thread;
    }

    private enum Mode { FRESH, RESUME, REPLAY_TERMINAL }

    private CatalystRuntime(Builder b, EventLog log) {
        this.log = log;
        this.defaultModel = b.model;
        this.clock = b.clock;
        this.replayMode = b.replayMode;
        this.inDoubtPolicy = b.inDoubtPolicy;
        this.costModel = b.costModel;
        this.retryPolicy = b.retryPolicy;
        this.eventMapper = b.eventMapper;
        this.payloads = new PayloadCodec();
        this.nodeId = b.nodeId;
        this.snapshotInterval = b.snapshotInterval;
        this.registry = b.registry;
        this.drainTimeout = b.drainTimeout;
        this.executor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("catalyst-exec-", 0).factory());
    }

    public static Builder builder() {
        return new Builder();
    }

    // ── Runtime API (spec §7) ────────────────────────────────────────────────

    /** Starts a new execution, or attaches/resumes an existing one when an idempotency key matches. */
    public <R> ExecutionHandle<R> execute(Task<R> task, ExecutionOptions opts) {
        requireOpen();
        String taskType = task.getClass().getName();
        Optional<String> key = opts.idempotencyKey();

        // No key → a fresh random id no caller can name yet, so only the id lock is needed. Creation
        // happens UNDER it, not before: ExecutionCreated puts the id in the execution index, and a
        // concurrent recoverAll() that saw it there — open, STARTING, nothing in flight — would resume
        // it and leave this FRESH attempt as a second writer on the same stream.
        if (key.isEmpty()) {
            ExecutionId id = ExecutionId.random();
            return byExecution.withLock(id, () -> {
                createExecution(id, taskType, "", opts);
                return scheduleAttempt(task, id, taskType, opts, Mode.FRESH);
            });
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
                // Created under the id lock for the same reason as the keyless path above.
                return byExecution.withLock(id, () -> {
                    createExecution(id, taskType, key.get(), opts);
                    return scheduleAttempt(task, id, taskType, opts, Mode.FRESH);
                });
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
        // Callers checked requireOpen() before writing anything; close() can still land between that
        // check and here, by which point execute() may have recorded ExecutionCreated. Return a failed
        // handle — the same result as the RejectedExecutionException race below — rather than throw
        // for an execution that now exists in the log and will be recovered on the next start.
        if (closing) {
            return new FutureExecutionHandle<>(id, CompletableFuture.failedFuture(notStartedHere(id, null)));
        }
        CompletableFuture<R> future = new CompletableFuture<>();
        RunningAttempt attempt = new RunningAttempt();
        inFlight.put(id, future);
        running.put(id, attempt);
        future.whenComplete((r, t) -> {
            inFlight.remove(id, future);
            running.remove(id, attempt);
        });
        try {
            executor.execute(() -> runAttempt(task, id, taskType, opts, mode, future, attempt));
        } catch (RejectedExecutionException e) {
            // Lost a race with close(): the executor stopped between requireOpen and here. Nothing ran
            // and nothing was appended for this attempt, so the execution is as recoverable as any
            // other open one — the next start resumes it.
            future.completeExceptionally(notStartedHere(id, e));
        }
        return new FutureExecutionHandle<>(id, future);
    }

    private static IllegalStateException notStartedHere(ExecutionId id, Throwable cause) {
        return new IllegalStateException("Runtime is shutting down; " + id + " was not started here and"
                + " will be resumed on the next start if its task type is registered", cause);
    }

    private void requireOpen() {
        if (closing) throw new IllegalStateException("Runtime is shutting down; not accepting new work");
    }

    /**
     * Whether {@link #close()} has begun: new work is refused, and running attempts unwind at their
     * next boundary. A readiness probe can report not-ready from this.
     */
    public boolean isShuttingDown() {
        return closing;
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
        try (AutoCapture.Scope captured = AutoCapture.bind(ctx)) {
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
        requireOpen();
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
     * Finds every execution a previous process left unfinished and resumes the ones a crash
     * interrupted — the startup half of running Catalyst as a deployable node. Reads the log's
     * execution index ({@link EventLog#openExecutions()}), folds each open execution (snapshot-warm),
     * and resumes it via {@link #resume(ExecutionId)} only if its status is one a crash leaves behind:
     * {@code NEW}, {@code STARTING}, {@code RUNNING} or {@code RESUMING}.
     *
     * <p>{@code PAUSED} is left alone: it is recorded on purpose — by {@link #pause(ExecutionId)}, or
     * by {@code InDoubtPolicy.ASK} holding an execution for a verdict — and a restart must not undo
     * what an operator or a pending decision is holding. {@code WAITING} (reserved for signals) will
     * be left alone for the same reason. They stay
     * resumable by id. An execution already running in this process is left alone too, so calling
     * this on a live runtime never schedules a second attempt.
     *
     * <p>Each decision is taken under that execution's lock, as {@code resume} does. Failures are
     * per execution — an unregistered task type or an unreadable stream is reported in
     * {@link RecoveryReport#notRecovered()} and the scan moves on.
     *
     * <p>Executions resume with no task input vars (vars are not persisted, only their hash), so a
     * task that branches on {@code ctx.var(...)} should be resumed individually with
     * {@link #resume(ExecutionId, ExecutionOptions)}. Executions recorded before the log kept an
     * index are not listed by it, and so are not recovered here.
     *
     * @throws IllegalStateException if the log keeps no execution index
     */
    public RecoveryReport recoverAll() {
        if (!log.supportsExecutionIndex()) {
            throw new IllegalStateException(log.getClass().getSimpleName()
                    + " keeps no execution index, so open executions cannot be found after a restart");
        }
        Map<ExecutionId, ExecutionHandle<?>> resumed = new LinkedHashMap<>();
        Map<ExecutionId, Status> leftAlone = new LinkedHashMap<>();
        Map<ExecutionId, String> notRecovered = new LinkedHashMap<>();

        for (ExecutionId id : log.openExecutions()) {
            try {
                byExecution.withLock(id, () -> {
                    if (inFlight.containsKey(id)) {
                        leftAlone.put(id, Status.RUNNING);
                        return;
                    }
                    ExecutionState state = foldState(id);
                    if (!isCrashInterrupted(state.status())) {
                        leftAlone.put(id, state.status());
                        return;
                    }
                    if (registry.lookup(state.taskType()).isEmpty()) {
                        notRecovered.put(id, "no task registered for type '" + state.taskType() + "'");
                        return;
                    }
                    resumed.put(id, resume(id)); // the lock is reentrant
                });
            } catch (RuntimeException e) {
                notRecovered.put(id, String.valueOf(e));
            }
        }

        RecoveryReport report = new RecoveryReport(resumed, leftAlone, notRecovered);
        LOG.info("Recovery: resumed {}, left alone {}, not recovered {}",
                resumed.size(), leftAlone.size(), notRecovered.size());
        notRecovered.forEach((id, reason) -> LOG.warn("Not recovered: {} ({})", id.value(), reason));
        return report;
    }

    /** The states a process death leaves an execution in — as opposed to ones recorded deliberately. */
    private static boolean isCrashInterrupted(Status status) {
        return status == Status.NEW || status == Status.STARTING
                || status == Status.RUNNING || status == Status.RESUMING;
    }

    /**
     * The report from the recovery scan run by {@code build()} when the runtime was built with
     * {@link Builder#recoverOnStart(boolean)}; empty otherwise.
     */
    public Optional<RecoveryReport> startupRecovery() {
        return Optional.ofNullable(startupRecovery);
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
        try (AutoCapture.Scope captured = AutoCapture.bind(ctx)) {
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
        RetryPolicy policy = opts.retryPolicy().orElse(this.retryPolicy);
        // Outer try: the future is ALWAYS completed on every exit — including setup failures (log I/O,
        // fold, context construction), which append nothing (the log may be unreadable). A retryable
        // task failure loops WITHOUT completing the future, so inFlight stays registered and a concurrent
        // same-key execute attaches to this attempt rather than scheduling a duplicate writer.
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

            boolean firstIteration = true;
            while (true) {
                // Re-read + re-fold each iteration: a retry appends RetryRequested + ExecutionResumed,
                // and the context must seed from the full recorded prefix (including its own last
                // failure). Full read, not foldState — so snapshots are never consulted here.
                if (!firstIteration) {
                    recorded = log.read(id);
                    state = Reducer.fold(id, recorded);
                }
                // Shutting down before this attempt began: don't record a start for work this
                // process will not do. The execution stays as it is for the next start.
                if (runningAttempt.token.isAbandoned()) {
                    abandon(id, future);
                    return;
                }
                int attempt = state.attempt() + 1;
                if (firstIteration && mode == Mode.FRESH) {
                    log.append(id, new CatalystEvent.ExecutionStarted(now(), attempt, nodeId));
                } else {
                    // A retry IS a resume: it re-enters the task with the recorded prefix substituted.
                    log.append(id, new CatalystEvent.ExecutionResumed(now(), attempt));
                }
                firstIteration = false;

                String effectiveTaskType = state.taskType() != null ? state.taskType() : taskType;
                ExecutionInfo info = new ExecutionInfo(id, attempt, effectiveTaskType, opts.metadata());
                Logger logger = LoggerFactory.getLogger("catalyst.exec." + id.value());

                ReplayingContext ctx = new ReplayingContext(id, log, defaultModel, info, opts.vars(),
                        eventMapper, payloads, inDoubtPolicy, costModel, replayMode, null, clock, logger,
                        recorded, /* appendEnabled */ true, runningAttempt.token);

                // The capture scope is per-attempt: a retry re-enters the task from the top, so its
                // auto-capture counter restarts in step with the recorded prefix it replays.
                try (AutoCapture.Scope captured = AutoCapture.bind(ctx)) {
                    R result = task.execute(ctx);
                    // A task can swallow a stop signal (catch (RuntimeException) around a boundary) and
                    // return normally. That return is not a completion: the result came from the stop,
                    // and the boundary that raised it was never recorded. Honour the stop instead. A
                    // task that genuinely finishes without reaching another boundary raised nothing,
                    // and still records its completion.
                    // The same stop rule as the failure path below: a stopped attempt with a cancel
                    // pending is a cancel, whichever signal stopped it and in whatever order they came.
                    if (ctx.raisedCancellation() || (ctx.raisedAbandon() && runningAttempt.token.isCancelled())) {
                        recordCancelled(id, runningAttempt.token.reason(), future);
                        return;
                    }
                    if (ctx.raisedAbandon()) {
                        abandon(id, future);
                        return;
                    }
                    log.append(id, new CatalystEvent.ExecutionCompleted(now(), payloads.toTree(result)));
                    future.complete(result);
                    return;
                } catch (ExecutionPausedSignal pause) {
                    // ExecutionPaused already recorded by the context; leave the execution paused.
                    future.completeExceptionally(pause);
                    return;
                } catch (Throwable t) {
                    // The stop rule — one decision for every ordering of cancel and shutdown:
                    //   1. stopped by a signal (raised here, or unwound by one) with a cancel pending
                    //      → record CANCELLED. Cancel wins, whichever signal actually stopped the task:
                    //      a cancel that arrives after the abandon signal (the task swallowed it and was
                    //      still running its fallback) must not be undone by the next start's recovery.
                    //   2. otherwise stopped by the abandon signal → abandon: append nothing. That signal
                    //      is raised before a boundary records anything, so no request is left in doubt.
                    //   3. otherwise → the normal retry/fail decision below.
                    // "Stopped" is checked, not merely "token tripped". A cancel's interrupt that
                    // surfaces as an ordinary exception inside a boundary stopped nothing cooperatively;
                    // it gets the handling it always had (an interrupt never reclassifies a failure),
                    // which is terminal, so recovery cannot resume it either. And a GENUINE failure
                    // during the drain also falls to (3) — essential for model calls: the boundary
                    // records CompletionRequested before calling the provider, and only the
                    // RetryRequested or ExecutionFailed written below closes that request as a verdict.
                    // Abandoned instead, the restart would see it in doubt and, under
                    // InDoubtPolicy.FAIL, fail terminally over a known, unbilled failure. A retryable
                    // one writes its RetryRequested and is then abandoned instead of sleeping (the
                    // backoff wakes on shutdown), so the retry runs on the next start.
                    boolean stoppedByCancel = t instanceof CancellationSignal || ctx.raisedCancellation();
                    boolean stoppedByAbandon = t instanceof ExecutionAbandonedSignal || ctx.raisedAbandon();
                    if (stoppedByCancel || (stoppedByAbandon && runningAttempt.token.isCancelled())) {
                        recordCancelled(id, runningAttempt.token.reason(), future);
                        return;
                    }
                    if (stoppedByAbandon) {
                        abandon(id, future);
                        return;
                    }
                    // Decide retry vs. terminal failure. isRetryable gates out failures a retry can never
                    // fix (determinism bugs, in-doubt, interrupts, Errors) before the policy is consulted;
                    // a cancel that raced the failure wins (no phantom retry-then-cancel).
                    Optional<Duration> backoff = isRetryable(t)
                            ? policy.nextBackoff(state.retries(), t) : Optional.empty();
                    if (backoff.isEmpty() || runningAttempt.token.isCancelled()) {
                        recordFailed(id, t, future);
                        return;
                    }
                    long backoffMillis = Math.max(0, backoff.get().toMillis());
                    try {
                        // failedSeq lets the next seed() re-run the failing boundary live instead of
                        // substituting its recorded failure; -1 when the failure is not one (see
                        // ReplayingContext.failedBoundarySeq). Appended BEFORE the sleep so a crash mid-
                        // backoff preserves the budget; ExecutionResumed follows atop the next iteration.
                        log.append(id, new CatalystEvent.RetryRequested(
                                now(), String.valueOf(t), backoffMillis, ctx.failedBoundarySeq(t)));
                    } catch (RuntimeException appendFailed) {
                        // Cannot durably record the retry → do not retry; fail with the original cause.
                        recordFailed(id, t, future);
                        return;
                    }
                    if (!sleepUnlessStopped(backoffMillis, runningAttempt.token)) {
                        // RetryRequested is already durable, so an abandoned backoff resumes — with its
                        // retry budget intact — on the next start.
                        if (runningAttempt.token.isCancelled()) {
                            recordCancelled(id, runningAttempt.token.reason(), future);
                        } else {
                            abandon(id, future);
                        }
                        return;
                    }
                    // Clear any stray interrupt so it cannot leak a spurious InterruptedException into the
                    // next attempt's task code.
                    Thread.interrupted();
                }
            }
        } catch (Throwable setupFailure) {
            future.completeExceptionally(setupFailure);
        }
    }

    /**
     * True if {@code t} is a failure a retry could plausibly fix. Enforced before any {@link RetryPolicy}
     * is consulted, so a policy never sees — and cannot elect to spin on — a failure retry can't help:
     * a determinism divergence (a bug in the task), an in-doubt boundary (governed by
     * {@link InDoubtPolicy}, and re-thrown identically), an interrupt (a cancellation nudge, not a
     * transient fault), or an {@link Error} (OOM/StackOverflow, which retrying only worsens).
     *
     * <p>The <em>cause chain</em> is inspected, not just the throwable itself. None of these become
     * retryable by being wrapped, and wrapping is the norm rather than the exception: {@code
     * ReplayingContext.applyUnchecked} wraps a tool's checked exceptions in a {@code RuntimeException},
     * and a {@link com.cajunsystems.catalyst.model.Model} adapter must do the same because its SPI
     * declares none. Matching only the top-level type let an interrupt arrive dressed as a plain
     * {@code RuntimeException} and burn retry budget on a cancellation nudge.
     */
    private static boolean isRetryable(Throwable t) {
        // Bounded walk: a self- or mutually-referential cause chain would otherwise loop forever
        // (the same guard ReplayingContext.failedBoundarySeq uses).
        Throwable cause = t;
        for (int depth = 0; cause != null && depth < 64; cause = cause.getCause(), depth++) {
            if (cause instanceof NonDeterministicReplayException
                    || cause instanceof InDoubtException
                    || cause instanceof InterruptedException
                    || cause instanceof Error) {
                return false;
            }
        }
        return true;
    }

    /** Best-effort {@code ExecutionFailed} append (terminal), then completes the future with the cause. */
    private <R> void recordFailed(ExecutionId id, Throwable t, CompletableFuture<R> future) {
        try {
            log.append(id, new CatalystEvent.ExecutionFailed(now(), String.valueOf(t), log.latestSeq(id)));
        } catch (RuntimeException ignored) {
            // best-effort failure record; the future still completes exceptionally below
        }
        future.completeExceptionally(t);
    }

    /**
     * The shutdown unwind: completes the handle and appends <strong>nothing</strong>, leaving the
     * execution non-terminal in the log for the next start's recovery scan.
     */
    private <R> void abandon(ExecutionId id, CompletableFuture<R> future) {
        future.completeExceptionally(new ExecutionAbandonedSignal(
                "Runtime shut down; " + id + " is left for the next start to resume"));
    }

    /** Best-effort {@code ExecutionCancelled} append (terminal), then completes with CancellationException. */
    private <R> void recordCancelled(ExecutionId id, String reason, CompletableFuture<R> future) {
        try {
            log.append(id, new CatalystEvent.ExecutionCancelled(now(), reason, log.latestSeq(id)));
        } catch (RuntimeException ignored) {
            // best-effort cancellation record; the future still completes exceptionally below
        }
        future.completeExceptionally(new CancellationException(reason));
    }

    /**
     * Sleeps the backoff on the worker thread. Returns true to proceed with the next attempt, or false if
     * the attempt was cancelled — detected via the token (which {@code cancel(id)} trips before it
     * interrupts the worker), not via the interrupt flag. A spurious interrupt with no cancel is treated
     * as "keep going"; the caller then clears the flag so it cannot leak into the next attempt.
     */
    private boolean sleepUnlessStopped(long millis, CancellationToken token) {
        // Sliced so a shutdown wakes it too: abandon does not interrupt (an interrupt mid-boundary
        // would leave the boundary in doubt), so the sleep has to look for it.
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(0, millis));
        while (!token.isCancelled() && !token.isAbandoned()) {
            long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            if (remaining <= 0) return true;
            try {
                Thread.sleep(Math.min(remaining, BACKOFF_POLL_MILLIS));
            } catch (InterruptedException e) {
                // fall through: the token is the source of truth for whether this was a cancel
            }
        }
        return false;
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

    /**
     * Shuts down without turning in-flight work into failures. New submissions are refused, every
     * running attempt is told to abandon, and the runtime waits up to the drain timeout
     * ({@link Builder#drainTimeout}) for them to unwind before closing the log — which releases a
     * file log's directory lock for the next process.
     *
     * <p>Abandoning is not interrupting. A worker inside a model or tool call is allowed to finish it
     * and record the result; it unwinds at its <em>next</em> boundary, before recording anything,
     * appending nothing on the way out. The execution therefore stays {@code RUNNING} with a clean
     * tail, and a restart with {@code recoverOnStart} resumes it with zero duplicate calls.
     * (Interrupting instead would cut a provider call off after its request was recorded, leaving
     * a boundary that is in doubt on resume.)
     *
     * <p>A worker still busy when the drain timeout expires — a call slower than the timeout, or task
     * code that reaches no boundary — has the log closed under it. Its late writes fail and are
     * discarded; what it leaves behind is what a {@code kill -9} at that moment would, which recovery
     * already handles (a call in flight is governed by {@code InDoubtPolicy}). Size the timeout
     * above your slowest boundary.
     *
     * <p>Two consequences worth knowing. "The next start resumes it" assumes the task type is
     * registered there (as for any {@link #resume(ExecutionId)}); an unregistered one is reported in
     * {@link RecoveryReport#notRecovered()}. And only what the shutdown itself unwound is abandoned:
     * a call that fails for a genuine reason during the drain is recorded exactly as it would be
     * outside a shutdown — a {@code RetryRequested} if the retry policy allows (the retry then runs on
     * the next start), otherwise {@code ExecutionFailed} — so a failed model call is never left as an
     * in-doubt request.
     *
     * <p>Idempotent, and a barrier: a concurrent second call returns only once the first has closed
     * the log. Called from one of this runtime's own task threads, it cannot wait — the drain includes
     * the caller, which would be waiting on itself — so it hands the drain and the log close to
     * another thread and returns. Every attempt, the caller included, still gets the full drain: the
     * log is not closed under a task that is mid-call, which would leave its request in doubt.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            awaitFirstClose();
            return;
        }
        closing = true;
        executor.shutdown();
        if (calledFromTaskThread()) {
            Thread.ofVirtual().name("catalyst-close").start(this::drainAndCloseLog);
            return;
        }
        drainAndCloseLog();
    }

    /** Waits up to the drain timeout for running attempts, then closes the log and releases waiters. */
    private void drainAndCloseLog() {
        try {
            if (!awaitDrain()) {
                List<String> sample = running.keySet().stream()
                        .limit(DRAIN_WARNING_SAMPLE).map(ExecutionId::value).toList();
                LOG.warn("Shutdown drain timed out after {} with {} attempt(s) still running (e.g. {}); closing"
                        + " the log under them — they will be recovered on the next start",
                        drainTimeout, running.size(), sample);
            }
            log.close();
        } finally {
            closeFinished.countDown();
        }
    }

    /** Tears down a runtime whose {@code build()} failed after construction. */
    private void abortStartup(boolean closeLog) {
        closed.set(true);
        closing = true;
        executor.shutdown();
        closeFinished.countDown();
        if (closeLog) log.close();
    }

    private boolean awaitDrain() {
        try {
            return executor.awaitTermination(drainTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void awaitFirstClose() {
        if (calledFromTaskThread()) return; // the first close() may be waiting on this very thread
        try {
            closeFinished.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean calledFromTaskThread() {
        Thread current = Thread.currentThread();
        return running.values().stream().anyMatch(a -> a.thread == current);
    }

    // ── Builder ──────────────────────────────────────────────────────────────

    public static final class Builder {
        private EventLog log;
        private Model model;
        private Clock clock = Clock.systemUTC();
        private ReplayMode replayMode = ReplayMode.STRICT;
        private InDoubtPolicy inDoubtPolicy = InDoubtPolicy.FAIL;
        private CostModel costModel = CostModel.free();
        private RetryPolicy retryPolicy = RetryPolicy.none();
        private ObjectMapper eventMapper = EventJson.shared();
        private String nodeId = "node-0";
        private long snapshotInterval = 100;
        private TaskRegistry registry = new TaskRegistry();
        private boolean recoverOnStart;
        private EventLogFactory logFactory;
        private BlobStore blobStore;
        private int blobThresholdBytes = EventCodec.DEFAULT_BLOB_THRESHOLD_BYTES;
        private Duration drainTimeout = DEFAULT_DRAIN_TIMEOUT;

        /** Uses a log the caller already built. Exclusive with {@link #logFactory}. */
        public Builder log(EventLog log) {
            this.log = log;
            return this;
        }

        /**
         * Has {@code build()} construct the log, applying options that must be bound in at
         * construction (see {@link #blobStore(BlobStore)}). Exclusive with {@link #log(EventLog)}.
         */
        public Builder logFactory(EventLogFactory logFactory) {
            this.logFactory = logFactory;
            return this;
        }

        /**
         * Offloads payloads of {@link EventCodec#DEFAULT_BLOB_THRESHOLD_BYTES} or more to
         * {@code blobStore} — e.g. a remote S3/GCS store — instead of the backend's default. The log
         * owns the store and closes it when the runtime closes.
         *
         * <p>The store is bound into the log's codec when the log is constructed, so this applies only
         * when the builder constructs the log ({@link #logFactory}, as {@code Catalyst.builder(Path)}
         * does). Combined with {@link #log(EventLog)} it is <strong>rejected</strong> at
         * {@code build()} rather than silently ignored — pass the store to the log's own factory
         * instead (e.g. {@code GumboEventLog.at(path, store, threshold)}).
         */
        public Builder blobStore(BlobStore blobStore) {
            return blobStore(blobStore, EventCodec.DEFAULT_BLOB_THRESHOLD_BYTES);
        }

        /** As {@link #blobStore(BlobStore)}, offloading payloads at or over {@code thresholdBytes}. */
        public Builder blobStore(BlobStore blobStore, int thresholdBytes) {
            if (thresholdBytes <= 0) throw new IllegalArgumentException("thresholdBytes must be positive");
            this.blobStore = java.util.Objects.requireNonNull(blobStore, "blobStore");
            this.blobThresholdBytes = thresholdBytes;
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

        /**
         * The runtime-wide default retry policy (default {@link RetryPolicy#none()} — failures are
         * terminal on first throw). Overridable per execution via {@code ExecutionOptions.retryPolicy}.
         */
        public Builder retryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
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

        /**
         * How long {@link CatalystRuntime#close()} waits for running attempts to unwind before it
         * closes the log under them (default 10s). Set it above your slowest model/tool call, and
         * below the platform's kill deadline (Kubernetes' {@code terminationGracePeriodSeconds}
         * defaults to 30s) so the log is closed cleanly rather than by {@code SIGKILL}.
         */
        public Builder drainTimeout(Duration drainTimeout) {
            if (drainTimeout == null || drainTimeout.isNegative()) {
                throw new IllegalArgumentException("drainTimeout must be zero or positive");
            }
            this.drainTimeout = drainTimeout;
            return this;
        }

        /**
         * Runs {@link CatalystRuntime#recoverAll()} as part of {@code build()}, so a restarted node
         * resumes the executions its predecessor was running before it accepts new work. Register
         * every task type first — the scan reconstructs tasks from the registry. The report is
         * available from {@link CatalystRuntime#startupRecovery()}.
         *
         * <p>Requires a log with an execution index; {@code build()} refuses otherwise rather than
         * start a node that silently recovers nothing.
         */
        public Builder recoverOnStart(boolean recoverOnStart) {
            this.recoverOnStart = recoverOnStart;
            return this;
        }

        public CatalystRuntime build() {
            if (log != null && logFactory != null) {
                throw new IllegalStateException("Set either log(...) or logFactory(...), not both");
            }
            if (log != null && blobStore != null) {
                throw new IllegalStateException("blobStore(...) cannot apply to a log passed to log(...): the store"
                        + " is bound into the codec when the log is constructed. Pass it to the log's own factory"
                        + " (e.g. GumboEventLog.at(path, store, threshold)), or let the builder construct the log"
                        + " (Catalyst.builder(path)).");
            }
            if (log == null && logFactory == null) {
                throw new IllegalStateException("An EventLog is required (e.g. EventLogs.inMemory() or GumboEventLog.at(path))");
            }
            EventLog resolved;
            try {
                resolved = log != null ? log : logFactory.open(blobStore, blobThresholdBytes);
            } catch (RuntimeException e) {
                // The log takes ownership of the store only once it is constructed. If construction
                // failed, the store is still ours — and in the one-line usage the caller kept no
                // reference to it — so release it rather than leak its client.
                if (blobStore != null) blobStore.close();
                throw e;
            }
            if (recoverOnStart && !resolved.supportsExecutionIndex()) {
                IllegalStateException refused = new IllegalStateException("recoverOnStart needs a log with an"
                        + " execution index; " + resolved.getClass().getSimpleName() + " cannot list its open executions");
                if (log == null) resolved.close(); // the builder opened it, so the builder releases it
                throw refused;
            }
            CatalystRuntime runtime = new CatalystRuntime(this, resolved);
            if (recoverOnStart) {
                try {
                    runtime.startupRecovery = runtime.recoverAll();
                } catch (RuntimeException e) {
                    // Only the index read can get here (per-execution failures are reported), so no
                    // attempt is running. Stop the runtime, but close the log only if the builder
                    // opened it — a log the caller supplied is theirs, e.g. to retry build() with.
                    runtime.abortStartup(log == null);
                    throw e;
                }
            }
            return runtime;
        }
    }
}
