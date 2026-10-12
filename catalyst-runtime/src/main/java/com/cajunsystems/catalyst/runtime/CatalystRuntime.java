package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionInfo;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.ReplayMode;
import com.cajunsystems.catalyst.ShardRing;
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
import com.cajunsystems.catalyst.engine.ExecutionWaitingSignal;
import com.cajunsystems.catalyst.engine.StreamCursor;
import com.cajunsystems.catalyst.log.StaleWriterException;
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
    /** How long a node's claim on an execution lasts without renewal; see {@link Builder#lease}. */
    private static final Duration DEFAULT_LEASE_DURATION = Duration.ofSeconds(30);
    private static final Duration DEFAULT_LEASE_HEARTBEAT = Duration.ofSeconds(10);
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
    /** This node's shard and the ring it belongs to; both null for an unsharded runtime. */
    private final String shard;
    private final ShardRing shardRing;
    /** Built with {@link Builder#distributed()}: the log is shared with runtimes in other processes. */
    private final boolean distributed;
    /** This node's leases (Dist-3); null unless {@link #distributed}. */
    private final LeaseKeeper leases;

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
        /**
         * Set when a lease renewal finds another node has taken the execution over (Dist-3). Read as
         * an abandon flag alongside {@link #closing}: the attempt unwinds at its next boundary and
         * appends nothing.
         */
        volatile boolean leaseLost;
        final CancellationToken token = new CancellationToken(() -> closing || leaseLost);
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
        this.shard = b.shard;
        this.shardRing = b.shardRing;
        this.distributed = b.distributed;
        this.leases = b.distributed ? new LeaseKeeper(log, b.nodeId, b.clock, b.leaseDuration, b.leaseHeartbeat) : null;
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
        requireRoutedHere(key);

        // No key → a fresh random id no caller can name yet, so only the id lock is needed. Creation
        // happens UNDER it, not before: ExecutionCreated puts the id in the execution index, and a
        // concurrent recoverAll() that saw it there — open, STARTING, nothing in flight — would resume
        // it and leave this FRESH attempt as a second writer on the same stream.
        if (key.isEmpty()) {
            ExecutionId id = mintId();
            return byExecution.withLock(id, () -> underLease(id, () -> {
                createExecution(id, taskType, "", opts);
                return scheduleAttempt(task, id, taskType, opts, Mode.FRESH);
            }));
        }

        // Keyed: hold the key lock across the whole decision — resolving the key, creating the
        // execution and scheduling its attempt. Releasing it once the id is known would leave a window
        // where a same-key submission resolves to that id and schedules a second attempt for it before
        // this one has registered its future. Lock order is key → id (see byIdempotencyKey).
        return byIdempotencyKey.withLock(key.get(), () -> {
            Optional<ExecutionId> existing = log.findByKey(key.get());
            if (existing.isEmpty()) {
                ExecutionId id = mintId();
                log.putKey(key.get(), id);
                // Created under the id lock for the same reason as the keyless path above.
                return byExecution.withLock(id, () -> underLease(id, () -> {
                    createExecution(id, taskType, key.get(), opts);
                    return scheduleAttempt(task, id, taskType, opts, Mode.FRESH);
                }));
            }
            ExecutionId id = existing.get();
            return byExecution.withLock(id, () -> {
                // If an attempt for this execution is already running in this process, attach to it
                // rather than scheduling a duplicate attempt that would corrupt the event stream.
                CompletableFuture<?> live = liveAttempt(id);
                if (live != null) {
                    return new FutureExecutionHandle<>(id, CatalystRuntime.<R>cast(live));
                }
                ExecutionState state = foldState(id);
                if (state.status() == Status.WAITING) return waitingHandle(id, state);
                Mode mode = state.isTerminal() ? Mode.REPLAY_TERMINAL : Mode.RESUME;
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
        // A distributed runtime runs an execution only while holding its lease: claimed here, under the
        // id lock, before anything is registered or written, and released when the attempt ends —
        // whatever the outcome, waiting and paused included. A live lease held by another node throws
        // LeaseHeldException: the execution is running there. Replaying a terminal outcome writes
        // nothing, so it needs no lease.
        if (leases != null && mode != Mode.REPLAY_TERMINAL) leases.acquire(id);
        CompletableFuture<R> future = new CompletableFuture<>(); // completed by runAttempt
        RunningAttempt attempt = new RunningAttempt();
        if (leases != null) leases.onLost(id, () -> attempt.leaseLost = true);
        // Every caller — this handle, and anyone who attaches via inFlight — sees `visible`, which
        // completes only AFTER the attempt is deregistered. Completing the attempt's own future first
        // and deregistering in a dependent let a caller observe the result while the attempt still
        // looked in flight: an immediate signal() was refused, or its resume() attached to the finished
        // attempt and scheduled nothing. The unconditional inFlight.remove is safe: no other attempt
        // for this id can be scheduled while `visible` is not yet done.
        CompletableFuture<R> visible = future.whenComplete((r, t) -> {
            // Released before deregistering, so a caller who sees the handle complete can take the
            // execution over from another node at once.
            if (leases != null && running.get(id) == attempt) leases.release(id);
            if (running.remove(id, attempt)) inFlight.remove(id);
        });
        inFlight.put(id, visible);
        running.put(id, attempt);
        try {
            executor.execute(() -> runAttempt(task, id, taskType, opts, mode, future, attempt));
        } catch (RejectedExecutionException e) {
            // Lost a race with close(): the executor stopped between requireOpen and here. Nothing ran
            // and nothing was appended for this attempt, so the execution is as recoverable as any
            // other open one — the next start resumes it.
            future.completeExceptionally(notStartedHere(id, e));
        }
        return new FutureExecutionHandle<>(id, visible);
    }

    /**
     * Runs {@code body} holding the lease on {@code id}, when this runtime is distributed: for the
     * paths that write to a stream outside an attempt (creating, signalling, pausing, cancelling). If
     * {@code body} scheduled an attempt, that attempt now owns the lease and releases it when it ends.
     * Otherwise it is released here. Called under the id lock.
     *
     * @throws LeaseHeldException if another node holds a live lease on {@code id}
     */
    private <T> T underLease(ExecutionId id, java.util.function.Supplier<T> body) {
        if (leases == null) return body.get();
        boolean claimedHere = leases.acquire(id);
        try {
            return body.get();
        } finally {
            if (claimedHere && !running.containsKey(id)) leases.release(id);
        }
    }

    /** The attempt running for {@code id} in this process, or null; a finished one does not count. */
    private CompletableFuture<?> liveAttempt(ExecutionId id) {
        CompletableFuture<?> live = inFlight.get(id);
        return live != null && !live.isDone() ? live : null;
    }

    private static IllegalStateException notStartedHere(ExecutionId id, Throwable cause) {
        return new IllegalStateException("Runtime is shutting down; " + id + " was not started here and"
                + " will be resumed on the next start if its task type is registered", cause);
    }

    /** A fresh id, stamped with this node's shard when the runtime is one shard of several. */
    private ExecutionId mintId() {
        return shard == null ? ExecutionId.random() : ExecutionId.random(shard);
    }

    /**
     * A sharded runtime accepts only work routed to it, checked before anything is written. Without a
     * key there is nothing to route on — the id does not exist yet — and a key that routes to another
     * shard means the caller's router disagrees with this ring; accepting it would put the key's
     * execution where a later, correctly routed re-submission will not find it, duplicating every
     * model call. Refusing is the only safe answer to both.
     */
    private void requireRoutedHere(Optional<String> key) {
        if (shard == null) return;
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Sharded runtime (shard '" + shard + "') requires an idempotency"
                    + " key on every submission: it is what routes the work to a shard."
                    + " Use ExecutionOptions.withKey(...)");
        }
        String owner = shardRing.shardFor(key.get());
        if (!owner.equals(shard)) {
            throw new IllegalArgumentException("Idempotency key '" + key.get() + "' routes to shard '" + owner
                    + "', not this node's shard '" + shard + "'; submit it there (ShardRing.shardFor)");
        }
    }

    /**
     * An id stamped with another shard names an execution in another node's log: here it would read
     * as unknown, or be resumed by the wrong node. Refuse instead. Unstamped ids — minted before the
     * deployment was sharded, or by an unsharded runtime — are allowed through.
     */
    private void requireOwned(ExecutionId id) {
        if (shard == null) return;
        id.shard().ifPresent(owner -> {
            if (!owner.equals(shard)) {
                throw new IllegalArgumentException("Execution " + id + " belongs to shard '" + owner
                        + "', not this node's shard '" + shard + "'; route it there (ShardRing.shardOf)");
            }
        });
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

    /**
     * Whether this runtime was built with {@link Builder#distributed()}, sharing its log with runtimes
     * in other processes. When true, {@code build()} has already checked that the log is fenced and
     * multi-writer.
     */
    public boolean isDistributed() {
        return distributed;
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
        requireOwned(id);
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
        requireOwned(id);
        List<SequencedEvent> recorded = log.read(id);
        ExecutionState state = Reducer.fold(id, recorded);
        ExecutionInfo info = new ExecutionInfo(id, state.attempt(),
                state.taskType() != null ? state.taskType() : task.getClass().getName(), opts.metadata());
        Logger logger = LoggerFactory.getLogger("catalyst.replay." + id.value());
        ReplayingContext ctx = new ReplayingContext(id, log, defaultModel, info, opts.vars(),
                eventMapper, payloads, inDoubtPolicy, costModel, replayMode, null, clock, logger, recorded, /* appendEnabled */ false);
        try (AutoCapture.Scope captured = AutoCapture.bind(ctx)) {
            task.execute(ctx);
        } catch (ExecutionWaitingSignal stillWaiting) {
            // The recording ends at a wait with no signal yet: the replay reached the end of the log.
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
        requireOwned(id);
        return byExecution.withLock(id, () -> {
            // Already running here → attach to the in-flight attempt rather than scheduling a duplicate
            // that would interleave events and corrupt the stream (same guard as execute()).
            CompletableFuture<?> live = liveAttempt(id);
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
            // Still waiting for a signal: an attempt would only replay to the wait and park again, and
            // its ExecutionResumed would fold the status to RUNNING while it is really waiting.
            if (state.status() == Status.WAITING) return waitingHandle(id, state);

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
     * <p>On a {@linkplain Builder#distributed() distributed} runtime the index is shared, so it lists
     * work running on other nodes too. Each resume claims the execution's lease first, and one that
     * another node holds live is left alone and reported {@code RUNNING}: it is running there. One
     * whose holder died has a lapsed lease and is taken over.
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
                    if (liveAttempt(id) != null) {
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
                    try {
                        resumed.put(id, resume(id)); // the lock is reentrant
                    } catch (LeaseHeldException held) {
                        leftAlone.put(id, Status.RUNNING); // running on held.lease().holder()
                    }
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
     * Delivers signal {@code name} to an execution waiting for it ({@code ctx.awaitSignal}) and
     * resumes it. The payload is recorded as the result of the await boundary, so it is substituted on
     * every later resume or replay.
     *
     * <p>Accepted only while the execution is {@code WAITING} for exactly this signal. Delivery to a
     * running execution is refused rather than buffered: nothing may append to a running stream except
     * its own worker. A task that has not reached its wait yet is not waiting; retry once it is
     * ({@code inspect(id).awaitingSignal()}).
     *
     * <p>Recording the signal folds the execution to {@code RESUMING}. It is then resumed here if its
     * task type is registered; if the process dies first, startup recovery resumes it. If the type is
     * not registered, the signal is still recorded and the returned handle fails — resume it by
     * re-submitting the task with its idempotency key.
     *
     * <p>The resume runs with no task input vars or per-execution options, as {@link #resume(ExecutionId)}
     * does. A task that reads {@code ctx.var(...)}, or ran with its own retry policy, must be signalled
     * with {@link #signal(ExecutionId, String, Object, ExecutionOptions)} and the same options it was
     * submitted with — otherwise its re-entry takes a different path and fails the replay check.
     *
     * @return the resumed execution's handle
     * @throws IllegalStateException if the execution is not waiting for {@code name}
     */
    public ExecutionHandle<?> signal(ExecutionId id, String name, Object payload) {
        return signal(id, name, payload, ExecutionOptions.none());
    }

    /**
     * As {@link #signal(ExecutionId, String, Object)}, resuming with {@code opts} — the task input vars
     * and retry policy the execution was submitted with, which the log does not persist.
     */
    public ExecutionHandle<?> signal(ExecutionId id, String name, Object payload, ExecutionOptions opts) {
        java.util.Objects.requireNonNull(opts, "opts");
        requireOpen();
        requireOwned(id);
        java.util.Objects.requireNonNull(name, "name");
        return byExecution.withLock(id, () -> {
            if (liveAttempt(id) != null) {
                throw new IllegalStateException("Execution " + id + " is running, not waiting for signal '" + name + "'");
            }
            // Held across the checks, the append and the resume, so no other node writes in between.
            return underLease(id, () -> signalUnderLock(id, name, payload, opts));
        });
    }

    /** {@link #signal}'s checks, append and resume; called under the id lock and the lease. */
    private ExecutionHandle<?> signalUnderLock(ExecutionId id, String name, Object payload, ExecutionOptions opts) {
        ExecutionState state = foldState(id);
        Optional<String> waitingFor = state.awaitingSignal();
        if (waitingFor.isEmpty() || !waitingFor.get().equals(name)) {
            throw new IllegalStateException("Execution " + id + " is " + state.status()
                    + (waitingFor.map(w -> " waiting for signal '" + w + "'").orElse(""))
                    + ", not waiting for signal '" + name + "'");
        }
        // A branch child records no task type (its stream starts with ExecutionBranched) and cannot be
        // resumed — its overrides are not recorded. Refuse before writing: a recorded signal nothing
        // can consume would strand it in RESUMING.
        if (state.taskType() == null) {
            throw new IllegalStateException("Execution " + id + " has no recorded task type (a branch child)"
                    + " and cannot be resumed, so it cannot accept signal '" + name + "'");
        }
        // Refuse a payload the resume could never accept — before writing it. Once recorded it would be
        // substituted into every resume and fail each one, terminally, on an append-only log.
        com.fasterxml.jackson.databind.JsonNode tree = payloads.toTree(payload);
        Object decoded;
        try {
            decoded = payloads.fromTree(tree); // the allowlist the resume will apply
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Signal '" + name + "' payload cannot be recorded: " + e.getMessage()
                    + " Use a record, enum, string, number, java.time value or UUID.", e);
        }
        requirePayloadType(id, name, state.awaitingSignalType(), decoded);
        // Fenced on the fold the checks above were made against.
        StreamCursor.after(log, id, state.lastSeq()).append(new CatalystEvent.SignalReceived(now(), name, tree));
        if (registry.lookup(state.taskType()).isEmpty()) {
            return new FutureExecutionHandle<>(id, CompletableFuture.failedFuture(new IllegalStateException(
                    "Signal '" + name + "' recorded for " + id + ", but no task is registered for type '"
                            + state.taskType() + "' to resume it here; re-submit the task with its"
                            + " idempotency key, or register it with Catalyst.builder().task(...)")));
        }
        return resume(id, opts); // reentrant on the id lock, and on the lease
    }

    /** Refuses a payload that is not of the type the task awaits; a type not loadable here is not checked. */
    private static void requirePayloadType(ExecutionId id, String name, Optional<String> typeName, Object payload) {
        if (payload == null || typeName.isEmpty()) return;
        Class<?> expected;
        try {
            expected = Class.forName(typeName.get(), false, CatalystRuntime.class.getClassLoader());
        } catch (ClassNotFoundException | LinkageError e) {
            return; // cannot verify from this process; the resume's own check still applies
        }
        if (!expected.isInstance(payload)) {
            throw new IllegalArgumentException("Execution " + id + " waits for signal '" + name + "' of type "
                    + expected.getName() + ", not " + payload.getClass().getName());
        }
    }

    /** A handle for an execution parked on a signal: no attempt runs; it completes with the wait. */
    private static <R> ExecutionHandle<R> waitingHandle(ExecutionId id, ExecutionState state) {
        String name = state.awaitingSignal().orElse(null);
        return new FutureExecutionHandle<>(id, CompletableFuture.<R>failedFuture(new ExecutionWaitingSignal(name,
                "Execution " + id + " is waiting for signal '" + name + "'")));
    }

    /**
     * Forks a recorded execution at {@code atSeq} to explore an alternative (spec §7): the returned
     * {@link BranchBuilder} lets you swap the model, substitute counterfactual tool results, or
     * supply vars, then {@code run(task)} it forward into a new child execution and get its
     * {@link Trajectory}.
     */
    public BranchBuilder branch(ExecutionId id, long atSeq) {
        requireOwned(id);
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

        ExecutionId childId = mintId();
        String components = changedComponents == null || changedComponents.isBlank()
                ? "branch@" + atSeq : changedComponents;
        BranchSpec spec = new BranchSpec(atSeq, parentId.value(), overrides, components);

        // The child log begins with the branch marker + started (the fork itself won't re-record it).
        StreamCursor childCursor = StreamCursor.after(log, childId, -1);
        childCursor.append(new CatalystEvent.ExecutionBranched(now(), parentId.value(), atSeq, components));
        childCursor.append(new CatalystEvent.ExecutionStarted(now(), 1, nodeId));

        String taskType = parentState.taskType() != null ? parentState.taskType() : task.getClass().getName();
        ExecutionInfo info = new ExecutionInfo(childId, 1, taskType, Map.of());
        Logger logger = LoggerFactory.getLogger("catalyst.branch." + childId.value());
        Model model = overrideModel != null ? overrideModel : defaultModel;

        ReplayingContext ctx = new ReplayingContext(childId, log, model, info, vars,
                eventMapper, payloads, inDoubtPolicy, costModel, ReplayMode.BRANCH, spec, clock, logger,
                parentEvents, /* appendEnabled */ true, null, childCursor);
        try (AutoCapture.Scope captured = AutoCapture.bind(ctx)) {
            R result = task.execute(ctx);
            childCursor.append(new CatalystEvent.ExecutionCompleted(now(), payloads.toTree(result)));
        } catch (ExecutionPausedSignal pause) {
            // ExecutionPaused already recorded by the context (e.g. an ASK in-doubt tool); the child
            // is left PAUSED — do NOT append a spurious ExecutionFailed on top of it.
            throw pause;
        } catch (ExecutionWaitingSignal waiting) {
            // Likewise SignalAwaited: the child is left WAITING, not failed.
            throw waiting;
        } catch (RuntimeException e) {
            childCursor.append(new CatalystEvent.ExecutionFailed(now(), String.valueOf(e), childCursor.lastSeq()));
            throw e;
        } catch (Exception e) {
            childCursor.append(new CatalystEvent.ExecutionFailed(now(), String.valueOf(e), childCursor.lastSeq()));
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
        requireOwned(id);
        byExecution.withLock(id, () -> {
            ExecutionState state = inspect(id);
            if (state.isTerminal()) return;  // terminal (incl. just-completed) → no-op
            if (liveAttempt(id) != null) {
                throw new IllegalStateException("Cannot pause an execution running in this process: " + id);
            }
            // Fenced on the fold just read: a writer elsewhere that moved the stream on makes this refuse.
            // Distributed: only under the lease — refused while another node is running it.
            underLease(id, () -> StreamCursor.after(log, id, state.lastSeq())
                    .append(new CatalystEvent.ExecutionPaused(now(), "paused by request")));
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
        requireOwned(id);
        byExecution.withLock(id, () -> {
            ExecutionState state = inspect(id);
            if (state.isTerminal()) return;  // terminal (incl. just-completed) → no-op
            RunningAttempt attempt = running.get(id);
            if (attempt != null) {
                // Cooperative: the worker thread — the only writer for a running execution — records the
                // ExecutionCancelled when it observes the trip. Interrupt to unblock a task parked in I/O.
                attempt.token.cancel("cancelled by request");
                Thread worker = attempt.thread;
                if (worker != null) worker.interrupt();
                return;
            }
            // Fenced on the fold just read, and under the lease, like pause().
            underLease(id, () -> StreamCursor.after(log, id, state.lastSeq())
                    .append(new CatalystEvent.ExecutionCancelled(now(), "cancelled by request", state.lastSeq())));
        });
    }

    public EventLog log() {
        return log;
    }

    // ── Internals ────────────────────────────────────────────────────────────

    private void createExecution(ExecutionId id, String taskType, String key, ExecutionOptions opts) {
        String configFingerprint = "replayMode=" + replayMode + ";inDoubt=" + inDoubtPolicy;
        // A new stream: fenced at seq 0, so it is created exactly once even if two writers mint the same id.
        StreamCursor.after(log, id, -1)
                .append(new CatalystEvent.ExecutionCreated(now(), taskType, argsHash(opts.vars()), configFingerprint, key));
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

            // This attempt's position in the stream: one past what it just read, advanced only by its own
            // appends — lifecycle events here and boundary events in the context alike. Over a log that
            // can fence, every append is conditioned on it, so another writer that has moved the stream on
            // makes this attempt's next append fail instead of interleaving with it (Dist-1).
            StreamCursor cursor = StreamCursor.after(log, id, recorded);

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
                    abandon(id, future, runningAttempt);
                    return;
                }
                int attempt = state.attempt() + 1;
                if (firstIteration && mode == Mode.FRESH) {
                    cursor.append(new CatalystEvent.ExecutionStarted(now(), attempt, nodeId));
                } else {
                    // A retry IS a resume: it re-enters the task with the recorded prefix substituted.
                    cursor.append(new CatalystEvent.ExecutionResumed(now(), attempt));
                }
                firstIteration = false;

                String effectiveTaskType = state.taskType() != null ? state.taskType() : taskType;
                ExecutionInfo info = new ExecutionInfo(id, attempt, effectiveTaskType, opts.metadata());
                Logger logger = LoggerFactory.getLogger("catalyst.exec." + id.value());

                ReplayingContext ctx = new ReplayingContext(id, log, defaultModel, info, opts.vars(),
                        eventMapper, payloads, inDoubtPolicy, costModel, replayMode, null, clock, logger,
                        recorded, /* appendEnabled */ true, runningAttempt.token, cursor);

                // The capture scope is per-attempt: a retry re-enters the task from the top, so its
                // auto-capture counter restarts in step with the recorded prefix it replays.
                try (AutoCapture.Scope captured = AutoCapture.bind(ctx)) {
                    R result = task.execute(ctx);
                    // Fenced out mid-task (the task swallowed the rejection): this writer no longer owns the
                    // stream, so nothing it decides from here may be recorded.
                    if (cursor.fencedOut() != null) {
                        future.completeExceptionally(cursor.fencedOut());
                        return;
                    }
                    // A task can swallow a stop signal (catch (RuntimeException) around a boundary) and
                    // return normally. That return is not a completion: the result came from the stop,
                    // and the boundary that raised it was never recorded. Honour the stop instead. A
                    // task that genuinely finishes without reaching another boundary raised nothing,
                    // and still records its completion.
                    // The same stop rule as the failure path below: a stopped attempt with a cancel
                    // pending is a cancel, whichever signal stopped it and in whatever order they came.
                    boolean stopped = ctx.raisedAbandon() || ctx.raisedWaiting();
                    if (ctx.raisedCancellation() || (stopped && runningAttempt.token.isCancelled())) {
                        recordCancelled(cursor, runningAttempt.token.reason(), future);
                        return;
                    }
                    if (ctx.raisedAbandon()) {
                        abandon(id, future, runningAttempt);
                        return;
                    }
                    if (ctx.raisedWaiting()) {
                        parkUnlessCancelled(cursor, future, runningAttempt, ctx.waitingSignal());
                        return;
                    }
                    cursor.append(new CatalystEvent.ExecutionCompleted(now(), payloads.toTree(result)));
                    future.complete(result);
                    return;
                } catch (ExecutionPausedSignal pause) {
                    // ExecutionPaused already recorded by the context; leave the execution paused —
                    // unless a cancel landed meanwhile (see parkUnlessCancelled).
                    parkUnlessCancelled(cursor, future, runningAttempt, pause);
                    return;
                } catch (Throwable t) {
                    // Fenced out (Dist-1): another writer has taken this stream over — a resume elsewhere,
                    // or a newer attempt — and storage refused this one's append. Its own exit, ahead of
                    // the stop rule and the retry decision: append NOTHING, not even ExecutionFailed, which
                    // the fence would refuse anyway on a stream this writer no longer owns, and which
                    // recordFailed would otherwise try to write. The new owner decides what happens next.
                    if (cursor.fencedOut() != null) {
                        future.completeExceptionally(cursor.fencedOut());
                        return;
                    }
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
                        recordCancelled(cursor, runningAttempt.token.reason(), future);
                        return;
                    }
                    if (stoppedByAbandon) {
                        abandon(id, future, runningAttempt);
                        return;
                    }
                    // Parked on a signal: SignalAwaited is recorded, so append nothing more — the execution
                    // stays WAITING with no thread held until signal() delivers it. Unless a cancel is
                    // pending: cancel() saw this attempt still registered as running and only tripped
                    // the token, so parking would leave nobody to record the cancel.
                    if (t instanceof ExecutionWaitingSignal || ctx.raisedWaiting()) {
                        parkUnlessCancelled(cursor, future, runningAttempt,
                                t instanceof ExecutionWaitingSignal w ? w : ctx.waitingSignal());
                        return;
                    }
                    // Decide retry vs. terminal failure. isRetryable gates out failures a retry can never
                    // fix (determinism bugs, in-doubt, interrupts, Errors) before the policy is consulted;
                    // a cancel that raced the failure wins (no phantom retry-then-cancel).
                    Optional<Duration> backoff = isRetryable(t)
                            ? policy.nextBackoff(state.retries(), t) : Optional.empty();
                    if (backoff.isEmpty() || runningAttempt.token.isCancelled()) {
                        recordFailed(cursor, t, future);
                        return;
                    }
                    long backoffMillis = Math.max(0, backoff.get().toMillis());
                    try {
                        // failedSeq lets the next seed() re-run the failing boundary live instead of
                        // substituting its recorded failure; -1 when the failure is not one (see
                        // ReplayingContext.failedBoundarySeq). Appended BEFORE the sleep so a crash mid-
                        // backoff preserves the budget; ExecutionResumed follows atop the next iteration.
                        cursor.append(new CatalystEvent.RetryRequested(
                                now(), String.valueOf(t), backoffMillis, ctx.failedBoundarySeq(t)));
                    } catch (RuntimeException appendFailed) {
                        if (cursor.fencedOut() != null) {
                            future.completeExceptionally(cursor.fencedOut());
                            return;
                        }
                        // Cannot durably record the retry → do not retry; fail with the original cause.
                        recordFailed(cursor, t, future);
                        return;
                    }
                    if (!sleepUnlessStopped(backoffMillis, runningAttempt.token)) {
                        // RetryRequested is already durable, so an abandoned backoff resumes — with its
                        // retry budget intact — on the next start.
                        if (runningAttempt.token.isCancelled()) {
                            recordCancelled(cursor, runningAttempt.token.reason(), future);
                        } else {
                            abandon(id, future, runningAttempt);
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
    private <R> void recordFailed(StreamCursor cursor, Throwable t, CompletableFuture<R> future) {
        try {
            cursor.append(new CatalystEvent.ExecutionFailed(now(), String.valueOf(t), cursor.lastSeq()));
        } catch (StaleWriterException fencedOut) {
            future.completeExceptionally(fencedOut); // not this writer's stream any more: record nothing
            return;
        } catch (RuntimeException ignored) {
            // best-effort failure record; the future still completes exceptionally below
        }
        future.completeExceptionally(t);
    }

    /**
     * Ends an attempt that parked — waiting for a signal, or paused — leaving the execution
     * non-terminal, unless a cancel is pending, which wins.
     *
     * <p>The check and the deregistration are made atomic with respect to {@code cancel()} by holding
     * the id lock {@code cancel()} decides under. Deregistration runs synchronously inside
     * {@code completeExceptionally} (the dependent was registered when the attempt was scheduled), so
     * it happens under the lock too. Without that, a {@code cancel()} landing between the check and the
     * deregistration would see the attempt still registered, only trip its token, and return — and
     * nothing would ever record the cancel. Now it lands either before (seen here, recorded) or after
     * (the attempt is gone, so {@code cancel()} appends {@code ExecutionCancelled} itself).
     */
    private <R> void parkUnlessCancelled(StreamCursor cursor, CompletableFuture<R> future,
                                         RunningAttempt attempt, RuntimeException parked) {
        byExecution.withLock(cursor.executionId(), () -> {
            if (attempt.token.isCancelled()) {
                recordCancelled(cursor, attempt.token.reason(), future);
            } else {
                future.completeExceptionally(parked);
            }
        });
    }

    /**
     * The shutdown unwind: completes the handle and appends <strong>nothing</strong>, leaving the
     * execution non-terminal in the log for the next start's recovery scan.
     */
    private <R> void abandon(ExecutionId id, CompletableFuture<R> future, RunningAttempt attempt) {
        if (attempt.leaseLost && !closing) {
            future.completeExceptionally(new LeaseLostException(id));
            return;
        }
        future.completeExceptionally(new ExecutionAbandonedSignal(
                "Runtime shut down; " + id + " is left for the next start to resume"));
    }

    /** Best-effort {@code ExecutionCancelled} append (terminal), then completes with CancellationException. */
    private <R> void recordCancelled(StreamCursor cursor, String reason, CompletableFuture<R> future) {
        try {
            cursor.append(new CatalystEvent.ExecutionCancelled(now(), reason, cursor.lastSeq()));
        } catch (StaleWriterException fencedOut) {
            future.completeExceptionally(fencedOut); // not this writer's stream any more: record nothing
            return;
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
            // After the drain: attempts that unwound have released their own leases; this releases
            // any still running past the drain timeout, so other nodes need not wait them out.
            if (leases != null) leases.close();
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
        if (leases != null) leases.close();
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
        private String shard;
        private ShardRing shardRing;
        private boolean distributed;
        private boolean nodeIdSet;
        private Duration leaseDuration = DEFAULT_LEASE_DURATION;
        private Duration leaseHeartbeat = DEFAULT_LEASE_HEARTBEAT;
        private boolean leaseSet;

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

        /**
         * This runtime's node id, recorded in {@code ExecutionStarted} (default {@code "node-0"}). A
         * {@linkplain #distributed() distributed} runtime must set it, and every node needs a different one.
         */
        public Builder nodeId(String nodeId) {
            if (nodeId == null || nodeId.isBlank()) throw new IllegalArgumentException("nodeId must be non-blank");
            this.nodeId = nodeId;
            this.nodeIdSet = true;
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
         * Makes this runtime shard {@code self} of {@code ring} in a sharded deployment: independent
         * nodes, each with its own log, with work routed by idempotency key ({@link ShardRing}).
         *
         * <p>A sharded runtime then requires an idempotency key on every {@code execute}, refuses a key
         * that {@code ring} routes to another shard, stamps {@code self} into every id it mints, and
         * refuses id-addressed calls for an id stamped with another shard. All of it is checked before
         * anything is written. Every node must be built with the same ring.
         */
        public Builder shard(String self, ShardRing ring) {
            java.util.Objects.requireNonNull(ring, "ring");
            if (!ring.contains(self)) {
                throw new IllegalArgumentException("shard '" + self + "' is not in " + ring);
            }
            this.shard = self;
            this.shardRing = ring;
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

        /**
         * Declares that this runtime shares its log with runtimes in other processes (the Dist
         * track). {@code build()} then refuses to start unless all of these hold:
         * <ul>
         *   <li>The log reports {@link EventLog#supportsConditionalAppend()}, so a stale writer is
         *       refused rather than appended.</li>
         *   <li>The log reports {@link EventLog#supportsMultiWriter()}, so writers in different
         *       processes are safe at all. A file-backed log is fenced but not multi-writer.</li>
         *   <li>The log reports {@link EventLog#supportsLeases()}, so nodes can tell which of them
         *       is running an execution.</li>
         *   <li>A {@link #nodeId(String)} was set. Two nodes left at the default would be
         *       indistinguishable in the log.</li>
         *   <li>{@link #shard} is not set. Sharding is the alternative to sharing a log: each shard
         *       has its own.</li>
         * </ul>
         * The capabilities are read from the log, never assumed. A log that does not report them is
         * refused even if it could in fact handle several writers.
         *
         * <p>What this mode gives:
         * <ul>
         *   <li>Every append is fenced (Dist-1), so runtimes sharing the log cannot corrupt a
         *       stream.</li>
         *   <li>Every write is made under the execution's lease (Dist-3), claimed before the first
         *       write and renewed on a heartbeat ({@link #lease}). An execution another node is
         *       running is refused with {@link LeaseHeldException} rather than started a second time.
         *       {@link CatalystRuntime#recoverAll()} skips what other nodes hold and takes over what
         *       dead nodes left.</li>
         * </ul>
         * The claim loop, which keeps watching for work instead of only scanning at startup, is Dist-4.
         */
        public Builder distributed() {
            this.distributed = true;
            return this;
        }

        /**
         * A {@linkplain #distributed() distributed} runtime's lease timing (defaults 30s and 10s).
         * {@code duration} is how long a claim lasts unrenewed: after a node dies, its executions wait
         * this long before another node may take them. {@code heartbeat} is how often a node renews
         * the leases it holds.
         *
         * <p>Too short a duration lets a GC pause or a slow renewal hand a live node's work to
         * another node. The fence keeps that from corrupting anything, but the work is wasted, and
         * the paused node's in-flight call is re-issued. Too long delays failover. Keep the heartbeat
         * at a third of the duration or less, so a renewal can fail twice before the lease lapses.
         * Clock skew between nodes eats into the margin. Refused on a runtime that is not distributed.
         */
        public Builder lease(Duration duration, Duration heartbeat) {
            if (duration == null || heartbeat == null || heartbeat.isNegative() || heartbeat.isZero()) {
                throw new IllegalArgumentException("lease duration and heartbeat must be positive");
            }
            if (heartbeat.compareTo(duration) >= 0) {
                throw new IllegalArgumentException("lease heartbeat (" + heartbeat + ") must be shorter than its"
                        + " duration (" + duration + "), or every lease lapses before it is renewed");
            }
            this.leaseDuration = duration;
            this.leaseHeartbeat = heartbeat;
            this.leaseSet = true;
            return this;
        }

        public CatalystRuntime build() {
            // Refusals that need no log come first, so nothing is opened only to be closed again.
            if (distributed) {
                if (shardRing != null) {
                    throw new IllegalStateException("distributed() and shard(...) are alternatives: a sharded"
                            + " deployment gives each node its own log, a distributed one shares one log");
                }
                if (!nodeIdSet) {
                    throw new IllegalStateException("A distributed runtime needs nodeId(...), unique per node:"
                            + " nodes left at the default '" + nodeId + "' are indistinguishable in a shared log");
                }
            }
            if (leaseSet && !distributed) {
                throw new IllegalStateException("lease(...) applies only to a distributed() runtime;"
                        + " a runtime that does not share its log takes no leases");
            }
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
            if (distributed && !(resolved.supportsConditionalAppend() && resolved.supportsMultiWriter()
                    && resolved.supportsLeases())) {
                IllegalStateException refused = new IllegalStateException("distributed() needs a log that is"
                        + " fenced, multi-writer and stores leases; " + resolved.getClass().getSimpleName() + " reports"
                        + " supportsConditionalAppend()=" + resolved.supportsConditionalAppend()
                        + ", supportsMultiWriter()=" + resolved.supportsMultiWriter()
                        + ", supportsLeases()=" + resolved.supportsLeases()
                        + ". Use a multi-writer log such as GumboEventLog.foundationDB(...).");
                if (log == null) resolved.close(); // the builder opened it, so the builder releases it
                throw refused;
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
