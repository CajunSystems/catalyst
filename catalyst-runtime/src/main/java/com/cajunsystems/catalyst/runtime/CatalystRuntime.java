package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionInfo;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.ReplayMode;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.ExecutionPausedSignal;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.InDoubtPolicy;
import com.cajunsystems.catalyst.engine.PayloadCodec;
import com.cajunsystems.catalyst.engine.Reducer;
import com.cajunsystems.catalyst.engine.ReplayingContext;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.EventJson;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.model.Model;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
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
    private final ObjectMapper eventMapper;
    private final PayloadCodec payloads;
    private final String nodeId;
    private final ExecutorService executor;

    private enum Mode { FRESH, RESUME, REPLAY_TERMINAL }

    private CatalystRuntime(Builder b) {
        this.log = b.log;
        this.defaultModel = b.model;
        this.clock = b.clock;
        this.replayMode = b.replayMode;
        this.inDoubtPolicy = b.inDoubtPolicy;
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
        executor.execute(() -> runAttempt(task, execId, taskType, opts, runMode, future));
        return new FutureExecutionHandle<>(execId, future);
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
     * Exact re-fold of an execution's events (spec §7). M0 returns the folded state; M1 layers on
     * strict substitution with zero external calls.
     */
    public ExecutionState replay(ExecutionId id) {
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

    public void pause(ExecutionId id) {
        log.append(id, new CatalystEvent.ExecutionPaused(now(), "paused by request"));
    }

    /**
     * Requests cancellation. Note: the v0 event schema (spec §5) has no dedicated cancellation event,
     * so M0 records this as a failure marked "cancelled" (open question §13). Folds to FAILED.
     */
    public void cancel(ExecutionId id) {
        log.append(id, new CatalystEvent.ExecutionFailed(now(), "cancelled by request", log.latestSeq(id)));
    }

    public EventLog log() {
        return log;
    }

    // ── Internals ────────────────────────────────────────────────────────────

    private void createExecution(ExecutionId id, String taskType, String key, ExecutionOptions opts) {
        String configFingerprint = "replayMode=" + replayMode + ";inDoubt=" + inDoubtPolicy;
        String argsHash = Integer.toHexString(opts.vars().hashCode());
        log.append(id, new CatalystEvent.ExecutionCreated(now(), taskType, argsHash, configFingerprint, key));
    }

    private <R> void runAttempt(Task<R> task, ExecutionId id, String taskType,
                                ExecutionOptions opts, Mode mode, CompletableFuture<R> future) {
        List<SequencedEvent> recorded = log.read(id);
        ExecutionState state = Reducer.fold(id, recorded);
        int attempt = state.attempt() + 1;
        boolean appendEnabled = mode != Mode.REPLAY_TERMINAL;

        switch (mode) {
            case FRESH -> log.append(id, new CatalystEvent.ExecutionStarted(now(), attempt, nodeId));
            case RESUME -> log.append(id, new CatalystEvent.ExecutionResumed(now(), attempt));
            case REPLAY_TERMINAL -> { /* pure replay: no lifecycle events appended */ }
        }

        String effectiveTaskType = state.taskType() != null ? state.taskType() : taskType;
        ExecutionInfo info = new ExecutionInfo(id, attempt, effectiveTaskType, opts.metadata());
        Logger logger = LoggerFactory.getLogger("catalyst.exec." + id.value());

        ReplayingContext ctx = new ReplayingContext(id, log, defaultModel, info, opts.vars(),
                eventMapper, payloads, inDoubtPolicy, clock, logger, recorded, appendEnabled);

        try {
            R result = task.execute(ctx);
            if (appendEnabled) {
                log.append(id, new CatalystEvent.ExecutionCompleted(now(), payloads.toTree(result)));
            }
            future.complete(result);
        } catch (ExecutionPausedSignal pause) {
            // ExecutionPaused already recorded by the context; leave the execution paused.
            future.completeExceptionally(pause);
        } catch (Throwable t) {
            if (appendEnabled) {
                log.append(id, new CatalystEvent.ExecutionFailed(now(), String.valueOf(t), log.latestSeq(id)));
            }
            future.completeExceptionally(t);
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
