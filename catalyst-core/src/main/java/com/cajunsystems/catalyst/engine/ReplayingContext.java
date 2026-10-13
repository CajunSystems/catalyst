package com.cajunsystems.catalyst.engine;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.Deterministic;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionInfo;
import com.cajunsystems.catalyst.Memory;
import com.cajunsystems.catalyst.ReplayMode;
import com.cajunsystems.catalyst.Tool;
import com.cajunsystems.catalyst.capture.AutoCapture;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.CatalystEvent.*;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Model;
import com.cajunsystems.catalyst.model.TokenSink;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The {@link Context} implementation that drives record + substitute — the heart of Catalyst's
 * durability, resume, replay and branching (spec §6, §7). It is seeded with the events already in the
 * log for this execution. For each recorded boundary the task re-produces, the matching result is
 * <em>substituted</em> from the log with no side effect; once the task runs past the end of the log,
 * boundaries <em>execute live</em> and are appended. This is why a resumed execution makes zero
 * duplicate model or tool calls.
 *
 * <p><strong>Strict replay (M1).</strong> Substitution is checked: each recorded boundary carries the
 * canonical hash/identity of the request that produced it (model request hash, tool name + input
 * hash, effect label, memory key). A mismatch under {@link ReplayMode#STRICT} raises
 * {@link NonDeterministicReplayException}, catching nondeterministic task code.
 *
 * <p><strong>Branching (M2).</strong> Under {@link ReplayMode#BRANCH} — or when a {@link BranchSpec}
 * is present — a mismatch is not an error: the context {@code fork()}s (appends
 * {@code ExecutionBranched} and switches to live execution from that point). A {@code BranchSpec}
 * additionally forces a cutover at {@code atSeq} and can substitute counterfactual tool results
 * during the replayed prefix. This is how {@code runtime.branch(id, seq)} explores alternatives.
 *
 * <p>Not thread-safe by itself: a single execution attempt runs on one virtual thread.
 */
public final class ReplayingContext implements Context {

    /**
     * A recorded, result-bearing boundary paired with the canonical identity/hash of the request
     * that produced it. {@code hash} is the model request hash or the tool input hash;
     * {@code identity} is the tool name, effect label, or memory key. Either may be {@code null}.
     */
    private record Boundary(long seq, CatalystEvent event, String identity, String hash) {}

    private final ExecutionId id;
    private final EventLog log;
    private final Model realModel;
    private final ExecutionInfo info;
    private final Map<String, Object> vars;
    private final ObjectMapper eventMapper;
    private final PayloadCodec payloads;
    private final InDoubtPolicy inDoubtPolicy;
    private final CostModel costModel;
    private final ReplayMode replayMode;
    private final BranchSpec branchSpec; // nullable: present only when driving a branch
    /** Per tool key, how many of the branch's still-unused recorded outputs have been handed out. */
    private final java.util.Map<String, Integer> recordedToolCursor = new java.util.HashMap<>();
    /**
     * The last recorded seq replay actually used: the highest boundary consumed, pulled back to just
     * before the fork on a divergence. Recorded tool outputs after it were never handed to this run.
     */
    private long lastReplayedSeq = -1;
    /** Raised (and possibly swallowed by the task): a branch that hit an unanswerable tool call. */
    private UnrecordedToolCallException raisedUnrecordedTool;
    private final Clock clock;
    private final Logger logger;
    private final boolean appendEnabled;
    /** Nullable: present only for a live attempt the runtime may cancel cooperatively. */
    private final CancellationToken cancellation;
    /** This attempt's position in the stream; every boundary append goes through it (fenced if the log can). */
    private final StreamCursor cursor;
    /**
     * Whether this context has raised a stop signal. The runtime cannot rely on the signal unwinding
     * out of the task: both signals are {@code RuntimeException}s, so a task with a
     * {@code catch (RuntimeException)} fallback around a boundary swallows them and returns normally.
     * Recording that return as {@code ExecutionCompleted} would make the execution terminal with a
     * result produced by the stop, and leave a boundary the log never recorded. See
     * {@link #raisedCancellation()} / {@link #raisedAbandon()}.
     */
    private volatile boolean raisedCancellation;
    private volatile boolean raisedAbandon;
    /** The signal this context parked the attempt on, if any — honoured even if the task swallowed it. */
    private volatile ExecutionWaitingSignal raisedWaiting;
    /**
     * A recorded {@code SignalAwaited} with no {@code SignalReceived} after it: the execution is still
     * waiting. Reaching it again parks the attempt without recording a second wait.
     */
    private String awaitingSignal;
    /** Timers started and not yet fired, by id: when each comes due. A resume waits only for what is left. */
    private final java.util.Map<String, java.time.Instant> pendingTimers = new java.util.HashMap<>();
    /** Timers this attempt has reached, so far: ids are timer-1, timer-2, … in task order, like auto-capture. */
    private int timerCount;

    /** Result-bearing recorded boundaries, in order, consumed as the task replays its prefix. */
    private final Deque<Boundary> boundaries = new ArrayDeque<>();
    /** A trailing {@code ToolRequested} with no completion: a tool that was in flight at crash. */
    private ToolRequested danglingTool;
    private long danglingToolSeq = -1;
    /**
     * A trailing {@code CompletionRequested} with no {@code CompletionReceived}: a provider call that
     * was in flight at crash. It may have been accepted and billed with the result lost on the way
     * back, so re-issuing it is a real cost, not just a repeated computation — the same in-doubt
     * shape a tool has, on the boundary where it is most expensive to get wrong.
     */
    private CompletionRequested danglingCompletion;
    private long danglingCompletionSeq = -1;
    /**
     * The seq of the {@code ToolCompleted} recording the most recent live tool failure, and the exact
     * exception instance it rethrew — so the runtime can attribute a propagating throwable to the
     * boundary that raised it ({@link #failedBoundarySeq}). {@code -1} until a live tool fails.
     */
    private long lastToolFailureSeq = -1;
    private RuntimeException lastToolFailureException;
    /** True once this run has forked off the recorded history (BRANCH mode). */
    private boolean branched;
    /** Working-memory state rebuilt from recorded {@code MemoryWritten} events. */
    private final Map<String, JsonNode> memoryState = new HashMap<>();

    private final MemoryImpl memory = new MemoryImpl();

    public ReplayingContext(ExecutionId id, EventLog log, Model realModel, ExecutionInfo info,
                            Map<String, Object> vars, ObjectMapper eventMapper, PayloadCodec payloads,
                            InDoubtPolicy inDoubtPolicy, CostModel costModel, ReplayMode replayMode,
                            BranchSpec branchSpec, Clock clock, Logger logger,
                            List<SequencedEvent> recorded, boolean appendEnabled) {
        this(id, log, realModel, info, vars, eventMapper, payloads, inDoubtPolicy, costModel, replayMode,
                branchSpec, clock, logger, recorded, appendEnabled, null);
    }

    public ReplayingContext(ExecutionId id, EventLog log, Model realModel, ExecutionInfo info,
                            Map<String, Object> vars, ObjectMapper eventMapper, PayloadCodec payloads,
                            InDoubtPolicy inDoubtPolicy, CostModel costModel, ReplayMode replayMode,
                            BranchSpec branchSpec, Clock clock, Logger logger,
                            List<SequencedEvent> recorded, boolean appendEnabled,
                            CancellationToken cancellation) {
        this(id, log, realModel, info, vars, eventMapper, payloads, inDoubtPolicy, costModel, replayMode,
                branchSpec, clock, logger, recorded, appendEnabled, cancellation,
                StreamCursor.after(log, id, recorded));
    }

    /**
     * As above, appending through {@code cursor} — the attempt's own position in the stream, shared
     * with the runtime's lifecycle appends so the whole attempt is one fenced writer.
     */
    public ReplayingContext(ExecutionId id, EventLog log, Model realModel, ExecutionInfo info,
                            Map<String, Object> vars, ObjectMapper eventMapper, PayloadCodec payloads,
                            InDoubtPolicy inDoubtPolicy, CostModel costModel, ReplayMode replayMode,
                            BranchSpec branchSpec, Clock clock, Logger logger,
                            List<SequencedEvent> recorded, boolean appendEnabled,
                            CancellationToken cancellation, StreamCursor cursor) {
        this.cursor = java.util.Objects.requireNonNull(cursor, "cursor");
        this.cancellation = cancellation;
        this.id = id;
        this.log = log;
        this.realModel = realModel;
        this.info = info;
        this.vars = vars == null ? Map.of() : vars;
        this.eventMapper = eventMapper;
        this.payloads = payloads;
        this.inDoubtPolicy = inDoubtPolicy;
        this.costModel = costModel == null ? CostModel.free() : costModel;
        this.replayMode = replayMode == null ? ReplayMode.STRICT : replayMode;
        this.branchSpec = branchSpec;
        this.clock = clock;
        this.logger = logger;
        this.appendEnabled = appendEnabled;
        seed(recorded);
    }

    /**
     * Builds the ordered boundary queue, pairing each {@code CompletionRequested}/{@code ToolRequested}
     * marker with the result it precedes so the recorded request hash/identity travels with the
     * substitutable event.
     */
    private void seed(List<SequencedEvent> recorded) {
        // Boundaries a retry was requested for: their recorded (failed) result is NOT substitutable —
        // the retry exists precisely to re-run that boundary live. Pre-scan so the drop below is a pure
        // function of the log, applied identically on resume and on replay(id, task).
        Set<Long> retriedFailures = new HashSet<>();
        for (SequencedEvent se : recorded) {
            if (se.event() instanceof RetryRequested rr && rr.failedSeq() >= 0) {
                retriedFailures.add(rr.failedSeq());
            }
        }

        String pendingRequestHash = null;
        CompletionRequested pendingCompletionRequest = null; // unmatched (in-doubt) if non-null at end
        long pendingCompletionRequestSeq = -1;
        ToolRequested pendingToolRequest = null; // an unmatched ToolRequested (in-doubt) if non-null at end
        long pendingToolRequestSeq = -1;
        String pendingToolInputHash = null;
        for (SequencedEvent se : recorded) {
            CatalystEvent e = se.event();
            switch (e) {
                case CompletionRequested cr -> {
                    pendingRequestHash = cr.requestHash();
                    pendingCompletionRequest = cr;
                    pendingCompletionRequestSeq = se.seq();
                }
                case CompletionReceived cr -> {
                    boundaries.add(new Boundary(se.seq(), e, null, pendingRequestHash));
                    pendingRequestHash = null;
                    pendingCompletionRequest = null;
                    pendingCompletionRequestSeq = -1;
                }
                case RetryRequested rr -> {
                    // A retry appended *after* an unmatched CompletionRequested means the provider call
                    // threw and the process lived to decide what to do about it. That is a failure, not
                    // a doubt: nothing was recorded for it, and retry-as-attempt re-runs the boundary
                    // live by design. Only a request with no verdict after it at all is in doubt.
                    pendingCompletionRequest = null;
                    pendingCompletionRequestSeq = -1;
                }
                case ToolRequested tr -> {
                    pendingToolRequest = tr;
                    pendingToolRequestSeq = se.seq();
                    pendingToolInputHash = Hashing.canonicalJsonHash(tr.input());
                }
                case ToolCompleted tc -> {
                    if (tc.error() != null && retriedFailures.contains(se.seq())) {
                        // The boundary a retry targets: drop it so call() re-runs the tool live. Clearing
                        // the pending request is load-bearing — leave it and the request dangles into
                        // handleInDoubt, turning a retry into an InDoubtException under the default policy.
                        pendingToolRequest = null;
                        pendingToolRequestSeq = -1;
                        pendingToolInputHash = null;
                    } else {
                        // A recorded failure is a fact, not a doubt: the tool ran and threw. That holds
                        // even at the very tail, where a crash cut off the retry-or-fail decision. It is
                        // substituted like any result: a task that handles failures (an agent feeding it
                        // to the model) continues exactly as it did live, and one that does not throws it
                        // again, so the interrupted decision is made now. A retry of it names its seq
                        // (call() attributes substituted failures), which drops it here next attempt.
                        String name = pendingToolRequest != null ? pendingToolRequest.toolName() : null;
                        boundaries.add(new Boundary(se.seq(), e, name, pendingToolInputHash));
                        pendingToolRequest = null;
                        pendingToolRequestSeq = -1;
                        pendingToolInputHash = null;
                    }
                }
                case EffectRecorded er -> boundaries.add(new Boundary(se.seq(), e, er.label(), null));
                case SignalAwaited sa -> this.awaitingSignal = sa.name();
                case TimerStarted ts -> pendingTimers.put(ts.timerId(), ts.wakeAt());
                case TimerFired tf -> {
                    // The sleep's boundary; the TimerStarted before it only said when to wake.
                    boundaries.add(new Boundary(se.seq(), e, tf.timerId(), null));
                    pendingTimers.remove(tf.timerId());
                }
                case SignalReceived sr -> {
                    // The await boundary's result; the SignalAwaited before it carries no result of its own.
                    boundaries.add(new Boundary(se.seq(), e, sr.name(), null));
                    this.awaitingSignal = null;
                }
                case MemoryRead mr -> boundaries.add(new Boundary(se.seq(), e, mr.key(), null));
                case MemoryWritten mw -> {
                    boundaries.add(new Boundary(se.seq(), e, mw.key(), null));
                    memoryState.put(mw.key(), mw.value());
                }
                default -> { /* lifecycle/marker events are not substitutable boundaries */ }
            }
        }
        // A tool call is in-doubt if any ToolRequested was never matched by a ToolCompleted — even if
        // lifecycle events (ExecutionFailed under FAIL, ExecutionPaused under ASK) were appended after
        // it, so it is no longer the literal last event.
        if (pendingToolRequest != null) {
            this.danglingTool = pendingToolRequest;
            this.danglingToolSeq = pendingToolRequestSeq;
        }
        // And a completion is in-doubt on the same terms: requested, never received, and no retry
        // verdict recorded after it. Lifecycle events may follow (ExecutionFailed under FAIL,
        // ExecutionPaused under ASK) without changing that.
        if (pendingCompletionRequest != null) {
            this.danglingCompletion = pendingCompletionRequest;
            this.danglingCompletionSeq = pendingCompletionRequestSeq;
        }
    }

    // ── Model boundary ─────────────────────────────────────────────────────────

    @Override
    public Model model() {
        return new RecordingModel();
    }

    /**
     * The model handed to task code. Both entry points fold onto one recorded boundary: streaming
     * changes how the text is <em>delivered</em>, never what is recorded, so a streamed execution and
     * a non-streamed one produce the same events and replay identically.
     */
    private final class RecordingModel implements Model {
        @Override
        public Completion complete(CompletionRequest request) {
            return modelBoundary(request, null);
        }

        @Override
        public Completion stream(CompletionRequest request, TokenSink sink) {
            if (sink == null) throw new IllegalArgumentException("sink");
            return modelBoundary(request, sink);
        }
    }

    /**
     * The model boundary. With {@code sink == null} this is a plain completion; otherwise the text is
     * streamed to the sink first and the assembled completion is recorded exactly as before. On the
     * substituted path the recorded message is re-emitted to the sink as a single chunk — the task
     * rebuilds its result from what it receives, so a replay that delivered nothing would diverge at
     * the task's own output.
     */
    private Completion modelBoundary(CompletionRequest request, TokenSink sink) {
        Optional<Boundary> recorded = pollExpected(CompletionReceived.class, "model completion");
        if (recorded.isPresent()) {
            Boundary b = recorded.get();
            String actualHash = Hashing.canonicalRequestHash(request);
            if (b.hash() == null || b.hash().equals(actualHash)) {
                Completion completion;
                try {
                    completion = eventMapper.treeToValue(
                            ((CompletionReceived) b.event()).completion(), Completion.class);
                } catch (Exception e) {
                    throw new IllegalStateException("Failed to substitute recorded completion", e);
                }
                if (sink != null && !completion.message().isEmpty()) {
                    guarded(sink).accept(completion.message());
                }
                return completion;
            }
            // hash mismatch → STRICT throws, BRANCH forks and falls through to live
            forkOrThrow(b.seq(), "model request " + b.hash(), "model request " + actualHash);
        }
        if (danglingCompletion != null) {
            return handleInDoubtCompletion(request, sink);
        }
        requireAppendable("model completion");
        requireModel();
        String requestHash = Hashing.canonicalRequestHash(request);
        append(new PromptBuilt(now(), Hashing.sha256(canonicalPrompt(request)), eventMapper.valueToTree(request.prompt())));
        append(new CompletionRequested(now(), requestHash));
        return invokeAndRecord(request, sink);
    }

    /**
     * Recovery for a completion that was requested and never received — the provider may have
     * accepted and billed the call with only the result lost, so a resume that re-issued it silently
     * would spend real money to hide a real question.
     *
     * <p>Catalyst has always done this for tools. The asymmetry was not a decision: a trailing
     * {@code CompletionRequested} set the pending request hash and was otherwise ignored, so the
     * resume simply ran the boundary live again. Measured on the single-node code before this
     * change, a log ending at {@code CompletionRequested} resumed and called the model a second time.
     *
     * <p>Keyed on the recorded {@code requestHash}: recovering an in-doubt call only means anything
     * if it is the <em>same</em> call. A task that reaches this boundary asking something else has
     * diverged, and re-issuing under the old request's doubt would attach the recovery to the wrong
     * question — so that is a determinism failure, reported as one.
     *
     * <p>A {@code RETRY} completes the dangling request rather than opening a new one: the log keeps
     * one {@code CompletionRequested} paired with one {@code CompletionReceived}, so a recovered
     * execution replays exactly like one that never crashed. Same shape as the tool path, which
     * appends only the {@code ToolCompleted}.
     */
    private Completion handleInDoubtCompletion(CompletionRequest request, TokenSink sink) {
        CompletionRequested pending = danglingCompletion;
        long pendingSeq = danglingCompletionSeq;
        danglingCompletion = null;
        String actualHash = Hashing.canonicalRequestHash(request);
        if (!pending.requestHash().equals(actualHash)) {
            throw new NonDeterministicReplayException(pendingSeq,
                    "in-doubt model request " + pending.requestHash(), "model request " + actualHash);
        }
        return switch (inDoubtPolicy) {
            case RETRY -> {
                requireAppendable("in-doubt retry of model completion");
                requireModel();
                yield invokeAndRecord(request, sink);
            }
            case FAIL -> throw new InDoubtException(
                    "In-doubt model completion (crashed between request and completion); the provider"
                    + " call may have been accepted and billed");
            case ASK -> {
                append(new ExecutionPaused(now(), "in-doubt model completion"));
                throw new ExecutionPausedSignal("in-doubt model completion");
            }
        };
    }

    private void requireModel() {
        if (realModel == null) {
            throw new IllegalStateException("No model configured for this runtime; ctx.model() is unavailable");
        }
    }

    /**
     * Calls the provider and records the completion. The request marker is the caller's business: a
     * live call opens one, an in-doubt retry closes the one already in the log.
     */
    private Completion invokeAndRecord(CompletionRequest request, TokenSink sink) {
        long t0 = System.nanoTime();
        Completion completion = sink == null
                ? realModel.complete(request)
                : realModel.stream(request, guarded(sink));
        long latencyMillis = (System.nanoTime() - t0) / 1_000_000;
        append(new CompletionReceived(now(), eventMapper.valueToTree(completion),
                completion.usage().promptTokens(), completion.usage().completionTokens(),
                latencyMillis, costModel.usd(completion.usage().promptTokens(), completion.usage().completionTokens()),
                completion.finishReason()));
        return completion;
    }

    /**
     * Wraps a sink so auto-capture is suppressed while it runs. The sink is task code executing
     * <em>inside</em> this boundary — between {@code CompletionRequested} and {@code CompletionReceived}
     * — so a capture in it would append its event in that gap. Replay would then meet an
     * {@code EffectRecorded} where it expected the completion and report a structural divergence.
     * Applied on both the live and the substituted path, so the sink sees identical conditions either
     * way.
     */
    private TokenSink guarded(TokenSink sink) {
        return text -> {
            boolean previousSuppression = AutoCapture.suppress();
            try {
                sink.accept(text);
            } finally {
                AutoCapture.restore(previousSuppression);
            }
        };
    }

    private String canonicalPrompt(CompletionRequest request) {
        StringBuilder sb = new StringBuilder();
        request.prompt().messages().forEach(m -> {
            sb.append(m.role()).append(':').append(m.content()).append('\n');
            Hashing.toolCallFields(sb, m); // nothing for a message without tool-call fields
        });
        return sb.toString();
    }

    // ── Tool boundary ──────────────────────────────────────────────────────────

    @Override
    @SuppressWarnings("unchecked")
    public <I, O> O call(Tool<I, O> tool, I input) {
        Optional<Boundary> recorded = pollExpected(ToolCompleted.class, "tool " + tool.name());
        if (recorded.isPresent()) {
            Boundary b = recorded.get();
            boolean identityOk = b.identity() == null || b.identity().equals(tool.name());
            String actualInputHash = Hashing.canonicalJsonHash(eventMapper.valueToTree(input));
            boolean hashOk = b.hash() == null || b.hash().equals(actualInputHash);
            if (identityOk && hashOk) {
                // Counterfactual: a branch may swap this tool's recorded result for an alternative.
                if (branchSpec != null && branchSpec.toolOverrides().containsKey(tool.name())) {
                    return (O) payloads.fromTree(branchSpec.toolOverrides().get(tool.name()));
                }
                ToolCompleted tc = (ToolCompleted) b.event();
                if (tc.error() != null) {
                    // Attributed to its recorded boundary, exactly as a live failure is (runToolLive), so a
                    // retry of it names this seq. The next attempt then drops the recorded failure and
                    // re-runs the tool live, instead of substituting the same failure every attempt.
                    RecordedToolFailureException failure = new RecordedToolFailureException(tool.name(), tc.error());
                    lastToolFailureSeq = b.seq();
                    lastToolFailureException = failure;
                    throw failure;
                }
                if (isDeterministic(tool)) {
                    return applyUnchecked(tool, input); // re-execute rather than deserialize (spec §4)
                }
                return (O) payloads.fromTree(tc.output());
            }
            // divergence → STRICT throws, BRANCH forks and falls through to live
            if (!identityOk) {
                forkOrThrow(b.seq(), "tool " + b.identity(), "tool " + tool.name());
            } else {
                forkOrThrow(b.seq(), "tool " + tool.name() + " input " + b.hash(), "input " + actualInputHash);
            }
        }
        if (danglingTool != null) {
            return handleInDoubt(tool, input);
        }
        if (branchSpec != null && branchSpec.toolReplay() != BranchSpec.ToolReplay.LIVE && !isDeterministic(tool)) {
            return answerFromRecording(tool, input);
        }
        return runToolLive(tool, input);
    }

    /**
     * A forked branch that must not re-run side effects: answer from the parent's recording when it
     * made the identical call, recording the boundary as usual so the branch's trajectory shows the
     * step. Otherwise run it live, or refuse under {@link BranchSpec.ToolReplay#RECORDED_ONLY}.
     * Deterministic tools never get here: re-running them is safe and is what they are for.
     */
    @SuppressWarnings("unchecked")
    private <I, O> O answerFromRecording(Tool<I, O> tool, I input) {
        com.fasterxml.jackson.databind.JsonNode inputTree = eventMapper.valueToTree(input);
        String inputHash = Hashing.canonicalJsonHash(inputTree);
        String key = BranchSpec.toolKey(tool.name(), inputHash);
        long replayedThrough = lastReplayedSeq;
        java.util.List<BranchSpec.RecordedOutput> outputs = branchSpec.recordedToolResults()
                .getOrDefault(key, java.util.List.of()).stream()
                .filter(r -> r.seq() > replayedThrough)   // earlier ones were substituted by replay
                .toList();
        int next = recordedToolCursor.getOrDefault(key, 0);
        BranchSpec.RecordedOutput recorded = next < outputs.size() ? outputs.get(next) : null;
        if (recorded == null) {
            if (branchSpec.toolReplay() == BranchSpec.ToolReplay.RECORDED_ONLY) {
                // Remembered, like the stop signals: a task that catches tool errors must not turn
                // "the candidate took a path the recording cannot answer" into a completion.
                raisedUnrecordedTool = new UnrecordedToolCallException(tool.name(), inputHash);
                throw raisedUnrecordedTool;
            }
            return runToolLive(tool, input);
        }
        requireAppendable("tool " + tool.name());
        recordedToolCursor.put(key, next + 1);
        append(new ToolRequested(now(), tool.name(), inputTree));
        if (recorded.error() != null) {
            // The recorded call failed: fail the same way, recorded and attributed like any failure.
            long failedSeq = append(new ToolCompleted(now(), null, recorded.error(), 0));
            RecordedToolFailureException failure = new RecordedToolFailureException(tool.name(), recorded.error());
            lastToolFailureSeq = failedSeq;
            lastToolFailureException = failure;
            throw failure;
        }
        append(new ToolCompleted(now(), recorded.output(), null, 0));
        return (O) payloads.fromTree(recorded.output());
    }

    private <I, O> O runToolLive(Tool<I, O> tool, I input) {
        requireAppendable("tool " + tool.name());
        append(new ToolRequested(now(), tool.name(), eventMapper.valueToTree(input)));
        long t0 = System.nanoTime();
        try {
            O output = applyUnchecked(tool, input);
            long latencyMillis = (System.nanoTime() - t0) / 1_000_000;
            append(new ToolCompleted(now(), payloads.toTree(output), null, latencyMillis));
            return output;
        } catch (RuntimeException ex) {
            long latencyMillis = (System.nanoTime() - t0) / 1_000_000;
            // Record the failure as a boundary result (so inspect/replay see it), and remember which seq
            // it landed at plus the instance we rethrow, so a retry can attribute the propagating failure
            // back to this boundary and re-run it live rather than substituting the recorded failure.
            lastToolFailureSeq = append(new ToolCompleted(now(), null, String.valueOf(ex), latencyMillis));
            lastToolFailureException = ex;
            throw ex;
        }
    }

    private <I, O> O handleInDoubt(Tool<I, O> tool, I input) {
        ToolRequested pending = danglingTool;
        long pendingSeq = danglingToolSeq;
        danglingTool = null;
        if (!pending.toolName().equals(tool.name())) {
            throw new NonDeterministicReplayException(pendingSeq,
                    "in-doubt tool " + pending.toolName(), "tool " + tool.name());
        }
        return switch (inDoubtPolicy) {
            case RETRY -> {
                requireAppendable("in-doubt retry of tool " + tool.name());
                long t0 = System.nanoTime();
                O output;
                try {
                    output = applyUnchecked(tool, input);
                } catch (RuntimeException ex) {
                    // The retried call failed: that is its verdict, recorded exactly as runToolLive
                    // records one, completing the dangling request. Left unrecorded, a task that handles
                    // the failure (an agent telling the model) would record later boundaries after a
                    // request that never completes: replay diverges there, and every resume would find
                    // it in doubt again.
                    long latencyMillis = (System.nanoTime() - t0) / 1_000_000;
                    lastToolFailureSeq = append(new ToolCompleted(now(), null, String.valueOf(ex), latencyMillis));
                    lastToolFailureException = ex;
                    throw ex;
                }
                long latencyMillis = (System.nanoTime() - t0) / 1_000_000;
                append(new ToolCompleted(now(), payloads.toTree(output), null, latencyMillis)); // completes the dangling request
                yield output;
            }
            case FAIL -> throw new InDoubtException("In-doubt tool call '" + pending.toolName()
                    + "' (crashed between request and completion)");
            case ASK -> {
                append(new ExecutionPaused(now(), "in-doubt tool: " + pending.toolName()));
                throw new ExecutionPausedSignal("in-doubt tool: " + pending.toolName());
            }
        };
    }

    // ── Effect boundary ────────────────────────────────────────────────────────

    @Override
    @SuppressWarnings("unchecked")
    public <T> T effect(String label, Supplier<T> supplier) {
        Optional<Boundary> recorded = pollExpected(EffectRecorded.class, "effect " + label);
        if (recorded.isPresent() && identityMatchesOrFork(recorded.get(), "effect", label)) {
            return (T) payloads.fromTree(((EffectRecorded) recorded.get().event()).value());
        }
        requireAppendable("effect " + label);
        // Auto-capture is suppressed inside the supplier: this boundary already records whatever the
        // supplier computes, so a nested capture would append its own event *before* this one and
        // desynchronise the boundary queue on replay.
        T value;
        boolean previousSuppression = AutoCapture.suppress();
        try {
            value = supplier.get();
        } finally {
            AutoCapture.restore(previousSuppression);
        }
        append(new EffectRecorded(now(), label, payloads.toTree(value)));
        return value;
    }

    // ── Signals ────────────────────────────────────────────────────────────────

    @Override
    @SuppressWarnings("unchecked")
    public <T> T awaitSignal(String name, Class<T> requested) {
        // Box a primitive (int.class -> Integer.class) before recording or checking anything: a payload
        // is always an object, Class.isInstance is false for every primitive class, and the recorded
        // name of a primitive ("int") cannot be loaded for signal()'s pre-write type check.
        Class<T> type = (Class<T>) java.lang.invoke.MethodType.methodType(requested).wrap().returnType();
        Optional<Boundary> recorded = pollExpected(SignalReceived.class, "signal " + name);
        if (recorded.isPresent() && identityMatchesOrFork(recorded.get(), "signal", name)) {
            Object payload = payloads.fromTree(((SignalReceived) recorded.get().event()).payload());
            if (payload != null && !type.isInstance(payload)) {
                throw new ClassCastException("Signal '" + name + "' carried a " + payload.getClass().getName()
                        + " but was awaited as " + type.getName());
            }
            return type.cast(payload);
        }
        if (awaitingSignal != null) {
            // Still waiting: the recorded wait has no signal after it. A different signal at this point
            // means the task no longer waits where it did — a divergence.
            String recordedWait = awaitingSignal;
            awaitingSignal = null;
            if (!recordedWait.equals(name)) {
                forkOrThrow(-1, "await signal " + recordedWait, "await signal " + name);
            } else {
                // A live attempt only gets here if something folded the execution out of WAITING after
                // the wait was recorded — a pause, then a resume whose ExecutionResumed folded it to
                // RUNNING. Parking without a record would leave it RUNNING: refused by signal(), and
                // re-resumed by every startup recovery. Re-record the wait so the stream folds back to
                // WAITING for this name. A pure replay (no appends) just stops here.
                if (appendEnabled) {
                    requireAppendable("await signal " + name);
                    append(new SignalAwaited(now(), name, type.getName()));
                }
                throw raisedWaiting = new ExecutionWaitingSignal(name, "Execution " + id + " is waiting for signal '" + name + "'");
            }
        }
        requireAppendable("await signal " + name);
        append(new SignalAwaited(now(), name, type.getName()));
        throw raisedWaiting = new ExecutionWaitingSignal(name, "Execution " + id + " is waiting for signal '" + name + "'");
    }

    // ── Durable timers ─────────────────────────────────────────────────────────

    @Override
    public void sleep(java.time.Duration duration) {
        if (duration == null || duration.isNegative()) throw new IllegalArgumentException("duration must be zero or positive");
        String timerId = "timer-" + (++timerCount);
        Optional<Boundary> fired = pollExpected(TimerFired.class, "timer " + timerId);
        if (fired.isPresent() && identityMatchesOrFork(fired.get(), "timer", timerId)) return; // already slept
        java.time.Instant wakeAt = pendingTimers.remove(timerId);
        boolean recorded = wakeAt != null;
        if (recorded && !appendEnabled) {
            throw raisedWaiting = sleepingUntil(timerId, wakeAt); // a pure replay ends at the wait
        }
        requireAppendable("sleep " + timerId);
        if (!recorded) {
            wakeAt = now().plus(duration);
            append(new TimerStarted(now(), timerId, wakeAt));
        }
        if (!now().isBefore(wakeAt)) {
            append(new TimerFired(now(), timerId)); // due: this attempt wakes it, as the stream's only writer
            return;
        }
        if (recorded) {
            // Not due yet, and only reachable after something folded the execution out of WAITING (a
            // pause, then a resume). Record the wait again, with the same wake time, so the fold
            // returns to WAITING. Otherwise it stays RUNNING and every recovery re-resumes it early.
            append(new TimerStarted(now(), timerId, wakeAt));
        }
        throw raisedWaiting = sleepingUntil(timerId, wakeAt);
    }

    private ExecutionWaitingSignal sleepingUntil(String timerId, java.time.Instant wakeAt) {
        return new ExecutionWaitingSignal(null, wakeAt, "Execution " + id + " is sleeping until " + wakeAt + " (" + timerId + ")");
    }

    /** Whether this context parked the attempt on a signal — whether or not the task let it unwind. */
    public boolean raisedWaiting() {
        return raisedWaiting != null;
    }

    /** The signal this context parked the attempt on, or null. */
    public ExecutionWaitingSignal waitingSignal() {
        return raisedWaiting;
    }

    // ── Memory ─────────────────────────────────────────────────────────────────

    @Override
    public Memory memory() {
        return memory;
    }

    private final class MemoryImpl implements Memory {
        @Override
        public void put(String key, Object value) {
            Optional<Boundary> recorded = pollExpected(MemoryWritten.class, "memory put " + key);
            if (recorded.isPresent() && identityMatchesOrFork(recorded.get(), "memory put", key)) {
                MemoryWritten mw = (MemoryWritten) recorded.get().event();
                memoryState.put(mw.key(), mw.value());
                return;
            }
            requireAppendable("memory put " + key);
            JsonNode node = payloads.toTree(value);
            append(new MemoryWritten(now(), key, node));
            memoryState.put(key, node);
        }

        @Override
        public <T> Optional<T> get(String key, Class<T> type) {
            Optional<Boundary> recorded = pollExpected(MemoryRead.class, "memory get " + key);
            JsonNode value;
            if (recorded.isPresent() && identityMatchesOrFork(recorded.get(), "memory get", key)) {
                value = ((MemoryRead) recorded.get().event()).value();
            } else {
                requireAppendable("memory get " + key);
                value = memoryState.get(key);
                append(new MemoryRead(now(), key, value));
            }
            if (value == null || value.isNull()) return Optional.empty();
            Object reconstructed = payloads.fromTree(value);
            // Honour the caller's requested type: fail here with a clear message rather than deferring
            // a confusing ClassCastException to the call site downstream.
            if (!type.isInstance(reconstructed)) {
                throw new ClassCastException("Memory key '" + key + "' holds a "
                        + reconstructed.getClass().getName() + " but was requested as " + type.getName());
            }
            return Optional.of(type.cast(reconstructed));
        }

        @Override
        public boolean contains(String key) {
            return memoryState.containsKey(key);
        }
    }

    // ── Misc context ───────────────────────────────────────────────────────────

    @Override
    @SuppressWarnings("unchecked")
    public <T> T var(String name) {
        return (T) vars.get(name);
    }

    @Override
    public ExecutionInfo info() {
        return info;
    }

    @Override
    public Logger log() {
        return logger;
    }

    // ── Internals ──────────────────────────────────────────────────────────────

    /**
     * Pops the next recorded boundary if it is of the expected type. An empty result means the task
     * has run past the recorded tail (go live) — or, in a branch, past the branch point or into a
     * divergence, at which point it forks. A type mismatch is a structural divergence.
     */
    private Optional<Boundary> pollExpected(Class<? extends CatalystEvent> type, String what) {
        Boundary head = boundaries.peek();
        if (head == null) return Optional.empty();
        // Forced cutover: a branch substitutes only up to atSeq, then runs live.
        if (branchSpec != null && head.seq() > branchSpec.atSeq()) {
            fork(head.seq());
            return Optional.empty();
        }
        if (type.isInstance(head.event())) {
            boundaries.poll();
            lastReplayedSeq = Math.max(lastReplayedSeq, head.seq());
            return Optional.of(head);
        }
        // structural divergence (wrong boundary type/order) → STRICT throws, BRANCH forks (go live)
        forkOrThrow(head.seq(),
                head.event().getClass().getSimpleName() + " (recorded next)",
                type.getSimpleName() + " (" + what + ")");
        return Optional.empty();
    }

    /** True if the boundary's identity matches; forks and returns false under BRANCH; throws under STRICT. */
    private boolean identityMatchesOrFork(Boundary b, String what, String actual) {
        if (b.identity() == null || b.identity().equals(actual)) return true;
        forkOrThrow(b.seq(), what + " " + b.identity(), what + " " + actual);
        return false; // BRANCH: fork happened, caller should go live
    }

    /**
     * Handles a divergence between the task and the record. Under {@link ReplayMode#STRICT} this is a
     * bug — throw. Under {@link ReplayMode#BRANCH} it is the branching mechanism — {@link #fork} and
     * let the caller fall through to live execution.
     */
    private void forkOrThrow(long seq, String expected, String actual) {
        if (replayMode == ReplayMode.BRANCH) {
            fork(seq);
            return;
        }
        throw new NonDeterministicReplayException(seq, expected, actual);
    }

    /**
     * Forks off recorded history at {@code seq}, then goes live. For auto-branch (a plain
     * {@link ReplayMode#BRANCH} divergence with no {@link BranchSpec}) this records the
     * {@code ExecutionBranched} marker; for an explicit {@code runtime.branch(...)} the runtime has
     * already recorded it, so we don't double it.
     */
    private void fork(long seq) {
        if (branched) return;
        branched = true;
        // The boundary at seq was polled but not used: it is where the run left the recording.
        lastReplayedSeq = Math.min(lastReplayedSeq, seq - 1);
        if (appendEnabled && branchSpec == null) {
            append(new ExecutionBranched(now(), null, seq, "divergence"));
        }
        boundaries.clear();  // everything after the fork runs live
        danglingTool = null; // the fork supersedes any in-doubt tool from the recorded tail
    }

    private void requireAppendable(String what) {
        // A swallowed wait stays a wait, as the stop signals do: nothing may be recorded after
        // SignalAwaited. A fallback that made a model/tool/effect call would put its events between the
        // wait and the signal delivered later, and the resume — which looks for SignalReceived where the
        // task waits — could never get past them. Checked FIRST, so a pure replay that reaches a recorded
        // wait unwinds as a wait (and replay() ends there) rather than as "ran past the log". Cancel
        // still wins: the runtime's park path records a pending cancel whichever signal unwound.
        if (raisedWaiting != null) throw raisedWaiting;
        if (!appendEnabled) {
            throw new NonDeterministicReplayException(-1,
                    "recorded boundary", "live execution at " + what
                    + " (pure replay ran past the recorded log — nondeterministic task code?)");
        }
        // Cooperative cancellation is observed only at live boundaries — never mid-replay, so a
        // cancelled resume still substitutes its recorded prefix intact before unwinding here.
        if (cancellation != null && cancellation.isCancelled()) {
            raisedCancellation = true;
            throw new CancellationSignal(cancellation.reason());
        }
        // Shutdown: checked after cancel so a requested cancel still wins. Raised here — before the
        // boundary records anything — so the unwind leaves no dangling request to be in doubt about.
        if (cancellation != null && cancellation.isAbandoned()) {
            raisedAbandon = true;
            throw new ExecutionAbandonedSignal("runtime shutting down before " + what + "; execution " + id
                    + " is left RUNNING for the next start to resume");
        }
    }

    private long append(CatalystEvent event) {
        return cursor.append(event);
    }

    /**
     * The seq of the recorded boundary whose live failure produced {@code propagating}, or {@code -1}
     * if this failure is not attributable to an unhandled tool boundary. Used by the runtime to stamp
     * {@code RetryRequested.failedSeq} so a retry re-runs that boundary live instead of substituting its
     * recorded failure.
     *
     * <p>Attribution is by exception <em>identity</em>: the seq is returned only when the last live tool
     * failure's exception is {@code propagating} itself or somewhere in its cause chain (covering "catch,
     * wrap, rethrow"). A failure the task caught and recovered from, or one raised in pure task code,
     * yields {@code -1} — and {@code -1} degrades safely to "retry is a no-op for this failure", never to
     * dropping a boundary a substitution still depends on.
     */
    /** Whether this context raised {@link CancellationSignal} — whether or not the task let it unwind. */
    public boolean raisedCancellation() {
        return raisedCancellation;
    }

    /** The unanswerable tool call this branch raised, whether or not the task let it unwind; null if none. */
    public UnrecordedToolCallException raisedUnrecordedTool() {
        return raisedUnrecordedTool;
    }

    /** Whether this context raised {@link ExecutionAbandonedSignal} — whether or not the task let it unwind. */
    public boolean raisedAbandon() {
        return raisedAbandon;
    }

    /** Attributed to a recorded {@code ToolCompleted(error)}: live, substituted, in-doubt retry or branch-reproduced. */
    @Override
    public boolean isRecordedToolFailure(Throwable failure) {
        return failure != null && failure == lastToolFailureException;
    }

    public long failedBoundarySeq(Throwable propagating) {
        if (lastToolFailureException == null) return -1;
        // Bounded walk: a self- or mutually-referential cause chain would otherwise loop forever.
        Throwable t = propagating;
        for (int depth = 0; t != null && depth < 64; t = t.getCause(), depth++) {
            if (t == lastToolFailureException) return lastToolFailureSeq;
        }
        return -1;
    }

    private Instant now() {
        return clock.instant();
    }

    private static boolean isDeterministic(Tool<?, ?> tool) {
        return tool.getClass().isAnnotationPresent(Deterministic.class);
    }

    /**
     * Runs a tool body with auto-capture suppressed: the tool's own output is the recorded boundary, so
     * nondeterminism inside it needs no separate capture. (A {@link Deterministic} tool is re-executed
     * on replay, but its purity is the annotation's promise — capturing inside it would record events a
     * pure replay must not append.)
     */
    @SuppressWarnings("unchecked")
    private static <I, O> O applyUnchecked(Tool<I, O> tool, I input) {
        boolean previousSuppression = AutoCapture.suppress();
        try {
            return tool.apply(input);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Tool '" + tool.name() + "' threw", e);
        } finally {
            AutoCapture.restore(previousSuppression);
        }
    }
}
