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
    private final Clock clock;
    private final Logger logger;
    private final boolean appendEnabled;
    /** Nullable: present only for a live attempt the runtime may cancel cooperatively. */
    private final CancellationToken cancellation;

    /** Result-bearing recorded boundaries, in order, consumed as the task replays its prefix. */
    private final Deque<Boundary> boundaries = new ArrayDeque<>();
    /** A trailing {@code ToolRequested} with no completion: a tool that was in flight at crash. */
    private ToolRequested danglingTool;
    private long danglingToolSeq = -1;
    /**
     * A trailing {@code CompletionRequested} with no {@code CompletionReceived}: a provider call that
     * was in flight at crash. The provider may have accepted, produced and billed a completion that
     * never reached the log, so re-issuing it is a real and silent cost — the same in-doubt shape as a
     * dangling tool, and routed through the same {@link InDoubtPolicy}. {@code -1} when none.
     */
    private long danglingModelSeq = -1;
    private String danglingModelHash;
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
        long lastRecordedSeq = recorded.isEmpty() ? -1 : recorded.get(recorded.size() - 1).seq();

        String pendingRequestHash = null;
        long pendingRequestSeq = -1;
        ToolRequested pendingToolRequest = null; // an unmatched ToolRequested (in-doubt) if non-null at end
        long pendingToolRequestSeq = -1;
        String pendingToolInputHash = null;
        for (SequencedEvent se : recorded) {
            CatalystEvent e = se.event();
            switch (e) {
                case CompletionRequested cr -> {
                    pendingRequestHash = cr.requestHash();
                    pendingRequestSeq = se.seq();
                }
                case CompletionReceived cr -> {
                    boundaries.add(new Boundary(se.seq(), e, null, pendingRequestHash));
                    pendingRequestHash = null;
                    pendingRequestSeq = -1;
                }
                case RetryRequested ignored -> {
                    // A model call that threw locally leaves the same trace as one that crashed —
                    // a CompletionRequested with no CompletionReceived, because a failed completion
                    // records no result event. What tells them apart is this: the process survived to
                    // record its retry decision, so the call is a failure the RetryPolicy already owns,
                    // not an in-doubt boundary. Clearing the pending request here is load-bearing for
                    // exactly the reason the ToolCompleted case below documents — leave it and the
                    // retry dangles into handleInDoubtModel, becoming an InDoubtException under the
                    // default policy.
                    pendingRequestHash = null;
                    pendingRequestSeq = -1;
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
                    } else if (tc.error() != null && se.seq() == lastRecordedSeq && pendingToolRequest != null) {
                        // An errored tool at the very tail with no RetryRequested naming it: the process
                        // crashed between recording the failure and deciding whether to retry. Its side
                        // effect is in-doubt on resume — route recovery through InDoubtPolicy (which may
                        // re-run it) exactly as for a tool with no recorded completion, rather than
                        // replaying the recorded failure (which would burn the retry budget on a boundary
                        // that can never succeed by substitution).
                        this.danglingTool = pendingToolRequest;
                        this.danglingToolSeq = pendingToolRequestSeq;
                        pendingToolRequest = null;
                        pendingToolRequestSeq = -1;
                        pendingToolInputHash = null;
                    } else {
                        String name = pendingToolRequest != null ? pendingToolRequest.toolName() : null;
                        boundaries.add(new Boundary(se.seq(), e, name, pendingToolInputHash));
                        pendingToolRequest = null;
                        pendingToolRequestSeq = -1;
                        pendingToolInputHash = null;
                    }
                }
                case EffectRecorded er -> boundaries.add(new Boundary(se.seq(), e, er.label(), null));
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
        // The model-side counterpart: a CompletionRequested never matched by a CompletionReceived. Any
        // RetryRequested after it has already cleared it above, so what survives to here is a crash
        // between the request and the response — the window in which the provider may have been billed
        // for a completion the log never saw. Keyed on the recorded request hash so the boundary the
        // task re-produces can be checked against the one that was actually in flight.
        if (pendingRequestSeq >= 0) {
            this.danglingModelSeq = pendingRequestSeq;
            this.danglingModelHash = pendingRequestHash;
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
        if (danglingModelSeq >= 0) {
            return handleInDoubtModel(request, sink);
        }
        requireAppendable("model completion");
        if (realModel == null) {
            throw new IllegalStateException("No model configured for this runtime; ctx.model() is unavailable");
        }
        String requestHash = Hashing.canonicalRequestHash(request);
        append(new PromptBuilt(now(), Hashing.sha256(canonicalPrompt(request)), eventMapper.valueToTree(request.prompt())));
        append(new CompletionRequested(now(), requestHash));
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
     * Recovery for a model completion that was in flight when the process died: the log holds the
     * {@code CompletionRequested} but no {@code CompletionReceived}, so there is nothing to substitute
     * and no way to know whether the provider produced (and charged for) a completion. Rather than
     * silently re-issuing — which is what happens with no policy, and which quietly double-bills — the
     * call is surfaced through the same {@link InDoubtPolicy} that governs tools.
     *
     * <p>Under {@code RETRY} only the {@code CompletionReceived} is appended: the recorded
     * {@code PromptBuilt}/{@code CompletionRequested} pair already stands, and completing it in place
     * keeps the stream's request/response pairing intact, so the resulting log replays exactly like one
     * that never crashed.
     */
    private Completion handleInDoubtModel(CompletionRequest request, TokenSink sink) {
        String pendingHash = danglingModelHash;
        long pendingSeq = danglingModelSeq;
        // Cleared before the policy runs, so an ASK/FAIL that unwinds cannot be re-entered on the way
        // out, and a RETRY's own append is never mistaken for a second in-doubt boundary.
        danglingModelSeq = -1;
        danglingModelHash = null;
        String actualHash = Hashing.canonicalRequestHash(request);
        if (pendingHash != null && !pendingHash.equals(actualHash)) {
            // The task reached a model boundary, but not the one that was in flight — the recorded
            // request can no longer be completed by this call, which is a determinism divergence rather
            // than an in-doubt decision.
            throw new NonDeterministicReplayException(pendingSeq,
                    "in-doubt model request " + pendingHash, "model request " + actualHash);
        }
        return switch (inDoubtPolicy) {
            case RETRY -> {
                requireAppendable("in-doubt retry of model completion");
                if (realModel == null) {
                    throw new IllegalStateException(
                            "No model configured for this runtime; ctx.model() is unavailable");
                }
                long t0 = System.nanoTime();
                Completion completion = sink == null
                        ? realModel.complete(request)
                        : realModel.stream(request, guarded(sink));
                long latencyMillis = (System.nanoTime() - t0) / 1_000_000;
                append(new CompletionReceived(now(), eventMapper.valueToTree(completion),
                        completion.usage().promptTokens(), completion.usage().completionTokens(),
                        latencyMillis,
                        costModel.usd(completion.usage().promptTokens(), completion.usage().completionTokens()),
                        completion.finishReason())); // completes the dangling request
                yield completion;
            }
            case FAIL -> throw new InDoubtException("In-doubt model completion at seq " + pendingSeq
                    + " (crashed between request and completion; the provider may already have billed it)");
            case ASK -> {
                append(new ExecutionPaused(now(), "in-doubt model completion at seq " + pendingSeq));
                throw new ExecutionPausedSignal("in-doubt model completion at seq " + pendingSeq);
            }
        };
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
        request.prompt().messages().forEach(m -> sb.append(m.role()).append(':').append(m.content()).append('\n'));
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
                    throw new RuntimeException("Recorded tool '" + tool.name() + "' failed: " + tc.error());
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
        return runToolLive(tool, input);
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
                O output = applyUnchecked(tool, input);
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
        if (appendEnabled && branchSpec == null) {
            append(new ExecutionBranched(now(), null, seq, "divergence"));
        }
        boundaries.clear();  // everything after the fork runs live
        danglingTool = null; // the fork supersedes any in-doubt tool from the recorded tail
        danglingModelSeq = -1; // …and any in-doubt model completion, for the same reason
        danglingModelHash = null;
    }

    private void requireAppendable(String what) {
        if (!appendEnabled) {
            throw new NonDeterministicReplayException(-1,
                    "recorded boundary", "live execution at " + what
                    + " (pure replay ran past the recorded log — nondeterministic task code?)");
        }
        // Cooperative cancellation is observed only at live boundaries — never mid-replay, so a
        // cancelled resume still substitutes its recorded prefix intact before unwinding here.
        if (cancellation != null && cancellation.isCancelled()) {
            throw new CancellationSignal(cancellation.reason());
        }
    }

    private long append(CatalystEvent event) {
        return log.append(id, event);
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
