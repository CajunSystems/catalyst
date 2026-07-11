package com.cajunsystems.catalyst.engine;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.Deterministic;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionInfo;
import com.cajunsystems.catalyst.Memory;
import com.cajunsystems.catalyst.ReplayMode;
import com.cajunsystems.catalyst.Tool;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.CatalystEvent.*;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Model;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    /** Result-bearing recorded boundaries, in order, consumed as the task replays its prefix. */
    private final Deque<Boundary> boundaries = new ArrayDeque<>();
    /** A trailing {@code ToolRequested} with no completion: a tool that was in flight at crash. */
    private ToolRequested danglingTool;
    private long danglingToolSeq = -1;
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
        CatalystEvent last = null;
        long lastSeq = -1;
        String pendingRequestHash = null;
        String pendingToolName = null;
        String pendingToolInputHash = null;
        for (SequencedEvent se : recorded) {
            CatalystEvent e = se.event();
            last = e;
            lastSeq = se.seq();
            switch (e) {
                case CompletionRequested cr -> pendingRequestHash = cr.requestHash();
                case CompletionReceived cr -> {
                    boundaries.add(new Boundary(se.seq(), e, null, pendingRequestHash));
                    pendingRequestHash = null;
                }
                case ToolRequested tr -> {
                    pendingToolName = tr.toolName();
                    pendingToolInputHash = Hashing.canonicalJsonHash(tr.input());
                }
                case ToolCompleted tc -> {
                    boundaries.add(new Boundary(se.seq(), e, pendingToolName, pendingToolInputHash));
                    pendingToolName = null;
                    pendingToolInputHash = null;
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
        // A tool call is in-doubt only if the very last recorded event is its (uncompleted) request.
        if (last instanceof ToolRequested tr) {
            this.danglingTool = tr;
            this.danglingToolSeq = lastSeq;
        }
    }

    // ── Model boundary ─────────────────────────────────────────────────────────

    @Override
    public Model model() {
        return this::completeRecorded;
    }

    private Completion completeRecorded(CompletionRequest request) {
        Optional<Boundary> recorded = pollExpected(CompletionReceived.class, "model completion");
        if (recorded.isPresent()) {
            Boundary b = recorded.get();
            String actualHash = Hashing.canonicalRequestHash(request);
            if (b.hash() == null || b.hash().equals(actualHash)) {
                try {
                    return eventMapper.treeToValue(((CompletionReceived) b.event()).completion(), Completion.class);
                } catch (Exception e) {
                    throw new IllegalStateException("Failed to substitute recorded completion", e);
                }
            }
            // hash mismatch → STRICT throws, BRANCH forks and falls through to live
            forkOrThrow(b.seq(), "model request " + b.hash(), "model request " + actualHash);
        }
        requireAppendable("model completion");
        if (realModel == null) {
            throw new IllegalStateException("No model configured for this runtime; ctx.model() is unavailable");
        }
        String requestHash = Hashing.canonicalRequestHash(request);
        append(new PromptBuilt(now(), Hashing.sha256(canonicalPrompt(request)), eventMapper.valueToTree(request.prompt())));
        append(new CompletionRequested(now(), requestHash));
        long t0 = System.nanoTime();
        Completion completion = realModel.complete(request);
        long latencyMillis = (System.nanoTime() - t0) / 1_000_000;
        append(new CompletionReceived(now(), eventMapper.valueToTree(completion),
                completion.usage().promptTokens(), completion.usage().completionTokens(),
                latencyMillis, costModel.usd(completion.usage().promptTokens(), completion.usage().completionTokens()),
                completion.finishReason()));
        return completion;
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
            append(new ToolCompleted(now(), null, String.valueOf(ex), latencyMillis));
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
        T value = supplier.get();
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
    }

    private void requireAppendable(String what) {
        if (!appendEnabled) {
            throw new NonDeterministicReplayException(-1,
                    "recorded boundary", "live execution at " + what
                    + " (pure replay ran past the recorded log — nondeterministic task code?)");
        }
    }

    private long append(CatalystEvent event) {
        return log.append(id, event);
    }

    private Instant now() {
        return clock.instant();
    }

    private static boolean isDeterministic(Tool<?, ?> tool) {
        return tool.getClass().isAnnotationPresent(Deterministic.class);
    }

    @SuppressWarnings("unchecked")
    private static <I, O> O applyUnchecked(Tool<I, O> tool, I input) {
        try {
            return tool.apply(input);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Tool '" + tool.name() + "' threw", e);
        }
    }
}
