package com.cajunsystems.catalyst.engine;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.Deterministic;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionInfo;
import com.cajunsystems.catalyst.Memory;
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
 * durability, resume and replay (spec §6). It is seeded with the events already in the log for this
 * execution. For each recorded boundary the task re-produces, the matching result is
 * <em>substituted</em> from the log with no side effect; once the task runs past the end of the log,
 * boundaries <em>execute live</em> and are appended. This is why a resumed execution makes zero
 * duplicate model or tool calls.
 *
 * <p><strong>Strict replay (M1).</strong> Substitution is not blind: each recorded boundary carries
 * the canonical hash/identity of the request that produced it (a model request hash, a tool
 * name + input hash, an effect label, a memory key). When the task replays, the boundary it
 * produces is checked against the recorded one; a mismatch under {@link com.cajunsystems.catalyst.ReplayMode#STRICT}
 * raises {@link NonDeterministicReplayException}, catching nondeterministic task code.
 *
 * <p>Not thread-safe by itself: a single execution attempt runs on one virtual thread.
 */
public final class ReplayingContext implements Context {

    /**
     * A recorded, result-bearing boundary paired with the canonical identity/hash of the request
     * that produced it. {@code hash} is the model request hash or the tool input hash;
     * {@code identity} is the tool name, effect label, or memory key. Either may be {@code null}
     * for older records or boundaries where it does not apply.
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
    private final Clock clock;
    private final Logger logger;
    private final boolean appendEnabled;

    /** Result-bearing recorded boundaries, in order, consumed as the task replays its prefix. */
    private final Deque<Boundary> boundaries = new ArrayDeque<>();
    /** A trailing {@code ToolRequested} with no completion: a tool that was in flight at crash. */
    private ToolRequested danglingTool;
    private long danglingToolSeq = -1;
    /** Working-memory state rebuilt from recorded {@code MemoryWritten} events. */
    private final Map<String, JsonNode> memoryState = new HashMap<>();

    private final MemoryImpl memory = new MemoryImpl();

    public ReplayingContext(ExecutionId id, EventLog log, Model realModel, ExecutionInfo info,
                            Map<String, Object> vars, ObjectMapper eventMapper, PayloadCodec payloads,
                            InDoubtPolicy inDoubtPolicy, CostModel costModel, Clock clock, Logger logger,
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
            if (b.hash() != null && !b.hash().equals(actualHash)) {
                throw new NonDeterministicReplayException(b.seq(),
                        "model request " + b.hash(), "model request " + actualHash);
            }
            try {
                return eventMapper.treeToValue(((CompletionReceived) b.event()).completion(), Completion.class);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to substitute recorded completion", e);
            }
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
            if (b.identity() != null && !b.identity().equals(tool.name())) {
                throw new NonDeterministicReplayException(b.seq(), "tool " + b.identity(), "tool " + tool.name());
            }
            String actualInputHash = Hashing.canonicalJsonHash(eventMapper.valueToTree(input));
            if (b.hash() != null && !b.hash().equals(actualInputHash)) {
                throw new NonDeterministicReplayException(b.seq(),
                        "tool " + tool.name() + " input " + b.hash(), "input " + actualInputHash);
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
        if (recorded.isPresent()) {
            Boundary b = recorded.get();
            requireIdentity(b, "effect", label);
            return (T) payloads.fromTree(((EffectRecorded) b.event()).value());
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
            if (recorded.isPresent()) {
                Boundary b = recorded.get();
                requireIdentity(b, "memory put", key);
                MemoryWritten mw = (MemoryWritten) b.event();
                memoryState.put(mw.key(), mw.value());
                return;
            }
            requireAppendable("memory put " + key);
            JsonNode node = payloads.toTree(value);
            append(new MemoryWritten(now(), key, node));
            memoryState.put(key, node);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Optional<T> get(String key, Class<T> type) {
            Optional<Boundary> recorded = pollExpected(MemoryRead.class, "memory get " + key);
            JsonNode value;
            if (recorded.isPresent()) {
                Boundary b = recorded.get();
                requireIdentity(b, "memory get", key);
                value = ((MemoryRead) b.event()).value();
            } else {
                requireAppendable("memory get " + key);
                value = memoryState.get(key);
                append(new MemoryRead(now(), key, value));
            }
            if (value == null || value.isNull()) return Optional.empty();
            return Optional.of((T) payloads.fromTree(value));
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
     * has run past the recorded tail (go live). A different type at the head is a structural
     * divergence — the task produced its boundaries in a different order than recorded.
     */
    private Optional<Boundary> pollExpected(Class<? extends CatalystEvent> type, String what) {
        Boundary head = boundaries.peek();
        if (head == null) return Optional.empty();
        if (type.isInstance(head.event())) {
            boundaries.poll();
            return Optional.of(head);
        }
        throw new NonDeterministicReplayException(head.seq(),
                head.event().getClass().getSimpleName() + " (recorded next)",
                type.getSimpleName() + " (" + what + ")");
    }

    /** Validates a substituted boundary's identity (effect label, memory key) matches the request. */
    private static void requireIdentity(Boundary b, String what, String actual) {
        if (b.identity() != null && !b.identity().equals(actual)) {
            throw new NonDeterministicReplayException(b.seq(),
                    what + " " + b.identity(), what + " " + actual);
        }
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
