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
 * <p>Not thread-safe by itself: a single execution attempt runs on one virtual thread. (Fan-out
 * within a task is a later concern; the reserved design uses a scoped carrier, not this object.)
 */
public final class ReplayingContext implements Context {

    private final ExecutionId id;
    private final EventLog log;
    private final Model realModel;
    private final ExecutionInfo info;
    private final Map<String, Object> vars;
    private final ObjectMapper eventMapper;
    private final PayloadCodec payloads;
    private final InDoubtPolicy inDoubtPolicy;
    private final Clock clock;
    private final Logger logger;
    private final boolean appendEnabled;

    /** Result-bearing recorded boundaries, in order, consumed as the task replays its prefix. */
    private final Deque<CatalystEvent> boundaries = new ArrayDeque<>();
    /** A trailing {@code ToolRequested} with no completion: a tool that was in flight at crash. */
    private ToolRequested danglingTool;
    /** Working-memory state rebuilt from recorded {@code MemoryWritten} events. */
    private final Map<String, JsonNode> memoryState = new HashMap<>();

    private final MemoryImpl memory = new MemoryImpl();

    public ReplayingContext(ExecutionId id, EventLog log, Model realModel, ExecutionInfo info,
                            Map<String, Object> vars, ObjectMapper eventMapper, PayloadCodec payloads,
                            InDoubtPolicy inDoubtPolicy, Clock clock, Logger logger,
                            List<SequencedEvent> recorded, boolean appendEnabled) {
        this.id = id;
        this.log = log;
        this.realModel = realModel;
        this.info = info;
        this.vars = vars == null ? Map.of() : vars;
        this.eventMapper = eventMapper;
        this.payloads = payloads;
        this.inDoubtPolicy = inDoubtPolicy;
        this.clock = clock;
        this.logger = logger;
        this.appendEnabled = appendEnabled;
        seed(recorded);
    }

    private void seed(List<SequencedEvent> recorded) {
        CatalystEvent last = null;
        for (SequencedEvent se : recorded) {
            CatalystEvent e = se.event();
            last = e;
            if (e instanceof CompletionReceived || e instanceof ToolCompleted
                    || e instanceof EffectRecorded || e instanceof MemoryRead || e instanceof MemoryWritten) {
                boundaries.add(e);
            }
            if (e instanceof MemoryWritten mw) {
                memoryState.put(mw.key(), mw.value());
            }
        }
        // A tool call is in-doubt only if the very last recorded event is its (uncompleted) request.
        if (last instanceof ToolRequested tr) {
            this.danglingTool = tr;
        }
    }

    // ── Model boundary ─────────────────────────────────────────────────────────

    @Override
    public Model model() {
        return this::completeRecorded;
    }

    private Completion completeRecorded(CompletionRequest request) {
        Optional<CompletionReceived> recorded = pollExpected(CompletionReceived.class, "model completion");
        if (recorded.isPresent()) {
            try {
                return eventMapper.treeToValue(recorded.get().completion(), Completion.class);
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
                latencyMillis, /* cost, priced by adapters in M1 */ 0.0, completion.finishReason()));
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
        Optional<ToolCompleted> recorded = pollExpected(ToolCompleted.class, "tool " + tool.name());
        if (recorded.isPresent()) {
            ToolCompleted tc = recorded.get();
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
        danglingTool = null;
        if (!pending.toolName().equals(tool.name())) {
            throw new ReplayDivergenceException("In-doubt tool was '" + pending.toolName()
                    + "' but task now calls '" + tool.name() + "'");
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
        Optional<EffectRecorded> recorded = pollExpected(EffectRecorded.class, "effect " + label);
        if (recorded.isPresent()) {
            return (T) payloads.fromTree(recorded.get().value());
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
            Optional<MemoryWritten> recorded = pollExpected(MemoryWritten.class, "memory put " + key);
            if (recorded.isPresent()) {
                memoryState.put(recorded.get().key(), recorded.get().value());
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
            Optional<MemoryRead> recorded = pollExpected(MemoryRead.class, "memory get " + key);
            JsonNode value;
            if (recorded.isPresent()) {
                value = recorded.get().value();
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

    private <E extends CatalystEvent> Optional<E> pollExpected(Class<E> type, String what) {
        CatalystEvent head = boundaries.peek();
        if (head == null) return Optional.empty();
        if (type.isInstance(head)) {
            boundaries.poll();
            return Optional.of(type.cast(head));
        }
        throw new ReplayDivergenceException("Replay divergence at " + what
                + ": next recorded boundary is " + head.getClass().getSimpleName()
                + " but task produced a " + type.getSimpleName());
    }

    private void requireAppendable(String what) {
        if (!appendEnabled) {
            throw new ReplayDivergenceException("Pure replay of a completed execution tried to go live at " + what
                    + " — the recorded log does not cover this boundary (nondeterministic task code?)");
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
