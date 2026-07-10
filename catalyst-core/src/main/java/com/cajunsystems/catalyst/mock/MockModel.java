package com.cajunsystems.catalyst.mock;

import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Model;
import com.cajunsystems.catalyst.model.Usage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * A first-party, deterministic model for tests and demos (spec §2). It returns scripted responses in
 * order and <strong>counts invocations</strong> — the M0 resume demo asserts that this count does
 * not increase across a resume, proving zero duplicate model calls.
 *
 * <p>Thread-safe: the call counter and script cursor are atomic.
 */
public final class MockModel implements Model {

    private final List<Function<CompletionRequest, Completion>> script;
    private final AtomicInteger cursor = new AtomicInteger(0);
    private final AtomicInteger callCount = new AtomicInteger(0);

    private MockModel(List<Function<CompletionRequest, Completion>> script) {
        this.script = script;
    }

    /** A model that always returns the same text completion. */
    public static MockModel alwaysReturn(String text) {
        return new MockModel(List.of(req -> textCompletion(text)));
    }

    /** A model that returns the given texts in order (then repeats the last). */
    public static MockModel scripted(String... texts) {
        List<Function<CompletionRequest, Completion>> steps = new ArrayList<>();
        for (String t : texts) steps.add(req -> textCompletion(t));
        return new MockModel(steps);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Completion complete(CompletionRequest request) {
        callCount.incrementAndGet();
        int idx = cursor.getAndIncrement();
        if (idx >= script.size()) idx = script.size() - 1; // repeat the last scripted step
        return script.get(idx).apply(request);
    }

    /** How many times the model was actually invoked (i.e. not substituted from the log). */
    public int callCount() {
        return callCount.get();
    }

    private static Completion textCompletion(String text) {
        long promptTokens = 8;
        long completionTokens = Math.max(1, text.length() / 4);
        return new Completion(text, List.of(), new Usage(promptTokens, completionTokens), "stop");
    }

    public static final class Builder {
        private final List<Function<CompletionRequest, Completion>> script = new ArrayList<>();

        public Builder respond(String text) {
            script.add(req -> textCompletion(text));
            return this;
        }

        public Builder respond(Function<CompletionRequest, Completion> responder) {
            script.add(responder);
            return this;
        }

        public MockModel build() {
            return new MockModel(List.copyOf(script));
        }
    }
}
