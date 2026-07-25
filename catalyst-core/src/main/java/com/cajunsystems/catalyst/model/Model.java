package com.cajunsystems.catalyst.model;

/**
 * The minimal model SPI (spec §4). The core defines no provider concepts; providers arrive through
 * adapters (v0: {@code MockModel} and {@code catalyst-langchain4j}). Catalyst never maintains
 * provider HTTP clients.
 */
@FunctionalInterface
public interface Model {

    /** Produces a completion for the given request. */
    Completion complete(CompletionRequest request);

    /**
     * Produces a completion, delivering its text to {@code sink} as it arrives, and returns the
     * assembled result (spec §13.1).
     *
     * <p>Streaming is a <em>delivery</em> concern, not a durability one. Whatever the provider does on
     * the wire, the boundary Catalyst records is the assembled {@link Completion} — the same
     * {@code CompletionReceived} event a non-streaming call produces, so a streamed execution and a
     * non-streamed one are indistinguishable in the log and replay identically. On replay the recorded
     * text is re-emitted to the sink and no provider is contacted. Recording individual chunks, so a
     * replay could reproduce the original chunk boundaries, is deliberately left to a later phase.
     *
     * <p>The default implementation is for models that cannot stream: it calls {@link #complete} and
     * hands the sink the whole message as one chunk. A provider adapter overrides this to deliver
     * incrementally, and must block until the completion is assembled — the boundary is not recorded
     * until this method returns.
     */
    default Completion stream(CompletionRequest request, TokenSink sink) {
        Completion completion = complete(request);
        if (!completion.message().isEmpty()) {
            sink.accept(completion.message());
        }
        return completion;
    }
}
