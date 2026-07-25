package com.cajunsystems.catalyst.model;

/**
 * Receives a completion's text as it arrives (spec §13.1). Task code supplies one to
 * {@link Model#stream} when it wants incremental delivery — appending to a UI, forwarding to a
 * socket — rather than waiting for the whole message.
 *
 * <p>The sink is task code and runs on the task's own thread, before the model boundary is recorded.
 * Two consequences worth knowing:
 *
 * <ul>
 *   <li><strong>Chunk boundaries are not recorded.</strong> The log stores the assembled completion,
 *       so a replay delivers the same text in a single chunk. Accumulating chunks is safe; branching
 *       on how many arrived is not, and is the one way streaming can make a task nondeterministic.
 *   <li><strong>The sink must not invoke context boundaries.</strong> Calling {@code ctx.effect},
 *       {@code ctx.call} or another model completion from inside a sink would append an event
 *       <em>between</em> this boundary's request and result, which no replay can match.
 * </ul>
 */
@FunctionalInterface
public interface TokenSink {

    /** Called with the next piece of the message. Chunks concatenate to the completion's text. */
    void accept(String text);

    /** A sink that discards everything — useful when only the assembled completion is wanted. */
    static TokenSink discarding() {
        return text -> { };
    }
}
