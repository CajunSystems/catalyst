package com.cajunsystems.catalyst.events;

/**
 * A {@link CatalystEvent} paired with the sequence number the {@code EventLog} assigned it on
 * append. {@code seq} is a dense, per-execution, monotonically increasing cursor
 * (0, 1, 2, …); together with the execution id it is the replay cursor {@code (executionId, seq)}.
 * The event stays pure — {@code seq} is an addressing concern of the log, not of the event.
 */
public record SequencedEvent(long seq, CatalystEvent event) {
    public SequencedEvent {
        if (seq < 0) throw new IllegalArgumentException("seq must be >= 0, was: " + seq);
        if (event == null) throw new IllegalArgumentException("event must not be null");
    }
}
