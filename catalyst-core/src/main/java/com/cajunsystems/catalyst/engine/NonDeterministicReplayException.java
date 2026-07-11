package com.cajunsystems.catalyst.engine;

/**
 * Thrown under {@link com.cajunsystems.catalyst.ReplayMode#STRICT} when task code, on replay,
 * produces a boundary that does not match what the log recorded — a different canonical request
 * hash, a different tool/effect/memory identity, a reordered boundary, or an attempt to execute a
 * boundary the log never recorded (spec §6). This is how nondeterminism bugs in task code are
 * caught. In {@code BRANCH} mode (M2) the same divergence instead forks the execution.
 */
public class NonDeterministicReplayException extends RuntimeException {

    private final long seq;
    private final String expected;
    private final String actual;

    public NonDeterministicReplayException(long seq, String expected, String actual) {
        super("Nondeterministic replay at seq " + seq + ": expected [" + expected + "] but task produced [" + actual + "]");
        this.seq = seq;
        this.expected = expected;
        this.actual = actual;
    }

    /** The sequence number of the recorded boundary that diverged, or {@code -1} if not applicable. */
    public long seq() {
        return seq;
    }

    /** What the log recorded at this point. */
    public String expected() {
        return expected;
    }

    /** What the replaying task produced instead. */
    public String actual() {
        return actual;
    }
}
