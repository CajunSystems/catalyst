package com.cajunsystems.catalyst.log;

import com.cajunsystems.catalyst.ExecutionId;

/**
 * Thrown by {@link EventLog#append(ExecutionId, com.cajunsystems.catalyst.events.CatalystEvent, long)}
 * when the execution's stream has moved past the sequence number the writer expected — someone else
 * has written to it, so this writer is stale and its append is rejected.
 *
 * <p>This is the failure a distributed runtime wants, and wants <em>from storage</em>. Ownership of
 * an execution can be arranged by a lease, a lock service or a placement map, but none of them can
 * tell a node it <em>still</em> holds ownership at the instant its write lands: a GC pause between
 * the check and the append is enough for two nodes to both believe they own a stream. Conditioning
 * the append on the stream's own position moves the decision to where the write happens, so a
 * coordination layer that is wrong costs a rejected append rather than a corrupted history.
 *
 * <p>Receiving this means the execution is being run somewhere else, or was resumed since this
 * writer last read it. The correct response is to stop writing and re-read, never to retry the
 * append unconditionally.
 */
public class StaleWriterException extends RuntimeException {

    private final ExecutionId executionId;
    private final long expectedSeq;
    private final long actualSeq;

    public StaleWriterException(ExecutionId executionId, long expectedSeq, long actualSeq) {
        super("Stale writer for execution " + executionId.value() + ": expected the stream to be at seq "
                + expectedSeq + " but it is at " + actualSeq);
        this.executionId = executionId;
        this.expectedSeq = expectedSeq;
        this.actualSeq   = actualSeq;
    }

    /** The execution whose stream rejected the append. */
    public ExecutionId executionId() {
        return executionId;
    }

    /** The next seq this writer believed the stream was at. */
    public long expectedSeq() {
        return expectedSeq;
    }

    /**
     * The next seq the stream is actually at, or {@code -1} if the log could not report it. The
     * gap is how far ahead the other writer is.
     */
    public long actualSeq() {
        return actualSeq;
    }
}
