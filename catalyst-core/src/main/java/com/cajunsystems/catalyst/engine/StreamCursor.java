package com.cajunsystems.catalyst.engine;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.log.StaleWriterException;

import java.util.List;

/**
 * One writer's position in one execution's stream, and the only way that writer appends to it.
 *
 * <p>When the log can fence ({@link EventLog#supportsConditionalAppend()}), every append is
 * conditioned on the seq this writer expects: one past the last event it <em>read</em>, advanced only
 * by its own successful appends. The expectation must come from the read. Taken from the log's tip at
 * write time instead, a writer that read through seq 10 while another appended seq 11 would fence on
 * 11 and write on top of history it never saw — which is the interleaving the fence exists to stop.
 *
 * <p>A rejected append ({@link StaleWriterException}) means another writer has taken the stream over.
 * The cursor is then <strong>fenced out for good</strong>: every later append on it is refused with
 * the same exception, so a zombie cannot slip anything in afterwards — not even a best-effort failure
 * record on its way out.
 *
 * <p>Over a log that cannot fence, appends are unconditional, exactly as before.
 *
 * <p>Not thread-safe; a cursor belongs to one attempt, whose appends are already serialised.
 */
public final class StreamCursor {

    private final EventLog log;
    private final ExecutionId id;
    private final boolean fenced;
    private long next;
    private StaleWriterException fencedOut;

    private StreamCursor(EventLog log, ExecutionId id, long lastSeqRead) {
        this.log = log;
        this.id = id;
        this.fenced = log.supportsConditionalAppend();
        this.next = lastSeqRead + 1;
    }

    /** A cursor for a writer that has read {@code id}'s stream through {@code lastSeqRead} ({@code -1}: nothing). */
    public static StreamCursor after(EventLog log, ExecutionId id, long lastSeqRead) {
        return new StreamCursor(log, id, lastSeqRead);
    }

    /** A cursor positioned after the last of {@code read} — the events this writer is acting on. */
    public static StreamCursor after(EventLog log, ExecutionId id, List<SequencedEvent> read) {
        return after(log, id, read.isEmpty() ? -1 : read.get(read.size() - 1).seq());
    }

    /**
     * Appends {@code event} at this writer's position.
     *
     * @throws StaleWriterException if another writer has moved the stream on — now, or on any earlier
     *                              append through this cursor
     */
    public long append(CatalystEvent event) {
        if (fencedOut != null) throw fencedOut;
        long seq;
        if (fenced) {
            try {
                seq = log.append(id, event, next);
            } catch (StaleWriterException e) {
                fencedOut = e;
                throw e;
            }
        } else {
            seq = log.append(id, event);
        }
        next = seq + 1;
        return seq;
    }

    /** The rejection that fenced this cursor out, or null if it has not been. */
    public StaleWriterException fencedOut() {
        return fencedOut;
    }

    /** The seq of the last event this writer has read or written, or {@code -1}. */
    public long lastSeq() {
        return next - 1;
    }

    public ExecutionId executionId() {
        return id;
    }
}
