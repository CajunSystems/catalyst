package com.cajunsystems.catalyst.log;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Objects;

/**
 * Which node is running an execution, and until when: one row per execution, stored by the log and
 * changed only by compare-and-set ({@link EventLog#compareAndSetLease}).
 *
 * <p>A lease decides <em>who should run</em> an execution. It is not what keeps a stream correct.
 * That is the fence on every append: a holder whose lease expired while it was paused, and which
 * still believes it is the owner, has its next append refused by storage. So a lease needs no fencing
 * token of its own. A takeover always appends {@code ExecutionResumed} before doing any work, which
 * moves the stream past anything the old holder read. The lease row itself is changed by
 * compare-and-set on its exact previous value, so lease operations cannot race each other either.
 *
 * <p>{@code expiresAtMillis} is wall-clock time from the writing node's clock. Clock skew between
 * nodes can make a takeover early or late. Neither corrupts the stream. An early takeover is not
 * free, though: a call the old holder had in flight is found in doubt by the new holder and handled
 * by its {@code InDoubtPolicy}, which under the default {@code FAIL} ends the execution as
 * {@code FAILED}.
 *
 * @param holder          the node id of the runtime that holds it
 * @param expiresAtMillis epoch millis after which another node may take it over
 */
public record Lease(String holder, long expiresAtMillis) {

    private static final String FORMAT = "lease1";

    public Lease {
        Objects.requireNonNull(holder, "holder");
        if (holder.isBlank()) throw new IllegalArgumentException("holder must be non-blank");
    }

    /** Whether the lease still binds other nodes at {@code now}. */
    public boolean liveAt(Instant now) {
        return now.toEpochMilli() < expiresAtMillis;
    }

    public Instant expiresAt() {
        return Instant.ofEpochMilli(expiresAtMillis);
    }

    /**
     * Canonical bytes. Compare-and-set compares stored bytes, so equal leases must encode
     * identically: {@code lease1|<expiresAtMillis>|<holder>}.
     */
    public byte[] encode() {
        return (FORMAT + "|" + expiresAtMillis + "|" + holder).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * The inverse of {@link #encode()}.
     *
     * @throws IllegalArgumentException if {@code bytes} is not an encoded lease
     */
    public static Lease decode(byte[] bytes) {
        String s = new String(bytes, StandardCharsets.UTF_8);
        int a = s.indexOf('|');
        int b = a < 0 ? -1 : s.indexOf('|', a + 1);
        if (a < 0 || b < 0 || !FORMAT.equals(s.substring(0, a))) {
            throw new IllegalArgumentException("Not an encoded lease: " + s);
        }
        try {
            return new Lease(s.substring(b + 1), Long.parseLong(s.substring(a + 1, b)));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Not an encoded lease: " + s, e);
        }
    }
}
