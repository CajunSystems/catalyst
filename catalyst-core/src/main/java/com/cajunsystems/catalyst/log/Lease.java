package com.cajunsystems.catalyst.log;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;

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
 * <p>{@code cancelRequest} is how a node cancels an execution another node is running (Dist-4): it
 * adds the request to the holder's row, and the holder's next renewal finds it and cancels locally.
 * A node that takes the lease over carries the request with it and honours it before anything else,
 * so a request survives its holder dying.
 *
 * @param holder          the node id of the runtime that holds it
 * @param expiresAtMillis epoch millis after which another node may take it over
 * @param cancelRequest   the reason a cancel was requested, or {@code null} if none
 */
public record Lease(String holder, long expiresAtMillis, String cancelRequest) {

    private static final String FORMAT = "lease1";
    private static final String FORMAT_WITH_CANCEL = "lease1c";

    public Lease {
        Objects.requireNonNull(holder, "holder");
        if (holder.isBlank()) throw new IllegalArgumentException("holder must be non-blank");
    }

    public Lease(String holder, long expiresAtMillis) {
        this(holder, expiresAtMillis, null);
    }

    /** Whether the lease still binds other nodes at {@code now}. */
    public boolean liveAt(Instant now) {
        return now.toEpochMilli() < expiresAtMillis;
    }

    public Instant expiresAt() {
        return Instant.ofEpochMilli(expiresAtMillis);
    }

    public Optional<String> cancelRequested() {
        return Optional.ofNullable(cancelRequest);
    }

    /** This row with a cancel request added (or replaced); holder and expiry unchanged. */
    public Lease withCancelRequest(String reason) {
        return new Lease(holder, expiresAtMillis, Objects.requireNonNull(reason, "reason"));
    }

    /**
     * Canonical bytes. Compare-and-set compares stored bytes, so equal leases must encode
     * identically: {@code lease1|<expiresAtMillis>|<holder>}, or with a cancel request
     * {@code lease1c|<expiresAtMillis>|<base64url reason>|<holder>}. The holder goes last since it may
     * contain the separator, and the reason is base64 so it cannot.
     */
    public byte[] encode() {
        String s = cancelRequest == null
                ? FORMAT + "|" + expiresAtMillis + "|" + holder
                : FORMAT_WITH_CANCEL + "|" + expiresAtMillis + "|"
                        + Base64.getUrlEncoder().withoutPadding().encodeToString(cancelRequest.getBytes(StandardCharsets.UTF_8))
                        + "|" + holder;
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * The inverse of {@link #encode()}.
     *
     * @throws IllegalArgumentException if {@code bytes} is not an encoded lease
     */
    public static Lease decode(byte[] bytes) {
        String s = new String(bytes, StandardCharsets.UTF_8);
        try {
            int a = s.indexOf('|');
            int b = a < 0 ? -1 : s.indexOf('|', a + 1);
            if (a < 0 || b < 0) throw new IllegalArgumentException("Not an encoded lease: " + s);
            String format = s.substring(0, a);
            long expires = Long.parseLong(s.substring(a + 1, b));
            if (FORMAT.equals(format)) return new Lease(s.substring(b + 1), expires);
            if (FORMAT_WITH_CANCEL.equals(format)) {
                int c = s.indexOf('|', b + 1);
                if (c < 0) throw new IllegalArgumentException("Not an encoded lease: " + s);
                String reason = new String(Base64.getUrlDecoder().decode(s.substring(b + 1, c)), StandardCharsets.UTF_8);
                return new Lease(s.substring(c + 1), expires, reason);
            }
            throw new IllegalArgumentException("Not an encoded lease: " + s);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Not an encoded lease: " + s, e);
        }
    }
}
