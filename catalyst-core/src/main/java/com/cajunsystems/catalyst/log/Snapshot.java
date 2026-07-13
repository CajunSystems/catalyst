package com.cajunsystems.catalyst.log;

/**
 * A durable fold checkpoint (spec §8): the reducer's accumulated state through {@code throughSeq},
 * serialized to opaque bytes owned by the runtime. On {@code inspect}/resume the runtime restores a
 * snapshot and folds only the events <em>after</em> {@code throughSeq}, instead of re-folding the
 * whole log — the win grows with execution length.
 *
 * <p>The {@link EventLog} treats {@code state} as an opaque blob; only the runtime knows how to
 * decode it. This keeps the storage seam free of engine types and lets any backend persist a
 * checkpoint as bytes + a sequence number.
 */
public record Snapshot(long throughSeq, byte[] state) {

    public Snapshot {
        if (state == null) throw new IllegalArgumentException("snapshot state must not be null");
        state = state.clone(); // defensive: the log owns an immutable copy
    }

    @Override
    public byte[] state() {
        return state.clone();
    }
}
