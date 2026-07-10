package com.cajunsystems.catalyst;

/**
 * How the runtime reacts when a recorded boundary's request hash differs from what task code
 * produces on replay (spec §6).
 */
public enum ReplayMode {

    /**
     * Default. A hash mismatch throws — this is how nondeterminism bugs in task code are caught.
     * (Mismatch detection is fully wired in M1; M0 substitutes strictly by cursor position.)
     */
    STRICT,

    /**
     * A hash mismatch appends {@code ExecutionBranched} and continues live from the divergence
     * point — the branching mechanism, not an error path. Wired in M2.
     */
    BRANCH
}
