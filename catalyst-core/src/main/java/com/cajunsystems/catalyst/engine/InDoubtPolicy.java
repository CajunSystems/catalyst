package com.cajunsystems.catalyst.engine;

/**
 * What to do when, on resume, the log ends with a {@code ToolRequested} that has no matching
 * {@code ToolCompleted} — a tool call that was in flight when the process crashed (spec §6). This is
 * a deliberate improvement over "make everything idempotent and hope": the in-doubt call is surfaced
 * rather than silently re-executed.
 */
public enum InDoubtPolicy {

    /** Re-execute the tool. Correct for idempotent tools. */
    RETRY,

    /** Fail the execution with an in-doubt error. */
    FAIL,

    /** Pause the execution and surface the in-doubt call for a human/operator decision. */
    ASK
}
