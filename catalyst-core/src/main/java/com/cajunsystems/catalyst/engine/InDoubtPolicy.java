package com.cajunsystems.catalyst.engine;

/**
 * What to do when, on resume, the log holds a side-effecting request with no matching result — a call
 * that was in flight when the process crashed (spec §6). Two boundaries can be left in this state: a
 * {@code ToolRequested} with no {@code ToolCompleted}, and a {@code CompletionRequested} with no
 * {@code CompletionReceived}. This is a deliberate improvement over "make everything idempotent and
 * hope": the in-doubt call is surfaced rather than silently re-executed.
 *
 * <p>The model case is the one that costs money. A provider may have accepted, produced and billed a
 * completion whose result never reached the log; with nothing recorded to substitute, a resume would
 * otherwise re-issue it and pay twice, silently. So "zero duplicate model calls" holds under crash
 * recovery only because this policy governs that window.
 *
 * <p>The policy applies to a genuine crash, not to a call that threw. A provider call that failed in
 * a process that survived it is recorded as a task failure and governed by the {@code RetryPolicy}
 * instead.
 */
public enum InDoubtPolicy {

    /** Re-execute the tool. Correct for idempotent tools. */
    RETRY,

    /** Fail the execution with an in-doubt error. */
    FAIL,

    /** Pause the execution and surface the in-doubt call for a human/operator decision. */
    ASK
}
