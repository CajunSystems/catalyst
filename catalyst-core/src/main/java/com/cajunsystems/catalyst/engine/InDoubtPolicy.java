package com.cajunsystems.catalyst.engine;

/**
 * What to do when, on resume, the log holds a request with no matching result — a call that was in
 * flight when the process crashed (spec §6). This is a deliberate improvement over "make everything
 * idempotent and hope": the in-doubt call is surfaced rather than silently re-executed.
 *
 * <p>Two boundaries reach here, on the same terms:
 * <ul>
 *   <li>a {@code ToolRequested} with no {@code ToolCompleted} — a side effect that may or may not
 *       have happened;</li>
 *   <li>a {@code CompletionRequested} with no {@code CompletionReceived} — a provider call that may
 *       have been accepted and billed with only the result lost on the way back.</li>
 * </ul>
 *
 * <p>The model case is not merely symmetric, it is the expensive one: re-issuing costs money and is
 * the failure "zero duplicate model calls" is measured on. A request the process lived long enough
 * to record a verdict for — a {@code RetryRequested} after it — is a failure rather than a doubt,
 * and re-runs live under the retry policy instead of arriving here.
 */
public enum InDoubtPolicy {

    /** Re-execute the tool. Correct for idempotent tools. */
    RETRY,

    /** Fail the execution with an in-doubt error. */
    FAIL,

    /** Pause the execution and surface the in-doubt call for a human/operator decision. */
    ASK
}
