package com.cajunsystems.catalyst.events;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

/**
 * The Catalyst event model. Everything important becomes an event; nothing else is stored and
 * everything derives from events by folding (spec §5). This sealed hierarchy is the durable schema
 * and lives in its own module for schema stability.
 *
 * <p>Each event carries the wall-clock {@link #at()} it was recorded. The replay cursor is the pair
 * {@code (executionId, seq)}; {@code seq} is not stored on the event itself — it is assigned by the
 * {@code EventLog} on append and carried by {@link SequencedEvent}. Structured payloads that belong
 * to higher layers (prompts, completions, tool I/O, results, effect/memory values) are stored as
 * opaque {@link JsonNode} so this module stays decoupled from {@code catalyst-core}.
 *
 * <p>Reserved slots present from day one so later milestones add behaviour without a breaking schema
 * change: {@link RetryRequested}, {@link ExecutionPaused} (the {@code WAITING} state), and
 * {@link ExecutionBranched}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = CatalystEvent.ExecutionCreated.class, name = "ExecutionCreated"),
        @JsonSubTypes.Type(value = CatalystEvent.ExecutionStarted.class, name = "ExecutionStarted"),
        @JsonSubTypes.Type(value = CatalystEvent.PromptBuilt.class, name = "PromptBuilt"),
        @JsonSubTypes.Type(value = CatalystEvent.CompletionRequested.class, name = "CompletionRequested"),
        @JsonSubTypes.Type(value = CatalystEvent.CompletionReceived.class, name = "CompletionReceived"),
        @JsonSubTypes.Type(value = CatalystEvent.ToolRequested.class, name = "ToolRequested"),
        @JsonSubTypes.Type(value = CatalystEvent.ToolCompleted.class, name = "ToolCompleted"),
        @JsonSubTypes.Type(value = CatalystEvent.EffectRecorded.class, name = "EffectRecorded"),
        @JsonSubTypes.Type(value = CatalystEvent.MemoryRead.class, name = "MemoryRead"),
        @JsonSubTypes.Type(value = CatalystEvent.MemoryWritten.class, name = "MemoryWritten"),
        @JsonSubTypes.Type(value = CatalystEvent.RetryRequested.class, name = "RetryRequested"),
        @JsonSubTypes.Type(value = CatalystEvent.ExecutionPaused.class, name = "ExecutionPaused"),
        @JsonSubTypes.Type(value = CatalystEvent.ExecutionResumed.class, name = "ExecutionResumed"),
        @JsonSubTypes.Type(value = CatalystEvent.ExecutionBranched.class, name = "ExecutionBranched"),
        @JsonSubTypes.Type(value = CatalystEvent.ExecutionCompleted.class, name = "ExecutionCompleted"),
        @JsonSubTypes.Type(value = CatalystEvent.ExecutionFailed.class, name = "ExecutionFailed"),
        @JsonSubTypes.Type(value = CatalystEvent.ExecutionCancelled.class, name = "ExecutionCancelled"),
        @JsonSubTypes.Type(value = CatalystEvent.SignalAwaited.class, name = "SignalAwaited"),
        @JsonSubTypes.Type(value = CatalystEvent.SignalReceived.class, name = "SignalReceived"),
        @JsonSubTypes.Type(value = CatalystEvent.TimerStarted.class, name = "TimerStarted"),
        @JsonSubTypes.Type(value = CatalystEvent.TimerFired.class, name = "TimerFired"),
        @JsonSubTypes.Type(value = CatalystEvent.ChildStarted.class, name = "ChildStarted"),
        @JsonSubTypes.Type(value = CatalystEvent.ChildCompleted.class, name = "ChildCompleted"),
        @JsonSubTypes.Type(value = CatalystEvent.ChildSpawned.class, name = "ChildSpawned"),
        @JsonSubTypes.Type(value = CatalystEvent.ChildAwaited.class, name = "ChildAwaited"),
})
public sealed interface CatalystEvent {

    /** Wall-clock instant at which this event was recorded. */
    Instant at();

    // ── Lifecycle ────────────────────────────────────────────────────────────

    /** A new execution was created: task type, args hash, config fingerprint, idempotency key. */
    record ExecutionCreated(Instant at, String taskType, String argsHash,
                            String configFingerprint, String idempotencyKey) implements CatalystEvent {}

    /** Execution (re-)started running under the given attempt number and node id. */
    record ExecutionStarted(Instant at, int attempt, String nodeId) implements CatalystEvent {}

    /** Execution resumed after a crash or pause under the given attempt number. */
    record ExecutionResumed(Instant at, int attempt) implements CatalystEvent {}

    /** Execution paused (reserved for v1 durable signals / human-in-the-loop; WAITING state). */
    record ExecutionPaused(Instant at, String reason) implements CatalystEvent {}

    /** Execution completed successfully with the folded result (opaque JSON). */
    record ExecutionCompleted(Instant at, JsonNode result) implements CatalystEvent {}

    /** Execution failed. {@code failedSeq} points at the boundary that raised. */
    record ExecutionFailed(Instant at, String error, long failedSeq) implements CatalystEvent {}

    /**
     * Execution was cancelled by request — a clean, deliberate stop that folds to {@code CANCELLED}
     * rather than {@code FAILED}. {@code atSeq} points at the boundary reached when cancellation was
     * observed (the log tail for an out-of-band cancel, or the live boundary a running task unwound at).
     */
    record ExecutionCancelled(Instant at, String reason, long atSeq) implements CatalystEvent {}

    // ── Signals (spec §5 WAITING) ──────────────────────────────────────────────

    /**
     * The task is waiting for the named signal: folds the execution to {@code WAITING}. Not a
     * substitutable boundary on its own — the boundary's recorded result is the {@link SignalReceived}
     * that follows it. Recorded with no thread held: the attempt unwinds once it is appended.
     *
     * <p>{@code payloadType} is the class name the task awaits, so a signal of another type can be
     * refused before it is recorded — once recorded, a payload the task cannot accept would fail every
     * resume, and the log is append-only.
     */
    record SignalAwaited(Instant at, String name, String payloadType) implements CatalystEvent {}

    /**
     * A signal delivered to an execution waiting for it: the recorded result of its await boundary,
     * substituted on resume/replay like any other. Folds {@code WAITING → RESUMING} — a state change,
     * not just a wake-up — so a crash between delivery and the resume leaves an execution startup
     * recovery resumes, rather than a waiting one whose signal nothing will ever consume.
     */
    record SignalReceived(Instant at, String name, JsonNode payload) implements CatalystEvent {}

    /**
     * A durable timer was started by {@code ctx.sleep(duration)}, waking at {@code wakeAt} (the
     * runtime clock at the sleep, plus the duration). The execution folds to {@code WAITING} and
     * holds no thread. Like {@link SignalAwaited} it is not a boundary: it records <em>when</em> to
     * wake, so a resume, on any node, after any restart, waits only for what is left. Recorded again
     * if a live attempt reaches a timer that has not come due, so the fold returns to WAITING.
     */
    record TimerStarted(Instant at, String timerId, Instant wakeAt) implements CatalystEvent {}

    /**
     * The timer came due: recorded by the attempt that resumed at or after {@code wakeAt}, so the
     * execution's own worker is still the only writer. The sleep's boundary (identity = the timer
     * id), substituted on replay so the sleep returns at once.
     */
    record TimerFired(Instant at, String timerId) implements CatalystEvent {}

    /**
     * The parent started a child execution with {@code ctx.child(task)}: {@code childRef} is the
     * child's place in the parent's code ({@code child-1}, {@code child-2}, … in task order),
     * {@code childId} its own execution. Recorded before the child is created, so a crash between the
     * two leaves an intent the next attempt completes. The parent then waits as {@code WAITING},
     * holding no thread. Like {@link TimerStarted} it is not a boundary, and it is recorded again if a
     * live attempt reaches a child that has not finished, so the fold returns to WAITING.
     */
    record ChildStarted(Instant at, String childRef, String childId, String taskType) implements CatalystEvent {}

    /**
     * The child reached a terminal state: recorded by the parent's own attempt when it resumed and
     * found the child done, so the parent's worker is still the only writer of the parent's stream.
     * The {@code ctx.child} boundary (identity = {@code childRef}), substituted on replay.
     * {@code result} is the child's recorded result when it completed. Otherwise {@code status}
     * ({@code FAILED}/{@code CANCELLED}) and {@code error} say how it ended.
     */
    record ChildCompleted(Instant at, String childRef, String childId, String status, JsonNode result, String error)
            implements CatalystEvent {}

    /**
     * The parent started a child with {@code ctx.spawn(task)} and carried on without waiting (fan-out).
     * The same fields as {@link ChildStarted}, but the parent stays running. Not a boundary. Its
     * outcome is recorded as a {@link ChildCompleted} when the parent awaits it.
     */
    record ChildSpawned(Instant at, String childRef, String childId, String taskType) implements CatalystEvent {}

    /**
     * The parent waits, holding no thread, until any one of these spawned children ends
     * ({@code ctx.await} names one, {@code ctx.awaitAny} several). Folds to {@code WAITING}. Not a
     * boundary, and recorded again if a live attempt reaches the same wait unfinished.
     */
    record ChildAwaited(Instant at, java.util.List<String> childRefs, java.util.List<String> childIds) implements CatalystEvent {}

    // ── Model boundary ───────────────────────────────────────────────────────

    /** A prompt was built: canonical hash plus the full prompt (opaque JSON) for forensics. */
    record PromptBuilt(Instant at, String promptHash, JsonNode prompt) implements CatalystEvent {}

    /** A completion was requested from the model; carries the canonical request hash. */
    record CompletionRequested(Instant at, String requestHash) implements CatalystEvent {}

    /** A completion was received: the completion (opaque JSON), token usage, latency and cost. */
    record CompletionReceived(Instant at, JsonNode completion, long promptTokens,
                              long completionTokens, long latencyMillis, double costUsd,
                              String finishReason) implements CatalystEvent {}

    // ── Tool boundary (ToolRequested appended BEFORE execution — in-doubt marker) ──

    /** A tool invocation was requested; appended before the tool runs. */
    record ToolRequested(Instant at, String toolName, JsonNode input) implements CatalystEvent {}

    /** A tool invocation completed with an output or an error, and its latency. */
    record ToolCompleted(Instant at, JsonNode output, String error, long latencyMillis)
            implements CatalystEvent {}

    // ── Other recorded boundaries ──────────────────────────────────────────────

    /** An explicit effect captured nondeterminism at a labelled call site (spec §6). */
    record EffectRecorded(Instant at, String label, JsonNode value) implements CatalystEvent {}

    /** A working-memory read, recorded with the value-at-read for determinism. */
    record MemoryRead(Instant at, String key, JsonNode value) implements CatalystEvent {}

    /** A working-memory write. */
    record MemoryWritten(Instant at, String key, JsonNode value) implements CatalystEvent {}

    /**
     * A failed attempt will be retried as a new attempt on the same stream (retry-as-attempt). Appended
     * instead of {@link ExecutionFailed} when a retry policy elects to retry; the retry then re-enters
     * the task with the recorded prefix substituted (an {@link ExecutionResumed} follows).
     *
     * <p>{@code failedSeq} is the seq of the recorded boundary whose live failure triggered the retry —
     * today a {@code ToolCompleted} carrying an error — or {@code -1} when the failure was not an
     * unhandled recorded boundary (a model/effect failure records no result, and a failure the task
     * caught and rethrew is not attributable to a boundary). The replay seeder drops that boundary so the
     * retry re-runs it live instead of substituting the recorded failure.
     */
    record RetryRequested(Instant at, String cause, long backoffMillis, long failedSeq) implements CatalystEvent {}

    /** One execution forked into many at a branch point (reserved for M2 branching). */
    record ExecutionBranched(Instant at, String parentId, long branchPointSeq,
                             String changedComponents) implements CatalystEvent {}
}
