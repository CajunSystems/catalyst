package com.cajunsystems.catalyst.engine;

/**
 * A tool failure substituted from the log on replay or resume: the recorded call failed, so the
 * replayed call fails the same way, without running the tool.
 *
 * <p>{@link #recordedError()} is exactly what was recorded when the call failed live: the original
 * exception's {@code String.valueOf}. A task that turns a tool failure into data, for example an agent
 * that tells the model "the lookup failed: …", should use it for the live exception's text as well.
 * Then the data is identical live and on replay, and the next prompt does not diverge.
 */
public class RecordedToolFailureException extends RuntimeException {

    private final String toolName;
    private final String recordedError;

    public RecordedToolFailureException(String toolName, String recordedError) {
        super("Recorded tool '" + toolName + "' failed: " + recordedError);
        this.toolName = toolName;
        this.recordedError = recordedError;
    }

    public String toolName() {
        return toolName;
    }

    /** What the live failure recorded: {@code String.valueOf(theOriginalException)}. */
    public String recordedError() {
        return recordedError;
    }
}
