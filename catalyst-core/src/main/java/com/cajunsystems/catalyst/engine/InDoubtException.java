package com.cajunsystems.catalyst.engine;

/**
 * Raised when a resume encounters an in-doubt tool call (a {@code ToolRequested} with no matching
 * {@code ToolCompleted}) and the {@link InDoubtPolicy} is {@link InDoubtPolicy#FAIL}.
 */
public class InDoubtException extends RuntimeException {
    public InDoubtException(String message) {
        super(message);
    }
}
