package com.cajunsystems.catalyst.engine;

/**
 * Raised when a resume encounters an in-doubt boundary — a {@code ToolRequested} with no matching
 * {@code ToolCompleted}, or a {@code CompletionRequested} with no matching {@code CompletionReceived}
 * — and the {@link InDoubtPolicy} is {@link InDoubtPolicy#FAIL}.
 */
public class InDoubtException extends RuntimeException {
    public InDoubtException(String message) {
        super(message);
    }
}
