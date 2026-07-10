package com.cajunsystems.catalyst.engine;

/**
 * Raised when, on replay, task code produces boundaries in a different order/shape than the log
 * recorded — a sign of nondeterministic task code. In M0 this is a coarse structural check
 * (boundary type/order); M1 refines it into {@code NonDeterministicReplayException} keyed on the
 * canonical request hash, and {@code BRANCH} mode turns it into a fork rather than an error.
 */
public class ReplayDivergenceException extends RuntimeException {
    public ReplayDivergenceException(String message) {
        super(message);
    }
}
