package com.cajunsystems.catalyst.model;

/**
 * The minimal model SPI (spec §4). The core defines no provider concepts; providers arrive through
 * adapters (v0: {@code MockModel} and {@code catalyst-langchain4j}). Catalyst never maintains
 * provider HTTP clients.
 */
@FunctionalInterface
public interface Model {

    /** Produces a completion for the given request. */
    Completion complete(CompletionRequest request);
}
