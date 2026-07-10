package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.log.EventLog;

/** Factories for built-in {@link EventLog} backends. */
public final class EventLogs {

    private EventLogs() {}

    /** An in-memory event log: tests, demos, ephemeral use. */
    public static EventLog inMemory() {
        return new InMemoryEventLog();
    }
}
