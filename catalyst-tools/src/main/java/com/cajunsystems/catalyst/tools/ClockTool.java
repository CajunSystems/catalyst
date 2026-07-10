package com.cajunsystems.catalyst.tools;

import com.cajunsystems.catalyst.Tool;

import java.time.Clock;
import java.time.Instant;

/**
 * Reads the current time. Invoked via {@code ctx.call(clockTool, query)} so the read is recorded and
 * substituted on replay — the disciplined way to bring wall-clock time into an execution without
 * breaking determinism. Deliberately <em>not</em> {@link com.cajunsystems.catalyst.Deterministic}:
 * its result depends on when it ran, so replay must substitute the recorded instant.
 */
public final class ClockTool implements Tool<ClockTool.Query, ClockTool.Now> {

    private final Clock clock;

    public ClockTool() {
        this(Clock.systemUTC());
    }

    public ClockTool(Clock clock) {
        this.clock = clock;
    }

    /** An empty query — reads "now". A field-less record keeps the input serializable and typed. */
    public record Query() {}

    public record Now(Instant instant, long epochMilli) {}

    @Override
    public String name() {
        return "clock";
    }

    @Override
    public Class<Query> inputType() {
        return Query.class;
    }

    @Override
    public Now apply(Query input) {
        Instant now = clock.instant();
        return new Now(now, now.toEpochMilli());
    }
}
