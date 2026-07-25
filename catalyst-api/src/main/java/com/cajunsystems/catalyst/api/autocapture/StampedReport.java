package com.cajunsystems.catalyst.api.autocapture;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.Task;

import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The auto-capture exit demo's task, and the whole point of the feature: a timestamp, an id and a
 * nonce, written the way anyone would write them — no {@code ctx.effect}, no {@code Clock} injection,
 * no ceremony. Whether this task is replayable depends entirely on whether the agent instrumented it.
 *
 * <p>It lives in its own package so the demo and the acceptance test can instrument exactly this code
 * and nothing else in {@code catalyst-api}.
 *
 * <p>Every value the task computes is also kept in a static list. That is what makes the failure mode
 * observable: an uninstrumented run recomputes a <em>different</em> value on replay and raises nothing
 * at all, because a task with no recorded boundaries has nothing to diverge at. Comparing what the
 * task actually computed is the only way to see it.
 */
public final class StampedReport implements Task<String> {

    private static final List<String> COMPUTED = new CopyOnWriteArrayList<>();

    @Override
    public String execute(Context ctx) {
        String stamp = Instant.now().toString();
        String id = UUID.randomUUID().toString();
        int nonce = new Random().nextInt(1_000_000);

        String value = stamp + "|" + id + "|" + nonce;
        COMPUTED.add(value);
        return value;
    }

    /** Every value produced since the last {@link #reset()}, in order. */
    public static List<String> computed() {
        return List.copyOf(COMPUTED);
    }

    public static void reset() {
        COMPUTED.clear();
    }

    /** The package this task lives in — what the agent is pointed at. */
    public static String instrumentedPackage() {
        return StampedReport.class.getPackageName();
    }
}
