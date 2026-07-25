package com.cajunsystems.catalyst.agent.fixture;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.Task;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Task code as a user would actually write it: plain JDK calls, no {@code ctx.effect} in sight. This
 * package is the one the agent's tests instrument, so these classes are compiled untouched and
 * rewritten at load (or retransform) time.
 */
public final class NondeterministicTasks {

    private NondeterministicTasks() {}

    /** The everyday case: a timestamp and an id, unwrapped. */
    public static final class TimestampAndId implements Task<String> {
        @Override
        public String execute(Context ctx) {
            return Instant.now() + "|" + UUID.randomUUID();
        }
    }

    /** Every capture source at once, all inline in the task body. */
    public static final class EverySource implements Task<String> {
        @Override
        public String execute(Context ctx) {
            List<String> parts = new ArrayList<>();
            parts.add(Instant.now().toString());
            parts.add(String.valueOf(System.currentTimeMillis()));
            parts.add(String.valueOf(System.nanoTime()));
            parts.add(LocalDate.now().toString());
            parts.add(UUID.randomUUID().toString());
            parts.add(String.valueOf(Math.random()));
            parts.add(String.valueOf(new Random().nextInt(1_000)));
            parts.add(String.valueOf(ThreadLocalRandom.current().nextLong()));
            return String.join("|", parts);
        }
    }

    /** Nondeterminism reached indirectly: through another class, and through a lambda body. */
    static final class Helper {
        static String drawViaHelper() {
            return UUID.randomUUID().toString();
        }

        static String drawInLambda() {
            java.util.function.Supplier<String> inLambda = () -> UUID.randomUUID().toString();
            return inLambda.get();
        }
    }

    /** Isolates the two indirections: a plain call into another class, then a lambda body. */
    public static final class ViaHelperThenLambda implements Task<String> {
        @Override
        public String execute(Context ctx) {
            return Helper.drawViaHelper() + "|" + Helper.drawInLambda();
        }
    }

    /** Nondeterminism inside a loop: labels must stay positional and dense. */
    public static final class InALoop implements Task<String> {
        private final int iterations;

        public InALoop(int iterations) {
            this.iterations = iterations;
        }

        @Override
        public String execute(Context ctx) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < iterations; i++) {
                sb.append(UUID.randomUUID()).append(';');
            }
            return sb.toString();
        }
    }

    /** A manual effect wrapping a capture — the nested capture must not record separately. */
    public static final class ManuallyWrapped implements Task<String> {
        @Override
        public String execute(Context ctx) {
            return ctx.effect("stamp", () -> Instant.now().toString());
        }
    }
}
