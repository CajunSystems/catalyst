package com.cajunsystems.catalyst.capture;

import com.cajunsystems.catalyst.Context;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.random.RandomGenerator;

/**
 * The runtime half of auto-capture (spec §6): the bridge that instrumented task code calls instead of
 * the JDK's nondeterministic sources, so {@code Instant.now()}, {@code UUID.randomUUID()} and
 * {@code Random} draws are recorded and substituted <em>without</em> the task author wrapping each one
 * in {@link Context#effect}.
 *
 * <p>This class holds no bytecode machinery — it is a plain static bridge with no dependencies beyond
 * {@code catalyst-core}, so task code that is instrumented still links and runs when the agent is
 * absent. The rewriting itself lives in the separate {@code catalyst-agent} module; see
 * {@code AutoCaptureAgent}. The split matters for classloading: instrumented call sites resolve this
 * class through the task's own loader, which already has {@code catalyst-core} on it.
 *
 * <h2>How a capture becomes a boundary</h2>
 * The runtime {@link #bind binds} the executing {@link Context} to the task's thread. Each bridge call
 * then delegates to {@code ctx.effect(label, realCall)} — a recorded boundary like any other, so it is
 * executed once on the first run and substituted from the log on resume, replay and branch. With no
 * context bound (the same class exercised outside a Catalyst execution) every method performs the
 * plain JDK call, so instrumented code keeps working unchanged.
 *
 * <h2>Labels are positional</h2>
 * Captures are labelled {@code auto:<source>#<n>} where {@code n} counts captures within one execution
 * attempt. Because a resume, replay or branch re-enters the task from the top, the counter realigns
 * with the recorded prefix. The {@code auto:} prefix is reserved — do not name a manual effect with it.
 * A task whose <em>sequence</em> of nondeterministic calls varies between runs is nondeterministic in
 * a way auto-capture cannot repair; strict replay reports it as a label divergence.
 *
 * <h2>Recorded boundaries suppress capture</h2>
 * Nondeterminism inside a boundary that is <em>already</em> recorded needs no capture of its own — the
 * boundary's own result is what gets substituted. The engine therefore {@link #suppress suppresses}
 * capture while a manual {@code effect} supplier or a tool body runs. Without that, an
 * {@code Instant.now()} inside {@code ctx.effect("t", ...)} would append its own event <em>before</em>
 * the enclosing one and desynchronise the boundary queue on replay.
 *
 * <h2>Threading</h2>
 * The binding is per-thread and not inherited: auto-capture covers the task's own thread, which is
 * where deterministic task code belongs. Work the task hands to another thread is not captured.
 */
public final class AutoCapture {

    /** Prefix marking an effect label as machine-generated. Reserved; manual labels must not use it. */
    public static final String LABEL_PREFIX = "auto:";

    private static final ThreadLocal<Binding> CURRENT = new ThreadLocal<>();

    private AutoCapture() {}

    /** The context bound to the current thread, plus the per-attempt capture counter. */
    private static final class Binding {
        final Context ctx;
        int counter;
        boolean suppressed;

        Binding(Context ctx) {
            this.ctx = ctx;
        }

        <T> T capture(String source, Supplier<T> real) {
            String label = LABEL_PREFIX + source + "#" + counter++;
            // Guard against re-entering the bridge from inside the supplier or the Context
            // implementation: a nested capture would append its event inside this one.
            boolean previous = suppressed;
            suppressed = true;
            try {
                return ctx.effect(label, real);
            } finally {
                suppressed = previous;
            }
        }
    }

    /** An active binding; closing it restores whatever was bound before. */
    public interface Scope extends AutoCloseable {
        @Override
        void close();
    }

    /**
     * Binds {@code ctx} to the calling thread for the duration of the returned scope, so instrumented
     * code running on this thread records its nondeterminism into {@code ctx}. Nested binds restore the
     * enclosing binding on close.
     */
    public static Scope bind(Context ctx) {
        if (ctx == null) throw new IllegalArgumentException("ctx");
        Binding previous = CURRENT.get();
        CURRENT.set(new Binding(ctx));
        return () -> {
            if (previous == null) {
                CURRENT.remove(); // don't strand a binding on a pooled or virtual thread
            } else {
                CURRENT.set(previous);
            }
        };
    }

    /** True if instrumented code on this thread would currently be captured. */
    public static boolean isCapturing() {
        Binding b = CURRENT.get();
        return b != null && !b.suppressed;
    }

    /**
     * Suppresses capture on this thread until the returned previous state is handed back to
     * {@link #restore}. Used by the engine to wrap code that is already inside a recorded boundary
     * (a manual {@code effect} supplier, a tool body):
     *
     * <pre>{@code
     * boolean previous = AutoCapture.suppress();
     * try { ... } finally { AutoCapture.restore(previous); }
     * }</pre>
     */
    public static boolean suppress() {
        Binding b = CURRENT.get();
        if (b == null) return true; // nothing bound: already inert, and restore() is a no-op
        boolean previous = b.suppressed;
        b.suppressed = true;
        return previous;
    }

    /** Restores the suppression state {@link #suppress} returned. */
    public static void restore(boolean previous) {
        Binding b = CURRENT.get();
        if (b != null) b.suppressed = previous;
    }

    /** Routes through the bound context, or performs {@code real} directly when capture is inactive. */
    private static <T> T capture(String source, Supplier<T> real) {
        Binding b = CURRENT.get();
        if (b == null || b.suppressed) return real.get();
        return b.capture(source, real);
    }

    // ── Time ───────────────────────────────────────────────────────────────────
    // Only the no-argument forms are captured. An overload taking an explicit Clock is the caller
    // already controlling determinism, and Catalyst does not second-guess it.

    public static Instant now() {
        return capture("Instant.now", Instant::now);
    }

    public static long currentTimeMillis() {
        return capture("System.currentTimeMillis", System::currentTimeMillis);
    }

    /**
     * Captured because the value is nondeterministic, even though it is most often used for elapsed-time
     * measurement rather than as data. A task that only times itself can exclude the time source from
     * the agent's capture set.
     */
    public static long nanoTime() {
        return capture("System.nanoTime", System::nanoTime);
    }

    public static LocalDate localDateNow() {
        return capture("LocalDate.now", LocalDate::now);
    }

    public static LocalTime localTimeNow() {
        return capture("LocalTime.now", LocalTime::now);
    }

    public static LocalDateTime localDateTimeNow() {
        return capture("LocalDateTime.now", LocalDateTime::now);
    }

    public static ZonedDateTime zonedDateTimeNow() {
        return capture("ZonedDateTime.now", ZonedDateTime::now);
    }

    public static OffsetDateTime offsetDateTimeNow() {
        return capture("OffsetDateTime.now", OffsetDateTime::now);
    }

    // ── Identity ───────────────────────────────────────────────────────────────

    public static UUID randomUUID() {
        return capture("UUID.randomUUID", UUID::randomUUID);
    }

    // ── Randomness ─────────────────────────────────────────────────────────────
    // Instance draws take the receiver as their first parameter, which is how the bytecode
    // substitution passes it. The receiver is typed as RandomGenerator rather than Random so one
    // bridge covers Random, ThreadLocalRandom, SecureRandom and SplittableRandom (which is a
    // RandomGenerator but not a Random). What is recorded is the value the receiver drew, so a
    // seeded generator behaves identically whether or not it is captured.

    public static double mathRandom() {
        return capture("Math.random", Math::random);
    }

    public static int nextInt(RandomGenerator receiver) {
        return capture("Random.nextInt", receiver::nextInt);
    }

    public static int nextInt(RandomGenerator receiver, int bound) {
        return capture("Random.nextInt", () -> receiver.nextInt(bound));
    }

    public static int nextInt(RandomGenerator receiver, int origin, int bound) {
        return capture("Random.nextInt", () -> receiver.nextInt(origin, bound));
    }

    public static long nextLong(RandomGenerator receiver) {
        return capture("Random.nextLong", receiver::nextLong);
    }

    public static double nextDouble(RandomGenerator receiver) {
        return capture("Random.nextDouble", receiver::nextDouble);
    }

    public static float nextFloat(RandomGenerator receiver) {
        return capture("Random.nextFloat", receiver::nextFloat);
    }

    public static boolean nextBoolean(RandomGenerator receiver) {
        return capture("Random.nextBoolean", receiver::nextBoolean);
    }

    public static double nextGaussian(RandomGenerator receiver) {
        return capture("Random.nextGaussian", receiver::nextGaussian);
    }

    /**
     * Fills {@code bytes} from the recorded draw. The captured value is a fresh array of the same
     * length — the caller's array is an out-parameter, so it is written from the recorded bytes rather
     * than being the thing recorded.
     */
    public static void nextBytes(RandomGenerator receiver, byte[] bytes) {
        byte[] drawn = capture("Random.nextBytes", () -> {
            byte[] fresh = new byte[bytes.length];
            receiver.nextBytes(fresh);
            return fresh;
        });
        System.arraycopy(drawn, 0, bytes, 0, Math.min(drawn.length, bytes.length));
    }
}
