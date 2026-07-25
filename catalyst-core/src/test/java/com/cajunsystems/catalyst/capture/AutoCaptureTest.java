package com.cajunsystems.catalyst.capture;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionInfo;
import com.cajunsystems.catalyst.Memory;
import com.cajunsystems.catalyst.Tool;
import com.cajunsystems.catalyst.model.Model;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The bridge in isolation: with no bytecode involved, calling {@link AutoCapture} directly is exactly
 * what an instrumented call site does, so these tests pin the contract the agent depends on.
 */
class AutoCaptureTest {

    /** A Context that records effects and can replay them back, standing in for ReplayingContext. */
    private static final class RecordingContext implements Context {
        final List<String> labels = new ArrayList<>();
        final Map<String, Object> recorded = new LinkedHashMap<>();
        /** When non-null, effects are substituted from here instead of running the supplier. */
        Map<String, Object> substituteFrom;
        int supplierCalls;

        @Override
        @SuppressWarnings("unchecked")
        public <T> T effect(String label, Supplier<T> supplier) {
            labels.add(label);
            if (substituteFrom != null && substituteFrom.containsKey(label)) {
                return (T) substituteFrom.get(label);
            }
            supplierCalls++;
            T value = supplier.get();
            recorded.put(label, value);
            return value;
        }

        @Override public Model model() { throw new UnsupportedOperationException(); }
        @Override public <I, O> O call(Tool<I, O> tool, I input) { throw new UnsupportedOperationException(); }
        @Override public Memory memory() { throw new UnsupportedOperationException(); }
        @Override public <T> T var(String name) { return null; }
        @Override public ExecutionInfo info() { throw new UnsupportedOperationException(); }
        @Override public Logger log() { throw new UnsupportedOperationException(); }
    }

    @Test
    void performsThePlainJdkCallWhenNoContextIsBound() {
        // An instrumented class used outside a Catalyst execution must still behave normally.
        assertThat(AutoCapture.isCapturing()).isFalse();
        Instant before = Instant.now();
        Instant captured = AutoCapture.now();
        Instant after = Instant.now();
        assertThat(captured).isBetween(before, after);
        assertThat(AutoCapture.randomUUID()).isNotEqualTo(AutoCapture.randomUUID());
    }

    @Test
    void routesCapturesThroughTheBoundContextWithPositionalLabels() {
        RecordingContext ctx = new RecordingContext();
        try (AutoCapture.Scope scope = AutoCapture.bind(ctx)) {
            assertThat(AutoCapture.isCapturing()).isTrue();
            AutoCapture.now();
            AutoCapture.randomUUID();
            AutoCapture.now();
        }
        assertThat(ctx.labels).containsExactly(
                "auto:Instant.now#0", "auto:UUID.randomUUID#1", "auto:Instant.now#2");
        assertThat(AutoCapture.isCapturing()).isFalse();
    }

    @Test
    void substitutesRecordedValuesOnASecondRun() {
        RecordingContext recording = new RecordingContext();
        Instant recordedInstant;
        UUID recordedUuid;
        try (AutoCapture.Scope scope = AutoCapture.bind(recording)) {
            recordedInstant = AutoCapture.now();
            recordedUuid = AutoCapture.randomUUID();
        }

        // Replay: same call sequence, values served from the record, supplier never run.
        RecordingContext replaying = new RecordingContext();
        replaying.substituteFrom = recording.recorded;
        Instant replayedInstant;
        UUID replayedUuid;
        try (AutoCapture.Scope scope = AutoCapture.bind(replaying)) {
            replayedInstant = AutoCapture.now();
            replayedUuid = AutoCapture.randomUUID();
        }

        assertThat(replayedInstant).isEqualTo(recordedInstant);
        assertThat(replayedUuid).isEqualTo(recordedUuid);
        assertThat(replaying.supplierCalls).isZero();
    }

    @Test
    void doesNotCaptureWhileSuppressed() {
        RecordingContext ctx = new RecordingContext();
        try (AutoCapture.Scope scope = AutoCapture.bind(ctx)) {
            AutoCapture.now();
            boolean previous = AutoCapture.suppress();
            try {
                assertThat(AutoCapture.isCapturing()).isFalse();
                AutoCapture.now();     // real call, not recorded
                AutoCapture.randomUUID();
            } finally {
                AutoCapture.restore(previous);
            }
            AutoCapture.now();
        }
        // The counter advances only for actual captures, so the labels stay dense.
        assertThat(ctx.labels).containsExactly("auto:Instant.now#0", "auto:Instant.now#1");
    }

    @Test
    void suppressionNestsAndRestores() {
        RecordingContext ctx = new RecordingContext();
        try (AutoCapture.Scope scope = AutoCapture.bind(ctx)) {
            boolean outer = AutoCapture.suppress();
            boolean inner = AutoCapture.suppress();
            AutoCapture.restore(inner);
            assertThat(AutoCapture.isCapturing()).isFalse(); // still inside the outer suppression
            AutoCapture.restore(outer);
            assertThat(AutoCapture.isCapturing()).isTrue();
        }
    }

    @Test
    void bindingNestsAndRestoresThePreviousContext() {
        RecordingContext outer = new RecordingContext();
        RecordingContext inner = new RecordingContext();
        try (AutoCapture.Scope outerScope = AutoCapture.bind(outer)) {
            AutoCapture.now();
            try (AutoCapture.Scope innerScope = AutoCapture.bind(inner)) {
                AutoCapture.now();
            }
            AutoCapture.now();
        }
        assertThat(outer.labels).containsExactly("auto:Instant.now#0", "auto:Instant.now#1");
        assertThat(inner.labels).containsExactly("auto:Instant.now#0");
        assertThat(AutoCapture.isCapturing()).isFalse();
    }

    @Test
    void capturesRandomDrawsThroughTheReceiver() {
        RecordingContext ctx = new RecordingContext();
        Random random = new Random(42);
        List<Object> first = new ArrayList<>();
        try (AutoCapture.Scope scope = AutoCapture.bind(ctx)) {
            first.add(AutoCapture.nextInt(random));
            first.add(AutoCapture.nextInt(random, 100));
            first.add(AutoCapture.nextLong(random));
            first.add(AutoCapture.nextDouble(random));
            first.add(AutoCapture.nextBoolean(random));
            first.add(AutoCapture.mathRandom());
        }
        assertThat(ctx.labels).containsExactly(
                "auto:Random.nextInt#0", "auto:Random.nextInt#1", "auto:Random.nextLong#2",
                "auto:Random.nextDouble#3", "auto:Random.nextBoolean#4", "auto:Math.random#5");

        // Replay against a *differently* seeded Random: the recorded values win, so the draw sequence
        // is reproduced even though the generator's state differs.
        RecordingContext replaying = new RecordingContext();
        replaying.substituteFrom = ctx.recorded;
        List<Object> second = new ArrayList<>();
        Random other = new Random(7);
        try (AutoCapture.Scope scope = AutoCapture.bind(replaying)) {
            second.add(AutoCapture.nextInt(other));
            second.add(AutoCapture.nextInt(other, 100));
            second.add(AutoCapture.nextLong(other));
            second.add(AutoCapture.nextDouble(other));
            second.add(AutoCapture.nextBoolean(other));
            second.add(AutoCapture.mathRandom());
        }
        assertThat(second).isEqualTo(first);
        assertThat(replaying.supplierCalls).isZero();
    }

    @Test
    void advancesTheGeneratorEvenWhenTheDrawIsSubstituted() {
        // A resume substitutes the recorded prefix and then runs live. The generator is task-local
        // state, so it has to end up where the recorded run left it — otherwise the first live draw
        // continues from a stale position and repeats a value the prefix already used.
        RecordingContext recording = new RecordingContext();
        int first;
        int second;
        try (AutoCapture.Scope scope = AutoCapture.bind(recording)) {
            first = AutoCapture.nextInt(new Random(42));
            second = AutoCapture.nextInt(new Random(42)); // fresh generator: same value as `first`
        }
        assertThat(second).isEqualTo(first);

        // Now the resume: one recorded draw substituted, the next drawn live from the same generator.
        RecordingContext resuming = new RecordingContext();
        resuming.substituteFrom = Map.of("auto:Random.nextInt#0", first);
        Random generator = new Random(42);
        int substituted;
        int live;
        try (AutoCapture.Scope scope = AutoCapture.bind(resuming)) {
            substituted = AutoCapture.nextInt(generator); // served from the log
            live = AutoCapture.nextInt(generator);        // first genuinely live draw
        }

        assertThat(substituted).isEqualTo(first);
        assertThat(live).isNotEqualTo(first);            // the generator moved on
        assertThat(live).isEqualTo(new Random(42).ints().skip(1).findFirst().orElseThrow());
    }

    @Test
    void fillsNextBytesFromTheRecordedDraw() {
        RecordingContext ctx = new RecordingContext();
        byte[] recorded = new byte[16];
        try (AutoCapture.Scope scope = AutoCapture.bind(ctx)) {
            AutoCapture.nextBytes(new Random(1), recorded);
        }
        assertThat(recorded).isNotEqualTo(new byte[16]);

        RecordingContext replaying = new RecordingContext();
        replaying.substituteFrom = ctx.recorded;
        byte[] replayed = new byte[16];
        try (AutoCapture.Scope scope = AutoCapture.bind(replaying)) {
            AutoCapture.nextBytes(new Random(999), replayed);
        }
        assertThat(replayed).isEqualTo(recorded);
        assertThat(replaying.supplierCalls).isZero();
    }

    @Test
    void bindingIsNotVisibleToOtherThreads() throws Exception {
        RecordingContext ctx = new RecordingContext();
        boolean[] capturingOnOtherThread = new boolean[1];
        try (AutoCapture.Scope scope = AutoCapture.bind(ctx)) {
            Thread other = new Thread(() -> capturingOnOtherThread[0] = AutoCapture.isCapturing());
            other.start();
            other.join();
        }
        assertThat(capturingOnOtherThread[0]).isFalse();
    }
}
