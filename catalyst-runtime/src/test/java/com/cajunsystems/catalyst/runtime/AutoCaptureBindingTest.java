package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.capture.AutoCapture;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.PayloadCodec;
import com.cajunsystems.catalyst.engine.RetryPolicy;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.SequencedEvent;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The runtime half of auto-capture: that the executing {@link com.cajunsystems.catalyst.Context} is
 * bound to the task thread, so captures become ordinary recorded boundaries that resume, replay and
 * branch substitute.
 *
 * <p>These tests call {@link AutoCapture} directly rather than through the agent — that is precisely
 * what an instrumented call site compiles to, so the wiring is exercised without a bytecode dependency.
 * The agent's own tests cover the rewriting.
 */
class AutoCaptureBindingTest {

    /** A task that reads the clock and a UUID *without* wrapping them in ctx.effect. */
    private static Task<String> unwrappedNondeterminism() {
        return ctx -> AutoCapture.now() + "|" + AutoCapture.randomUUID();
    }

    /** The folded result is the raw payload envelope; decode it back to the task's own value. */
    private static Object decoded(ExecutionState state) {
        return new PayloadCodec().fromTree(state.result());
    }

    private static List<String> effectLabels(List<SequencedEvent> events) {
        List<String> labels = new ArrayList<>();
        for (SequencedEvent se : events) {
            if (se.event() instanceof CatalystEvent.EffectRecorded er) labels.add(er.label());
        }
        return labels;
    }

    @Test
    void capturesFromTaskCodeBecomeRecordedEffects() {
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory()).build()) {
            ExecutionHandle<String> handle =
                    runtime.execute(unwrappedNondeterminism(), ExecutionOptions.withKey("k"));
            String result = handle.result();

            assertThat(effectLabels(runtime.log().read(handle.id())))
                    .containsExactly("auto:Instant.now#0", "auto:UUID.randomUUID#1");
            assertThat(result).isNotBlank();
        }
    }

    @Test
    void replaySubstitutesCapturedValuesExactly() {
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory()).build()) {
            ExecutionHandle<String> handle =
                    runtime.execute(unwrappedNondeterminism(), ExecutionOptions.withKey("k"));
            String recorded = handle.result();

            // A pure replay may append nothing and make no live call: if the clock and UUID were not
            // substituted from the log, this would trip NonDeterministicReplayException instead.
            ExecutionState replayed = runtime.replay(handle.id(), unwrappedNondeterminism());
            assertThat(replayed.status()).isEqualTo(Status.COMPLETED);
            assertThat(decoded(replayed)).isEqualTo(recorded);
        }
    }

    @Test
    void theSameTaskWithoutCaptureRecomputesDifferentValuesOnReplay() {
        // The control case that makes the feature worth having. Raw JDK calls record nothing, so a
        // replay silently recomputes: no exception is raised (the task has no boundaries to diverge
        // at) but the value the replayed task produced is not the one the log says it produced.
        List<String> computed = new ArrayList<>();
        Task<String> raw = ctx -> {
            String value = Instant.now() + "|" + UUID.randomUUID();
            computed.add(value);
            return value;
        };
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory()).build()) {
            ExecutionHandle<String> handle = runtime.execute(raw, ExecutionOptions.withKey("k"));
            handle.result();
            runtime.replay(handle.id(), raw);

            assertThat(computed).hasSize(2);
            assertThat(computed.get(1)).isNotEqualTo(computed.get(0));
        }

        // With capture, the very same shape of task recomputes the *same* value.
        List<String> captured = new ArrayList<>();
        Task<String> wrapped = ctx -> {
            String value = AutoCapture.now() + "|" + AutoCapture.randomUUID();
            captured.add(value);
            return value;
        };
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory()).build()) {
            ExecutionHandle<String> handle = runtime.execute(wrapped, ExecutionOptions.withKey("k"));
            handle.result();
            runtime.replay(handle.id(), wrapped);

            assertThat(captured).hasSize(2);
            assertThat(captured.get(1)).isEqualTo(captured.get(0));
        }
    }

    @Test
    void capturesAreSuppressedInsideAManualEffect() {
        // A capture nested in a manual effect must not append its own event: the enclosing boundary
        // already records the value, and an extra event would desynchronise the boundary queue.
        Task<String> task = ctx -> ctx.effect("stamp", () -> AutoCapture.now().toString());
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory()).build()) {
            ExecutionHandle<String> handle = runtime.execute(task, ExecutionOptions.withKey("k"));
            String recorded = handle.result();

            assertThat(effectLabels(runtime.log().read(handle.id()))).containsExactly("stamp");

            ExecutionState replayed = runtime.replay(handle.id(), task);
            assertThat(decoded(replayed)).isEqualTo(recorded);
        }
    }

    @Test
    void reEnteringTheTaskSubstitutesCapturedBoundariesRatherThanRedrawing() {
        // A retry re-enters the task from the top on the same stream — the same substitution path a
        // crash resume takes. The captured identity must survive it: re-drawing here would mean a
        // resumed execution silently changed the value it had already committed to its log.
        List<String> seen = new ArrayList<>();
        boolean[] failOnce = {true};
        Task<String> task = ctx -> {
            String id = AutoCapture.randomUUID().toString();
            seen.add(id);
            if (failOnce[0]) {
                failOnce[0] = false;
                throw new IllegalStateException("transient");
            }
            return id;
        };

        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(EventLogs.inMemory())
                .retryPolicy(RetryPolicy.maxRetries(1, Duration.ZERO))
                .build()) {

            ExecutionHandle<String> handle = runtime.execute(task, ExecutionOptions.withKey("k"));

            assertThat(handle.result()).isEqualTo(seen.get(0));
            assertThat(seen).hasSize(2);
            assertThat(seen.get(1)).isEqualTo(seen.get(0)); // substituted, not re-drawn
            // One capture in the log, not one per attempt.
            assertThat(effectLabels(runtime.log().read(handle.id())))
                    .containsExactly("auto:UUID.randomUUID#0");
        }
    }
}
