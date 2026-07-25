package com.cajunsystems.catalyst.agent;

import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.agent.fixture.NondeterministicTasks;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.PayloadCodec;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.cajunsystems.catalyst.runtime.EventLogs;
import com.cajunsystems.catalyst.runtime.ExecutionHandle;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end instrumentation: real bytecode rewriting of a real task class, executed and replayed
 * through a real runtime. The point of the whole feature is that these tasks contain no
 * {@code ctx.effect} calls and are nonetheless perfectly replayable.
 */
class AutoCaptureAgentTest {

    private static AutoCaptureAgent.Installation installation;

    @BeforeAll
    static void installAgent() {
        // The fixture classes are already loaded by the time JUnit gets here, so this exercises the
        // retransformation path — the same one an application using install() from main() takes.
        installation = AutoCaptureAgent.install(
                AutoCaptureConfig.forPackages("com.cajunsystems.catalyst.agent.fixture"));
    }

    @AfterAll
    static void removeAgent() {
        installation.close();
    }

    private static List<String> effectLabels(List<SequencedEvent> events) {
        List<String> labels = new ArrayList<>();
        for (SequencedEvent se : events) {
            if (se.event() instanceof CatalystEvent.EffectRecorded er) labels.add(er.label());
        }
        return labels;
    }

    private static Object decoded(ExecutionState state) {
        return new PayloadCodec().fromTree(state.result());
    }

    @Test
    void rewritesUnwrappedClockAndUuidCallsIntoRecordedBoundaries() {
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory()).build()) {
            ExecutionHandle<String> handle = runtime.execute(
                    new NondeterministicTasks.TimestampAndId(), ExecutionOptions.withKey("k"));
            String recorded = handle.result();

            assertThat(effectLabels(runtime.log().read(handle.id())))
                    .containsExactly("auto:Instant.now#0", "auto:UUID.randomUUID#1");

            // Strict replay: every boundary substituted, nothing appended, same value recomputed.
            ExecutionState replayed = runtime.replay(handle.id(), new NondeterministicTasks.TimestampAndId());
            assertThat(replayed.status()).isEqualTo(Status.COMPLETED);
            assertThat(decoded(replayed)).isEqualTo(recorded);
        }
    }

    @Test
    void capturesEverySourceIncludingHelpersAndLambdas() {
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory()).build()) {
            ExecutionHandle<String> handle = runtime.execute(
                    new NondeterministicTasks.EverySource(), ExecutionOptions.withKey("k"));
            String recorded = handle.result();

            assertThat(effectLabels(runtime.log().read(handle.id()))).containsExactly(
                    "auto:Instant.now#0",
                    "auto:System.currentTimeMillis#1",
                    "auto:System.nanoTime#2",
                    "auto:LocalDate.now#3",
                    "auto:UUID.randomUUID#4",
                    "auto:Math.random#5",
                    "auto:Random.nextInt#6",
                    "auto:Random.nextLong#7");  // ThreadLocalRandom, reached through RandomGenerator

            ExecutionState replayed = runtime.replay(handle.id(), new NondeterministicTasks.EverySource());
            assertThat(decoded(replayed)).isEqualTo(recorded);
        }
    }

    @Test
    void capturesThroughHelperClassesAndLambdaBodies() {
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory()).build()) {
            ExecutionHandle<String> handle = runtime.execute(
                    new NondeterministicTasks.ViaHelperThenLambda(), ExecutionOptions.withKey("k"));
            String recorded = handle.result();

            // Instrumentation follows the code, not the Task class: any instrumented class the task
            // calls into is rewritten, including the synthetic method a lambda body compiles to.
            assertThat(effectLabels(runtime.log().read(handle.id())))
                    .containsExactly("auto:UUID.randomUUID#0", "auto:UUID.randomUUID#1");

            ExecutionState replayed = runtime.replay(handle.id(),
                    new NondeterministicTasks.ViaHelperThenLambda());
            assertThat(decoded(replayed)).isEqualTo(recorded);
        }
    }

    @Test
    void keepsLabelsPositionalAcrossALoop() {
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory()).build()) {
            ExecutionHandle<String> handle = runtime.execute(
                    new NondeterministicTasks.InALoop(4), ExecutionOptions.withKey("k"));
            String recorded = handle.result();

            assertThat(effectLabels(runtime.log().read(handle.id()))).containsExactly(
                    "auto:UUID.randomUUID#0", "auto:UUID.randomUUID#1",
                    "auto:UUID.randomUUID#2", "auto:UUID.randomUUID#3");

            ExecutionState replayed = runtime.replay(handle.id(), new NondeterministicTasks.InALoop(4));
            assertThat(decoded(replayed)).isEqualTo(recorded);
        }
    }

    @Test
    void doesNotDoubleRecordACaptureNestedInAManualEffect() {
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory()).build()) {
            ExecutionHandle<String> handle = runtime.execute(
                    new NondeterministicTasks.ManuallyWrapped(), ExecutionOptions.withKey("k"));
            String recorded = handle.result();

            // Just the manual boundary: the instrumented Instant.now() inside it is suppressed.
            assertThat(effectLabels(runtime.log().read(handle.id()))).containsExactly("stamp");

            ExecutionState replayed = runtime.replay(handle.id(), new NondeterministicTasks.ManuallyWrapped());
            assertThat(decoded(replayed)).isEqualTo(recorded);
        }
    }

    @Test
    void instrumentedCodeStillWorksOutsideAnExecution() {
        // No runtime, no bound context: the bridge falls through to the real JDK call, so an
        // instrumented class remains an ordinary class.
        NondeterministicTasks.TimestampAndId task = new NondeterministicTasks.TimestampAndId();
        String first = runOutsideCatalyst(task);
        String second = runOutsideCatalyst(task);
        assertThat(first).isNotBlank().isNotEqualTo(second);
    }

    private static String runOutsideCatalyst(NondeterministicTasks.TimestampAndId task) {
        try {
            return task.execute(null);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
