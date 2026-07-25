package com.cajunsystems.catalyst.agent;

import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.agent.fixture.NondeterministicTasks;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.cajunsystems.catalyst.runtime.EventLogs;
import com.cajunsystems.catalyst.runtime.ExecutionHandle;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code sources} selector has to gate the rewriting itself, not merely the configuration object —
 * excluding a source is how a task avoids paying an event per call for nondeterminism it does not
 * treat as data.
 */
class CaptureSourceSelectionTest {

    private static List<String> effectLabels(List<SequencedEvent> events) {
        List<String> labels = new ArrayList<>();
        for (SequencedEvent se : events) {
            if (se.event() instanceof CatalystEvent.EffectRecorded er) labels.add(er.label());
        }
        return labels;
    }

    @Test
    void capturesOnlyTheSelectedSources() {
        AutoCaptureConfig identityOnly = AutoCaptureConfig
                .forPackages("com.cajunsystems.catalyst.agent.fixture")
                .withSources(CaptureSource.IDENTITY);

        try (AutoCaptureAgent.Installation installation = AutoCaptureAgent.install(identityOnly);
             CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory()).build()) {

            ExecutionHandle<String> handle = runtime.execute(
                    new NondeterministicTasks.EverySource(), ExecutionOptions.withKey("k"));
            handle.result();

            // The clock and random draws stay untouched; only the UUID becomes a boundary.
            assertThat(effectLabels(runtime.log().read(handle.id())))
                    .containsExactly("auto:UUID.randomUUID#0");
        }
    }

    @Test
    void capturesNothingWhenNoPackageMatches() {
        AutoCaptureConfig elsewhere = AutoCaptureConfig.forPackages("com.acme.not.on.this.classpath");

        try (AutoCaptureAgent.Installation installation = AutoCaptureAgent.install(elsewhere);
             CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory()).build()) {

            ExecutionHandle<String> handle = runtime.execute(
                    new NondeterministicTasks.TimestampAndId(), ExecutionOptions.withKey("k"));
            handle.result();

            assertThat(effectLabels(runtime.log().read(handle.id()))).isEmpty();
        }
    }
}
