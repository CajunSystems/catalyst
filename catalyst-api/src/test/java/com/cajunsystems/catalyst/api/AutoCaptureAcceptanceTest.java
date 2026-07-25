package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.agent.AutoCaptureAgent;
import com.cajunsystems.catalyst.agent.AutoCaptureConfig;
import com.cajunsystems.catalyst.api.autocapture.StampedReport;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.cajunsystems.catalyst.runtime.ExecutionHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The v0.2 auto-capture exit criterion (spec §6): a task that reads the clock, draws a UUID and draws
 * from {@code Random} — with no {@code ctx.effect} anywhere in it — is recorded and replayed exactly,
 * because the agent rewrote those call sites into recorded boundaries.
 *
 * <p>The uninstrumented control run is half the criterion. Without capture the replay does not fail
 * loudly; it silently recomputes different values, which is precisely the class of bug auto-capture
 * exists to close, and the reason "replay threw nothing" is not evidence of determinism.
 */
class AutoCaptureAcceptanceTest {

    private static List<String> effectLabels(CatalystRuntime runtime,
                                             com.cajunsystems.catalyst.ExecutionId id) {
        List<String> labels = new ArrayList<>();
        for (var se : runtime.log().read(id)) {
            if (se.event() instanceof CatalystEvent.EffectRecorded er) labels.add(er.label());
        }
        return labels;
    }

    @Test
    void unwrappedNondeterminismIsRecordedAndReplayedExactly(@TempDir Path dir) throws Exception {
        Path uninstrumented = Files.createDirectory(dir.resolve("uninstrumented"));
        Path captured = Files.createDirectory(dir.resolve("captured"));

        // ── Control: the same class, not instrumented ──
        StampedReport.reset();
        try (CatalystRuntime runtime = Catalyst.builder().log(GumboEventLog.at(uninstrumented)).build()) {
            ExecutionHandle<String> handle = runtime.execute(new StampedReport());
            handle.result();

            assertThat(effectLabels(runtime, handle.id())).isEmpty();

            // Replay raises nothing at all — there are no boundaries to diverge at ...
            ExecutionState replayed = runtime.replay(handle.id(), new StampedReport());
            assertThat(replayed.status()).isEqualTo(Status.COMPLETED);
        }
        List<String> withoutAgent = StampedReport.computed();
        // ... but the task computed a different answer the second time, and nothing said so.
        assertThat(withoutAgent).hasSize(2);
        assertThat(withoutAgent.get(1)).isNotEqualTo(withoutAgent.get(0));

        // ── With the agent attached ──
        StampedReport.reset();
        AutoCaptureConfig config = AutoCaptureConfig.forPackages(StampedReport.instrumentedPackage());
        try (AutoCaptureAgent.Installation installed = AutoCaptureAgent.install(config);
             CatalystRuntime runtime = Catalyst.builder().log(GumboEventLog.at(captured)).build()) {

            ExecutionHandle<String> handle = runtime.execute(new StampedReport());
            String recorded = handle.result();

            assertThat(effectLabels(runtime, handle.id())).containsExactly(
                    "auto:Instant.now#0", "auto:UUID.randomUUID#1", "auto:Random.nextInt#2");

            // A strict replay may append nothing and run no live boundary: reaching a live boundary
            // here would raise NonDeterministicReplayException rather than return.
            ExecutionState replayed = runtime.replay(handle.id(), new StampedReport());
            assertThat(replayed.status()).isEqualTo(Status.COMPLETED);

            List<String> withAgent = StampedReport.computed();
            assertThat(withAgent).hasSize(2);
            assertThat(withAgent.get(0)).isEqualTo(recorded);
            assertThat(withAgent.get(1)).isEqualTo(recorded); // replay reproduced the run exactly
        }
    }
}
