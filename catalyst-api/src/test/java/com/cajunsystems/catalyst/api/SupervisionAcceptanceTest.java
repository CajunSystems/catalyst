package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Status;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Supervision exit criterion on the durable log, driving the same code as {@code Demo supervise}: a
 * failed child is restarted as fresh executions, across a runtime restart during the backoff, and
 * the coordinator replays exactly.
 */
class SupervisionAcceptanceTest {

    @Test
    void aFailedChildIsRestartedAcrossARuntimeRestart(@TempDir Path dir) {
        SupervisionDemo.Outcome o = SupervisionDemo.run(dir);
        assertThat(o.refs()).containsExactly("child-1", "child-1#2", "child-1#3");
        assertThat(o.childStatuses()).containsExactly(Status.FAILED, Status.FAILED, Status.COMPLETED);
        assertThat(o.parentAtShutdown()).isEqualTo(Status.WAITING);
        assertThat(o.result()).isEqualTo("summary=feed ok");
        assertThat(o.callsBeforeRestart()).isZero();
        assertThat(o.callsAfterRestart()).isEqualTo(1);
        assertThat(o.replayCalls()).isZero();
    }
}
