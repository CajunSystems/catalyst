package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Status;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Supervised-group exit criterion on the durable log, driving the same code as {@code Demo group}:
 * one child's failure restarts the whole group one-for-all (its running siblings cancelled), across
 * a runtime restart during the backoff, and the coordinator replays exactly.
 */
class GroupAcceptanceTest {

    @Test
    void oneFailureRestartsTheWholeGroupAcrossARestart(@TempDir Path dir) {
        GroupDemo.Outcome o = GroupDemo.run(dir);
        assertThat(o.childStatuses()).containsExactly(Status.CANCELLED, Status.FAILED, Status.CANCELLED,
                Status.COMPLETED, Status.COMPLETED, Status.COMPLETED);
        assertThat(o.result()).isEqualTo("draft=ok; factcheck=ok; summary=ok");
        assertThat(o.callsBeforeRestart()).isZero();
        assertThat(o.callsAfterRestart()).isEqualTo(3);
        assertThat(o.replayCalls()).isZero();
    }
}
