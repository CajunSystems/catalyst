package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Status;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fan-out exit criterion on the durable log, driving the same code as {@code Demo fanout}: spawned
 * children run at the same time as separate durable executions, survive a restart without repeating
 * a model call, and the coordinator replays exactly.
 */
class FanOutAcceptanceTest {

    @Test
    void spawnedChildrenSurviveARestartAndTheCoordinatorReplays(@TempDir Path dir) {
        FanOutDemo.Outcome o = FanOutDemo.run(dir);
        assertThat(o.children()).hasSize(3).doesNotHaveDuplicates().doesNotContain(o.parent());
        assertThat(o.childrenAtShutdown()).containsOnly(Status.WAITING);
        assertThat(o.result()).isEqualTo("logs=ok; metrics=ok; traces=ok");
        assertThat(o.callsBeforeRestart()).isZero();
        assertThat(o.callsAfterRestart()).isEqualTo(3);
        assertThat(o.replayCalls()).isZero();
    }
}
