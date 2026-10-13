package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Status;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Child execution exit criterion on the durable log, driving the same code as {@code Demo child}.
 * Delegated work runs as separate durable executions, survives a restart mid-way without repeating
 * a model call, and the coordinator replays exactly.
 */
class ChildAcceptanceTest {

    @Test
    void delegatedWorkSurvivesARestartAsSeparateExecutions(@TempDir Path dir) {
        ChildDemo.Outcome o = ChildDemo.run(dir);
        assertThat(o.children()).hasSize(2).doesNotContain(o.parent());
        assertThat(o.parentWhileWaiting()).isEqualTo(Status.WAITING);
        assertThat(o.result()).isEqualTo("findings=db failover draft=postmortem v1");
        assertThat(o.callsBeforeRestart()).as("the research, once").isEqualTo(1);
        assertThat(o.callsAfterRestart()).as("the draft, once").isEqualTo(1);
        assertThat(o.replayCalls()).isZero();
    }
}
