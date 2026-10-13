package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Status;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Durable timer exit criterion on the durable log, driving the same code as {@code Demo timer}. A
 * sleep survives its runtime shutting down, is found by the next runtime's startup recovery, wakes
 * no earlier than due, repeats no model call, and the run replays exactly.
 */
class TimerAcceptanceTest {

    @Test
    void aSleepSurvivesARestartAndWakesOnTime(@TempDir Path dir) {
        TimerDemo.Outcome o = TimerDemo.run(dir);
        assertThat(o.whileSleeping()).isEqualTo(Status.WAITING);
        assertThat(o.leftAloneAtStartup()).isTrue();
        assertThat(o.result()).isEqualTo("hello | checking in");
        assertThat(o.firstRunCalls()).isEqualTo(1);
        assertThat(o.secondRunCalls()).as("only the follow-up after the wake").isEqualTo(1);
        assertThat(o.wokeAfterMillis()).as("never early").isGreaterThanOrEqualTo(TimerDemo.SLEEP.toMillis());
        assertThat(o.replayCalls()).isZero();
    }
}
