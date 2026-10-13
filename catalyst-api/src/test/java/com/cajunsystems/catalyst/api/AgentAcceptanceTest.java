package com.cajunsystems.catalyst.api;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Agent exit criterion on the durable log, driving the same code as {@code Demo agent}. A
 * tool-calling agent behaves as any execution does. It resumes mid-conversation on a fresh runtime
 * without repeating a model turn or a tool call. It replays with zero calls. It is evaluated against
 * a candidate model without re-running a tool.
 */
class AgentAcceptanceTest {

    @Test
    void anAgentRunIsAnOrdinaryExecution(@TempDir Path dir) {
        AgentDemo.Outcome o = AgentDemo.run(dir);

        assertThat(o.answer()).isEqualTo("triaged: paged FACILITIES for TCK-1042");
        assertThat(o.turnsBeforeCrash()).isEqualTo(1);
        assertThat(o.modelCallsOnResume()).as("turns 2 and 3 only").isEqualTo(2);
        assertThat(o.lookupsTotal()).as("the lookup ran once, before the crash").isEqualTo(1);
        assertThat(o.pagesTotal()).isEqualTo(1);
        assertThat(o.callsDuringReplay()).isZero();
        assertThat(o.toolRunsDuringEval()).isZero();
        assertThat(o.eval().summary("same-policy").passedAllScorers()).isEqualTo(1);
        assertThat(o.eval().summary("terser-policy").passRate()).containsEntry("same-result", 0.0);
    }
}
