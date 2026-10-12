package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.eval.CaseResult;
import com.cajunsystems.catalyst.eval.EvalReport;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Eval harness exit criterion, on the durable log, driving the same code as {@code Demo eval}.
 * Recorded production runs are replayed against candidate models and a prompt rewrite and scored.
 * Nothing outside the candidates is touched: zero tool executions, zero calls to the recorded model.
 */
class EvalAcceptanceTest {

    @Test
    void recordedRunsAreAReplayableRegressionSuiteForCandidates(@TempDir Path dir) {
        EvalDemo.Outcome outcome = EvalDemo.run(dir);
        EvalReport report = outcome.report();

        assertThat(outcome.lookupsDuringEval()).as("tools answered from the recording").isZero();
        assertThat(outcome.productionCallsDuringEval()).as("only candidates' models called").isZero();

        assertThat(report.summary("same-model").passedAllScorers()).as("identical behaviour passes").isEqualTo(3);
        EvalReport.Summary concise = report.summary("concise-model");
        assertThat(concise.passRate()).containsEntry("same-result", 0.0).containsEntry("tokens<=1.0x", 1.0);
        assertThat(concise.candidateTokens()).isLessThan(concise.baselineTokens());
        assertThat(report.summary("prompt-v2").outcomes()).containsEntry(CaseResult.Outcome.COMPLETED, 3);
        assertThat(report.pretty()).contains("candidate concise-model: 0/3 passed all scorers");

        // Every evaluated run is a durable branch execution: reopen the log and inspect one.
        CaseResult one = report.resultsFor("prompt-v2").get(0);
        try (var reopened = Catalyst.builder().log(GumboEventLog.at(dir)).build()) {
            assertThat(reopened.inspect(one.branchId()).status()).isEqualTo(Status.COMPLETED);
        }
    }
}
