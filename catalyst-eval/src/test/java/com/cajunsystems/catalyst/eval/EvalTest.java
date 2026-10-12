package com.cajunsystems.catalyst.eval;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.Tool;
import com.cajunsystems.catalyst.engine.BranchSpec;
import com.cajunsystems.catalyst.engine.CostModel;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Model;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.model.Usage;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.cajunsystems.catalyst.runtime.EventLogs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The eval harness over an in-memory log: three recorded triage runs, evaluated against candidate
 * models and task variants. The properties that make it an eval rather than a re-run are checked
 * directly. The recorded prefix (clock read, ticket id) is substituted. Tools are never re-run. Only
 * the candidate's model is called.
 */
class EvalTest {

    /** Counts real invocations: an eval must not add any. */
    static final class LookupTool implements Tool<String, String> {
        final AtomicInteger calls = new AtomicInteger();
        @Override public String name() { return "lookup"; }
        @Override public Class<String> inputType() { return String.class; }
        @Override public String apply(String ticket) {
            calls.incrementAndGet();
            return "ticket " + ticket + ": printer offline on floor " + ticket.length();
        }
    }

    /** A deterministic model: its answer is a function of the prompt, with a style prefix and token usage. */
    static final class StyledModel implements Model {
        final String style;
        final AtomicInteger calls = new AtomicInteger();
        StyledModel(String style) { this.style = style; }
        @Override public Completion complete(CompletionRequest request) {
            calls.incrementAndGet();
            String user = request.prompt().messages().get(request.prompt().messages().size() - 1).content();
            String text = style + "(" + user + ")";
            return new Completion(text, List.of(), new Usage(user.length(), text.length()), "stop");
        }
    }

    /** The production task: input via a recorded effect (vars are not persisted), a tool lookup, one model call. */
    static class TriageTask implements Task<String> {
        final LookupTool lookup;
        final List<String> tickets;
        TriageTask(LookupTool lookup, List<String> tickets) { this.lookup = lookup; this.tickets = tickets; }

        @Override public String execute(Context ctx) {
            String ticket = ctx.effect("ticket", () -> tickets.remove(0));
            String receivedAt = ctx.effect("received-at", () -> Instant.now().toString());
            String details = ctx.call(lookup, ticket);
            String summary = ctx.model().complete(CompletionRequest.of(Prompt.builder()
                    .system("You triage support tickets.").user(prompt(details)).build())).message();
            return summary + " @" + receivedAt;
        }

        String prompt(String details) { return "Summarize: " + details; }
    }

    /** A prompt rewrite: same steps, different wording. */
    static final class TriageTaskV2 extends TriageTask {
        TriageTaskV2(LookupTool lookup) { super(lookup, new ArrayList<>()); }
        @Override String prompt(String details) { return "One line: " + details; }
    }

    /** A variant that looks the ticket up differently: an input the recording never saw. */
    static final class TriageTaskV3 implements Task<String> {
        final LookupTool lookup;
        TriageTaskV3(LookupTool lookup) { this.lookup = lookup; }
        @Override public String execute(Context ctx) {
            String ticket = ctx.effect("ticket", () -> "unused");
            ctx.effect("received-at", () -> "unused");
            String details = ctx.call(lookup, ticket.toUpperCase());
            return ctx.model().complete(CompletionRequest.of(Prompt.builder().user(details).build())).message();
        }
    }

    private final LookupTool lookup = new LookupTool();
    private final StyledModel production = new StyledModel("sum");
    private CatalystRuntime runtime;
    private final List<ExecutionId> recorded = new ArrayList<>();

    @BeforeEach
    void record() {
        runtime = CatalystRuntime.builder().log(EventLogs.inMemory()).model(production)
                .costModel(CostModel.perMillionTokens(1_000_000, 1_000_000)).build();
        List<String> tickets = new ArrayList<>(List.of("t-1", "t-22", "t-333"));
        TriageTask task = new TriageTask(lookup, tickets);
        for (int i = 0; i < 3; i++) {
            var run = runtime.execute(task, ExecutionOptions.none());
            run.result();
            recorded.add(run.id());
        }
        recorded.forEach(id -> assertThat(runtime.inspect(id).status()).isEqualTo(Status.COMPLETED));
        assertThat(lookup.calls).hasValue(3);
    }

    @AfterEach
    void close() {
        runtime.close();
    }

    private Eval eval() {
        return Eval.on(runtime).cases(recorded).task(new TriageTask(lookup, new ArrayList<>()));
    }

    @Test
    void aCandidateThatBehavesLikeProductionPassesAndNothingButItsModelIsCalled() {
        int lookupsBefore = lookup.calls.get();
        int productionBefore = production.calls.get();
        StyledModel twin = new StyledModel("sum");

        EvalReport report = eval().candidate(Candidate.model("twin", twin)).scorer(Scorers.sameResult()).run();

        assertThat(report.summary("twin").passedAllScorers()).isEqualTo(3);
        assertThat(twin.calls).as("one live call per case, to the candidate").hasValue(3);
        assertThat(production.calls).as("the recorded model is never called").hasValue(productionBefore);
        assertThat(lookup.calls).as("tools are answered from the recording, never re-run").hasValue(lookupsBefore);
        // The clock read was substituted: the candidate's result carries the recorded timestamp.
        report.resultsFor("twin").forEach(r ->
                assertThat(r.sample().candidateResult()).isEqualTo(r.sample().baselineResult()));
    }

    @Test
    void aCandidateModelThatAnswersDifferentlyFailsSameResultWithTheDifferenceShown() {
        EvalReport report = eval().candidate(Candidate.model("terse", new StyledModel("short")))
                .scorer(Scorers.sameResult()).scorer(Scorers.sameSteps()).run();

        EvalReport.Summary s = report.summary("terse");
        assertThat(s.outcomes()).containsEntry(CaseResult.Outcome.COMPLETED, 3);
        assertThat(s.passRate()).containsEntry("same-result", 0.0);
        assertThat(s.meanValue().get("same-steps")).as("only the model step differs").isEqualTo(1.0);
        assertThat(report.pretty()).contains("candidate terse: 0/3 passed all scorers").contains("short(");
        CaseResult first = report.resultsFor("terse").get(0);
        assertThat(runtime.inspect(first.branchId()).status()).as("the run is an inspectable branch").isEqualTo(Status.COMPLETED);
    }

    @Test
    void aPromptRewriteForksAtTheModelCallAndStillReplaysItsTools() {
        int lookupsBefore = lookup.calls.get();
        EvalReport report = eval().candidate(Candidate.task("prompt-v2", new TriageTaskV2(lookup)))
                .scorer(Scorers.sameResult()).run();

        assertThat(report.summary("prompt-v2").outcomes()).containsEntry(CaseResult.Outcome.COMPLETED, 3);
        assertThat(lookup.calls).hasValue(lookupsBefore);
        assertThat(report.resultsFor("prompt-v2").get(0).sample().candidateResult().toString()).startsWith("sum(One line: ");
    }

    /** The common agent shape: the model decides, then a tool acts on its answer. Past the fork. */
    static final class ActOnAnswerTask implements Task<String> {
        final LookupTool lookup;
        ActOnAnswerTask(LookupTool lookup) { this.lookup = lookup; }
        @Override public String execute(Context ctx) {
            String ticket = ctx.effect("ticket", () -> "t-9");
            String decision = ctx.model().complete(CompletionRequest.of(Prompt.builder().user(ticket).build())).message();
            return ctx.call(lookup, decision);
        }
    }

    @Test
    void aToolCalledAfterTheModelIsAnsweredFromTheRecordingNotRunAgain() {
        ActOnAnswerTask act = new ActOnAnswerTask(lookup);
        var recordedRun = runtime.execute(act, ExecutionOptions.none());
        recordedRun.result();
        ExecutionId id = recordedRun.id();
        int lookupsBefore = lookup.calls.get();

        EvalReport report = Eval.on(runtime).cases(id).task(act)
                .candidate(Candidate.model("twin", new StyledModel("sum")))
                .candidate(Candidate.model("other", new StyledModel("different")))
                .scorer(Scorers.sameResult()).run();

        assertThat(report.summary("twin").passedAllScorers()).isEqualTo(1);
        assertThat(report.summary("other").outcomes()).as("a new decision means a call the recording never saw")
                .containsEntry(CaseResult.Outcome.UNRECORDED_TOOL, 1);
        assertThat(lookup.calls).as("neither candidate ran the tool").hasValue(lookupsBefore);
    }

    @Test
    void aCandidateThatCallsAToolTheRecordingCannotAnswerIsReportedNotRun() {
        int lookupsBefore = lookup.calls.get();
        EvalReport report = eval().candidate(Candidate.task("v3", new TriageTaskV3(lookup))).run();

        assertThat(report.summary("v3").outcomes()).containsEntry(CaseResult.Outcome.UNRECORDED_TOOL, 3);
        assertThat(lookup.calls).as("refused, not executed").hasValue(lookupsBefore);
        assertThat(report.resultsFor("v3").get(0).error()).contains("lookup");
    }

    @Test
    void liveToolsCanBeAllowedExplicitly() {
        int lookupsBefore = lookup.calls.get();
        EvalReport report = eval().candidate(Candidate.task("v3", new TriageTaskV3(lookup)))
                .toolReplay(BranchSpec.ToolReplay.RECORDED_OR_LIVE).run();

        assertThat(report.summary("v3").outcomes()).containsEntry(CaseResult.Outcome.COMPLETED, 3);
        assertThat(lookup.calls).hasValue(lookupsBefore + 3);
    }

    @Test
    void oneCandidateFailingDoesNotStopTheOthers() {
        Model broken = request -> { throw new IllegalStateException("provider down"); };
        EvalReport report = eval().candidate(Candidate.model("broken", broken))
                .candidate(Candidate.model("twin", new StyledModel("sum"))).scorer(Scorers.sameResult()).run();

        assertThat(report.summary("broken").outcomes()).containsEntry(CaseResult.Outcome.FAILED, 3);
        assertThat(report.resultsFor("broken").get(0).error()).contains("provider down");
        assertThat(report.summary("twin").passedAllScorers()).isEqualTo(3);
        assertThat(report.results()).extracting(CaseResult::candidate)
                .as("case order, candidates in order within each case")
                .containsExactly("broken", "twin", "broken", "twin", "broken", "twin");
    }

    @Test
    void aRecordedRunThatDidNotCompleteIsSkipped() {
        ExecutionId paused = ExecutionId.random();
        runtime.log().append(paused, new CatalystEvent.ExecutionCreated(Instant.EPOCH, TriageTask.class.getName(), "novars", "cfg", ""));
        runtime.log().append(paused, new CatalystEvent.ExecutionPaused(Instant.EPOCH, "operator"));
        EvalReport report = Eval.on(runtime).cases(paused).task(new TriageTask(lookup, new ArrayList<>()))
                .candidate(Candidate.model("twin", new StyledModel("sum"))).run();

        assertThat(report.results()).singleElement().satisfies(r -> {
            assertThat(r.outcome()).isEqualTo(CaseResult.Outcome.SKIPPED);
            assertThat(r.error()).contains("PAUSED");
        });
    }

    /** Cost and tokens come from the folded log; a pricier, wordier model fails a cost bar. */
    @Test
    void costAndTokensAreComparedFromTheLog() {
        EvalReport report = eval().candidate(Candidate.model("wordy", new StyledModel("a-much-longer-style-prefix")))
                .scorer(Scorers.costAtMost(1.0)).scorer(Scorers.tokensAtMost(1.0)).run();

        EvalReport.Summary s = report.summary("wordy");
        assertThat(s.candidateTokens()).isGreaterThan(s.baselineTokens());
        assertThat(s.candidateUsd()).isGreaterThan(s.baselineUsd());
        assertThat(s.passRate()).containsEntry("cost<=1.0x", 0.0).containsEntry("tokens<=1.0x", 0.0);
    }

    @Test
    void aMisconfiguredEvalIsRefusedBeforeRunningAnything() {
        assertThatThrownBy(() -> Eval.on(runtime).cases(recorded).run())
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("No candidates");
        assertThatThrownBy(() -> Eval.on(runtime).cases(recorded).candidate(Candidate.model("m", production)).run())
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("no task");
        assertThatThrownBy(() -> eval().candidate(Candidate.model("m", production)).candidate(Candidate.model("m", production)).run())
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("named 'm'");
    }
}
