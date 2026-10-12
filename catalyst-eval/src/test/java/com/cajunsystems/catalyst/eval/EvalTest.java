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

    /** A status endpoint: answers "pending" until the third call. Polled repeatedly with the same input. */
    static final class StatusTool implements Tool<String, String> {
        final AtomicInteger calls = new AtomicInteger();
        @Override public String name() { return "status"; }
        @Override public Class<String> inputType() { return String.class; }
        @Override public String apply(String job) { return calls.incrementAndGet() >= 2 ? "done" : "pending"; }
    }

    /** The model names a job, then the task polls its status until done: repeated identical calls past the fork. */
    static final class PollTask implements Task<String> {
        final StatusTool status;
        PollTask(StatusTool status) { this.status = status; }
        @Override public String execute(Context ctx) {
            String job = ctx.model().complete(CompletionRequest.of(Prompt.builder().user("job").build())).message();
            List<String> seen = new ArrayList<>();
            String s;
            do {
                s = ctx.call(status, job);
                seen.add(s);
            } while (!s.equals("done") && seen.size() < 5);
            return String.join(",", seen);
        }
    }

    /**
     * Repeated identical calls are answered in recorded order. Answering each with the first recorded
     * output would make an identical candidate poll "pending" forever and look like a regression.
     */
    @Test
    void repeatedIdenticalToolCallsAreAnsweredInRecordedOrder() {
        StatusTool status = new StatusTool();
        var recordedRun = runtime.execute(new PollTask(status), ExecutionOptions.none());
        assertThat(recordedRun.result()).isEqualTo("pending,done");
        int polls = status.calls.get();

        EvalReport report = Eval.on(runtime).cases(recordedRun.id()).task(new PollTask(status))
                .candidate(Candidate.model("twin", new StyledModel("sum"))).scorer(Scorers.sameResult()).run();

        assertThat(report.summary("twin").passedAllScorers()).isEqualTo(1);
        assertThat(report.resultsFor("twin").get(0).sample().candidateResult()).isEqualTo("pending,done");
        assertThat(status.calls).as("answered from the recording").hasValue(polls);
    }

    /** One more call than the recording made has no answer left: reported, not run, not a repeat of the last one. */
    @Test
    void aCallBeyondTheRecordedOnesHasNoAnswer() {
        StatusTool status = new StatusTool();
        var recordedRun = runtime.execute(new PollTask(status), ExecutionOptions.none());
        recordedRun.result();
        Task<String> pollsOnceMore = ctx -> {
            String job = ctx.model().complete(CompletionRequest.of(Prompt.builder().user("job").build())).message();
            return ctx.call(status, job) + "," + ctx.call(status, job) + "," + ctx.call(status, job);
        };
        EvalReport report = Eval.on(runtime).cases(recordedRun.id()).task(pollsOnceMore)
                .candidate(Candidate.model("twin", new StyledModel("sum"))).run();
        assertThat(report.summary("twin").outcomes()).containsEntry(CaseResult.Outcome.UNRECORDED_TOOL, 1);
    }

    /** Not on the payload allowlist: recorded fine (toTree does not check), refused on read. */
    static final class Plain { public String value = "x"; }

    @Test
    void aCandidateWhoseResultCannotBeReadBackFailsAloneAndKeepsItsBranch() {
        Task<Plain> plain = ctx -> {
            ctx.effect("ticket", () -> "unused");
            ctx.effect("received-at", () -> "unused");
            ctx.call(lookup, "unused-input-is-fine-under-live");
            return new Plain();
        };
        EvalReport report = eval().toolReplay(BranchSpec.ToolReplay.RECORDED_OR_LIVE)
                .candidate(Candidate.task("plain", plain))
                .candidate(Candidate.model("twin", new StyledModel("sum")))
                .scorer(Scorers.sameResult()).run();

        CaseResult failed = report.resultsFor("plain").get(0);
        assertThat(failed.outcome()).isEqualTo(CaseResult.Outcome.FAILED);
        assertThat(failed.error()).contains("could not be read back");
        assertThat(failed.branchId()).as("the run is still findable").isNotNull();
        assertThat(report.summary("twin").passedAllScorers()).as("the other candidate is unaffected").isEqualTo(3);
    }

    @Test
    void aScorerThatThrowsFailsThatScoreNotTheEvaluation() {
        EvalReport report = eval().candidate(Candidate.model("twin", new StyledModel("sum")))
                .scorer(Scorers.sameResult())
                .scorer(sample -> { throw new IllegalStateException("scorer bug"); })
                .run();

        CaseResult r = report.resultsFor("twin").get(0);
        assertThat(r.scores()).extracting(Score::scorer).containsExactly("same-result", "scorer[1]");
        assertThat(r.scores().get(0).pass()).isTrue();
        assertThat(r.scores().get(1).detail()).contains("scorer bug");
        assertThat(report.results()).hasSize(3);
    }

    /** Failed and unrecorded-tool cases link to their branch execution, which the log kept. */
    @Test
    void failedCasesLinkToTheirBranchExecution() {
        EvalReport report = eval().candidate(Candidate.task("v3", new TriageTaskV3(lookup)))
                .candidate(Candidate.model("broken", request -> { throw new IllegalStateException("provider down"); }))
                .run();
        for (CaseResult r : report.results()) {
            assertThat(r.branchId()).isNotNull();
            assertThat(runtime.inspect(r.branchId()).status()).isEqualTo(Status.FAILED);
        }
    }

    /** Input, a lookup, then the model: the parent shape for the early-fork case. */
    static final class LookupThenModel implements Task<String> {
        final LookupTool lookup;
        final boolean traceFirst;
        LookupThenModel(LookupTool lookup, boolean traceFirst) { this.lookup = lookup; this.traceFirst = traceFirst; }
        @Override public String execute(Context ctx) {
            if (traceFirst) ctx.effect("trace", () -> "on");   // a new first step: diverges at the very start
            String ticket = ctx.effect("ticket", () -> "t-1");
            String details = ctx.call(lookup, ticket);
            return ctx.model().complete(CompletionRequest.of(Prompt.builder().user(details).build())).message();
        }
    }

    /**
     * A candidate that diverges before the planned branch point forks there. The parent's tool calls
     * from that point on were never used, so they must still answer: not a false UNRECORDED_TOOL, and
     * never a re-run of the side effect.
     */
    @Test
    void aCandidateThatForksEarlyIsStillAnsweredFromTheRecording() {
        var recordedRun = runtime.execute(new LookupThenModel(lookup, false), ExecutionOptions.none());
        recordedRun.result();
        int lookupsBefore = lookup.calls.get();

        EvalReport report = Eval.on(runtime).cases(recordedRun.id()).task(new LookupThenModel(lookup, false))
                .candidate(Candidate.task("traced", new LookupThenModel(lookup, true)))
                .scorer(Scorers.sameResult()).run();

        assertThat(report.summary("traced").passedAllScorers()).isEqualTo(1);
        assertThat(lookup.calls).hasValue(lookupsBefore);
    }

    /** An agent-style task that catches tool errors cannot turn an unanswerable call into a completion. */
    @Test
    void anUnrecordedToolCallTheTaskSwallowsIsStillReported() {
        int lookupsBefore = lookup.calls.get();
        Task<String> forgiving = ctx -> {
            ctx.effect("ticket", () -> "unused");
            ctx.effect("received-at", () -> "unused");
            String details;
            try {
                details = ctx.call(lookup, "A-DIFFERENT-INPUT");
            } catch (RuntimeException e) {
                details = "lookup unavailable";
            }
            return ctx.model().complete(CompletionRequest.of(Prompt.builder().user(details).build())).message();
        };
        EvalReport report = eval().candidate(Candidate.task("forgiving", forgiving)).run();

        assertThat(report.summary("forgiving").outcomes()).containsEntry(CaseResult.Outcome.UNRECORDED_TOOL, 3);
        assertThat(lookup.calls).hasValue(lookupsBefore);
    }
}
