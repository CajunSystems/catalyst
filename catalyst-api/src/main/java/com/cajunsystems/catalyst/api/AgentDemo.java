package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.Tool;
import com.cajunsystems.catalyst.agents.Agent;
import com.cajunsystems.catalyst.eval.Candidate;
import com.cajunsystems.catalyst.eval.Eval;
import com.cajunsystems.catalyst.eval.EvalReport;
import com.cajunsystems.catalyst.eval.Scorers;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Message;
import com.cajunsystems.catalyst.model.Model;
import com.cajunsystems.catalyst.model.Role;
import com.cajunsystems.catalyst.model.ToolCall;
import com.cajunsystems.catalyst.model.Usage;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The agent exit demo ({@code Demo agent}), shared with {@code AgentAcceptanceTest}. A tool-calling
 * triage agent (look the ticket up, page on-call if it is P1, answer) runs on a durable log, and gets
 * everything an execution gets. Its node dies after the lookup, and a fresh runtime resumes the
 * conversation without repeating the model turn or the lookup. A strict replay of the whole
 * conversation makes no calls. An evaluation of a candidate model over the run re-runs no tool.
 */
final class AgentDemo {

    private AgentDemo() {}

    record TicketQuery(String id) {}
    record Ticket(String id, String summary, int priority) {}
    enum Team { FACILITIES, NETWORK }

    static final class TicketLookup implements Tool<TicketQuery, Ticket> {
        final AtomicInteger calls = new AtomicInteger();
        Runnable after = () -> { };
        @Override public String name() { return "ticket-lookup"; }
        @Override public Class<TicketQuery> inputType() { return TicketQuery.class; }
        @Override public Ticket apply(TicketQuery q) {
            calls.incrementAndGet();
            Ticket t = new Ticket(q.id(), "printer offline on floor 3", 1);
            after.run();
            return t;
        }
    }

    static final class Pager implements Tool<Team, String> {
        final AtomicInteger calls = new AtomicInteger();
        @Override public String name() { return "page"; }
        @Override public Class<Team> inputType() { return Team.class; }
        @Override public String apply(Team team) { calls.incrementAndGet(); return "paged " + team; }
    }

    /** A deterministic stand-in for a tool-calling model: lookup, page if P1, answer. */
    static final class PolicyModel implements Model {
        final AtomicInteger calls = new AtomicInteger();
        final String voice;
        PolicyModel(String voice) { this.voice = voice; }
        @Override public Completion complete(CompletionRequest request) {
            calls.incrementAndGet();
            List<Message> results = request.prompt().messages().stream().filter(m -> m.role() == Role.TOOL).toList();
            if (results.isEmpty()) return call("ticket-lookup", "{\"id\":\"TCK-1042\"}");
            Message latest = results.get(results.size() - 1);
            if (results.size() == 1 && latest.content().contains("\"priority\":1")) return call("page", "{\"value\":\"FACILITIES\"}");
            return new Completion(voice + ": " + latest.content() + " for TCK-1042", List.of(), new Usage(40, 12), "stop");
        }
        private static Completion call(String tool, String args) {
            return new Completion("", List.of(new ToolCall(null, tool, args)), new Usage(40, 8), "tool_calls");
        }
    }

    /** The agent the current process uses: a named task type, so resume-by-id can rebuild it. */
    static final AtomicReference<Agent> AGENT = new AtomicReference<>();

    static final class TriageAgentTask implements Task<String> {
        private final String ticket;
        TriageAgentTask(String ticket) { this.ticket = ticket; }
        TriageAgentTask() { this(null); } // resume and replay substitute the recorded input
        @Override public String execute(Context ctx) {
            return AGENT.get().run(ctx, ctx.effect("ticket", () -> ticket)).answer();
        }
    }

    record Outcome(ExecutionId id, String answer, int turnsBeforeCrash, int modelCallsOnResume,
                   int lookupsTotal, int pagesTotal, int callsDuringReplay, EvalReport eval, int toolRunsDuringEval) {}

    static Outcome run(Path dir) {
        TicketLookup lookup = new TicketLookup();
        Pager pager = new Pager();
        AGENT.set(Agent.builder()
                .instructions("You triage support tickets. Look the ticket up; page on-call for P1.")
                .tool(lookup, "Look up a ticket by its id")
                .tool(pager, "Page a team's on-call engineer")
                .maxSteps(6)
                .build());

        // Node A: the lookup takes the node down. The next boundary (model turn 2) unwinds before
        // recording anything, so the execution is left RUNNING, as a crash would leave it.
        PolicyModel modelA = new PolicyModel("triaged");
        CatalystRuntime nodeA = Catalyst.builder().log(GumboEventLog.at(dir)).model(modelA)
                .drainTimeout(Duration.ofSeconds(5)).build();
        lookup.after = () -> {
            new Thread(nodeA::close).start();
            while (!nodeA.isShuttingDown()) Thread.onSpinWait();
        };
        var crashed = nodeA.execute(new TriageAgentTask("TCK-1042"), ExecutionOptions.withKey("TCK-1042"));
        try {
            crashed.result();
            throw new AssertionError("node A was supposed to stop mid-conversation");
        } catch (RuntimeException expected) {
            // abandoned at shutdown: left for the next start
        }
        nodeA.close(); // waits for the log to be released
        lookup.after = () -> { };
        ExecutionId id = crashed.id();
        int turnsBeforeCrash = modelA.calls.get();

        PolicyModel modelB = new PolicyModel("triaged");
        try (CatalystRuntime nodeB = Catalyst.builder().log(GumboEventLog.at(dir)).model(modelB)
                .task(TriageAgentTask.class, TriageAgentTask::new).build()) {
            String answer = String.valueOf(nodeB.resume(id).result());
            int resumeCalls = modelB.calls.get();
            int lookups = lookup.calls.get();
            int pages = pager.calls.get();

            if (nodeB.replay(id, new TriageAgentTask()).status() != Status.COMPLETED) {
                throw new AssertionError("the agent run did not replay exactly");
            }
            int replayCalls = (modelB.calls.get() - resumeCalls) + (lookup.calls.get() - lookups) + (pager.calls.get() - pages);

            EvalReport eval = Eval.on(nodeB).cases(id).task(new TriageAgentTask())
                    .candidate(Candidate.model("same-policy", new PolicyModel("triaged")))
                    .candidate(Candidate.model("terser-policy", new PolicyModel("ok")))
                    .scorer(Scorers.sameResult()).run();
            int toolRunsDuringEval = (lookup.calls.get() - lookups) + (pager.calls.get() - pages);
            return new Outcome(id, answer, turnsBeforeCrash, resumeCalls, lookup.calls.get(), pager.calls.get(),
                    replayCalls, eval, toolRunsDuringEval);
        }
    }

    static void main(Path dir) {
        Outcome o = run(dir);
        System.out.println("[agent] execution " + o.id().value());
        System.out.println("[agent] node A made " + o.turnsBeforeCrash() + " model turn and the lookup, then died mid-conversation");
        System.out.println("[agent] resumed on a fresh runtime: \"" + o.answer() + "\"");
        System.out.println("[agent] model turns on resume: " + o.modelCallsOnResume() + " (turns 2-3; turn 1 was substituted)");
        System.out.println("[agent] tool executions in total: lookup " + o.lookupsTotal() + ", page " + o.pagesTotal() + " (none repeated)");
        System.out.println("[agent] calls during a strict replay of the whole conversation: " + o.callsDuringReplay());
        System.out.print(o.eval().pretty());
        System.out.println("[agent] tool executions during the evaluation: " + o.toolRunsDuringEval());
        if (o.modelCallsOnResume() != 2 || o.lookupsTotal() != 1 || o.pagesTotal() != 1
                || o.callsDuringReplay() != 0 || o.toolRunsDuringEval() != 0
                || o.eval().summary("same-policy").passedAllScorers() != 1) {
            throw new AssertionError("FAIL: the agent did not behave like an ordinary execution");
        }
        System.out.println("[agent] Agent exit criterion holds: a tool-calling agent resumed mid-conversation without"
                + " repeating a call, replayed with zero calls, and was evaluated without re-running a tool.");
    }
}
