package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.Tool;
import com.cajunsystems.catalyst.engine.CostModel;
import com.cajunsystems.catalyst.eval.Candidate;
import com.cajunsystems.catalyst.eval.Eval;
import com.cajunsystems.catalyst.eval.EvalReport;
import com.cajunsystems.catalyst.eval.Scorers;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Model;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.model.Usage;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The eval harness exit demo ({@code Demo eval}), shared with {@code EvalAcceptanceTest}. It records
 * three support-ticket triage runs on a durable log. Then it evaluates three candidates against them:
 * an identical model (all pass), a cheaper model that answers differently (fails same-result, and
 * the report shows how), and a prompt rewrite. No tool runs during the evaluation, and no model but
 * the candidates' is called.
 */
final class EvalDemo {

    private EvalDemo() {}

    /** The ticketing system: a side effect an evaluation must never repeat. */
    static final class TicketLookup implements Tool<String, String> {
        final AtomicInteger calls = new AtomicInteger();
        @Override public String name() { return "ticket-lookup"; }
        @Override public Class<String> inputType() { return String.class; }
        @Override public String apply(String ticket) {
            calls.incrementAndGet();
            return ticket + ": printer offline, floor " + (ticket.length() % 5);
        }
    }

    /** A deterministic stand-in for a provider: the answer is a function of the prompt, priced by length. */
    static final class StubModel implements Model {
        final String voice;
        final AtomicInteger calls = new AtomicInteger();
        StubModel(String voice) { this.voice = voice; }
        @Override public Completion complete(CompletionRequest request) {
            calls.incrementAndGet();
            String user = request.prompt().messages().get(request.prompt().messages().size() - 1).content();
            String text = voice.equals("concise") ? "P1: " + user.substring(user.indexOf(':') + 2) : "Summary — " + user;
            return new Completion(text, List.of(), new Usage(user.length(), text.length()), "stop");
        }
    }

    /** The production task. Its input arrives through a recorded effect, since task vars are not persisted. */
    static class TriageTask implements Task<String> {
        final TicketLookup lookup;
        final List<String> inbox;
        TriageTask(TicketLookup lookup, List<String> inbox) { this.lookup = lookup; this.inbox = inbox; }

        @Override public String execute(Context ctx) {
            String ticket = ctx.effect("ticket", () -> inbox.remove(0));
            String receivedAt = ctx.effect("received-at", () -> Instant.now().toString());
            String details = ctx.call(lookup, ticket);
            String triage = ctx.model().complete(CompletionRequest.of(Prompt.builder()
                    .system("You triage support tickets.").user(instruction() + details).build())).message();
            return triage + " (received " + receivedAt + ")";
        }

        String instruction() { return "Triage: "; }
    }

    /** A prompt rewrite under evaluation. */
    static final class TriageTaskV2 extends TriageTask {
        TriageTaskV2(TicketLookup lookup) { super(lookup, new ArrayList<>()); }
        @Override String instruction() { return "Classify urgency: "; }
    }

    /** What the demo observed, for the acceptance test to assert on. */
    record Outcome(EvalReport report, int lookupsDuringEval, int productionCallsDuringEval) {}

    static Outcome run(Path dir) {
        TicketLookup lookup = new TicketLookup();
        StubModel production = new StubModel("full");
        try (CatalystRuntime runtime = Catalyst.builder().log(GumboEventLog.at(dir)).model(production)
                .costModel(CostModel.perMillionTokens(3, 15)).build()) {
            TriageTask task = new TriageTask(lookup, new ArrayList<>(List.of("TCK-1042", "TCK-77", "TCK-31337")));
            List<ExecutionId> recorded = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                var run = runtime.execute(task, ExecutionOptions.none());
                run.result();
                recorded.add(run.id());
            }
            int lookups = lookup.calls.get();
            int productionCalls = production.calls.get();

            EvalReport report = Eval.on(runtime).cases(recorded).task(new TriageTask(lookup, new ArrayList<>()))
                    .candidate(Candidate.model("same-model", new StubModel("full")))
                    .candidate(Candidate.model("concise-model", new StubModel("concise")))
                    // A prompt rewrite brings its own model instance here only so the demo can show the
                    // recorded one untouched; Candidate.task(...) alone runs on the runtime's default model.
                    .candidate(Candidate.of("prompt-v2", new StubModel("full"), new TriageTaskV2(lookup)))
                    .scorer(Scorers.sameResult())
                    .scorer(Scorers.tokensAtMost(1.0))
                    .run();
            return new Outcome(report, lookup.calls.get() - lookups, production.calls.get() - productionCalls);
        }
    }

    static void main(Path dir) {
        Outcome outcome = run(dir);
        System.out.print(outcome.report().pretty());
        System.out.println("[eval] tool executions during the evaluation: " + outcome.lookupsDuringEval()
                + " (answered from the recording)");
        System.out.println("[eval] calls to the recorded production model during the evaluation: "
                + outcome.productionCallsDuringEval());
        if (outcome.lookupsDuringEval() != 0 || outcome.productionCallsDuringEval() != 0) {
            throw new AssertionError("FAIL: the evaluation reached beyond the candidates");
        }
        System.out.println("[eval] Eval harness exit criterion holds: recorded runs replayed against 3 candidates"
                + " and scored, with zero tool executions and zero calls outside the candidates.");
    }
}
