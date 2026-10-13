package com.cajunsystems.catalyst.agents;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.Tool;
import com.cajunsystems.catalyst.engine.UnrecordedToolCallException;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Message;
import com.cajunsystems.catalyst.model.Model;
import com.cajunsystems.catalyst.model.Role;
import com.cajunsystems.catalyst.model.ToolCall;
import com.cajunsystems.catalyst.model.Usage;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.cajunsystems.catalyst.runtime.EventLogs;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The agent loop over an in-memory runtime. The model is a deterministic "policy": look the ticket up,
 * page on-call if it is priority 1, then answer. The point of the module is that an agent is ordinary
 * task code, so the tests check the properties every execution has, here for a whole conversation:
 * exact replay, resume mid-loop without repeats, and failures that replay identically.
 */
class AgentTest {

    record TicketQuery(String id) {}
    record Ticket(String id, String summary, int priority) {}
    enum Team { FACILITIES, NETWORK }

    static final class LookupTool implements Tool<TicketQuery, Ticket> {
        final AtomicInteger calls = new AtomicInteger();
        @Override public String name() { return "lookup"; }
        @Override public Class<TicketQuery> inputType() { return TicketQuery.class; }
        @Override public Ticket apply(TicketQuery q) {
            calls.incrementAndGet();
            if (q.id().equals("DOWN")) throw new IllegalStateException("ticketing system unavailable");
            return new Ticket(q.id(), "printer offline", q.id().equals("T-1") ? 1 : 3);
        }
    }

    /** A scalar input: offered to the model as {"value": ...}. */
    static final class PagerTool implements Tool<Team, String> {
        final AtomicInteger calls = new AtomicInteger();
        @Override public String name() { return "page"; }
        @Override public Class<Team> inputType() { return Team.class; }
        @Override public String apply(Team team) { calls.incrementAndGet(); return "paged " + team; }
    }

    /** Decides from the conversation alone, like a real model: lookup → (page if P1) → answer. */
    static final class PolicyModel implements Model {
        final AtomicInteger calls = new AtomicInteger();
        final AtomicReference<CompletionRequest> last = new AtomicReference<>();
        String ticket = "T-1";

        @Override public Completion complete(CompletionRequest request) {
            calls.incrementAndGet();
            last.set(request);
            List<Message> msgs = request.prompt().messages();
            List<Message> results = msgs.stream().filter(m -> m.role() == Role.TOOL).toList();
            if (results.isEmpty()) return call("lookup", "{\"id\":\"" + ticket + "\"}");
            Message latest = results.get(results.size() - 1);
            if (results.size() == 1 && latest.content().contains("\"priority\":1")) {
                return call("page", "{\"value\":\"FACILITIES\"}");
            }
            return new Completion("done: " + latest.content(), List.of(), new Usage(10, 5), "stop");
        }

        private static Completion call(String tool, String args) {
            return new Completion("", List.of(new ToolCall(null, tool, args)), new Usage(10, 5), "tool_calls");
        }
    }

    final LookupTool lookup = new LookupTool();
    final PagerTool pager = new PagerTool();
    final Agent agent = Agent.builder()
            .instructions("You triage support tickets.")
            .tool(lookup, "Look up a ticket by its id")
            .tool(pager, "Page a team's on-call engineer")
            .maxSteps(5)
            .build();

    /** The documented shape: input through a recorded effect, so resume and eval see the same input. */
    final class TriageTask implements Task<String> {
        final String ticket;
        TriageTask(String ticket) { this.ticket = ticket; }
        @Override public String execute(Context ctx) {
            return agent.run(ctx, ctx.effect("ticket", () -> ticket)).answer();
        }
    }

    @Test
    void anAgentUsesToolsThenAnswers() {
        PolicyModel model = new PolicyModel();
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory()).model(model).build()) {
            AtomicReference<AgentResult> result = new AtomicReference<>();
            String answer = runtime.execute(ctx -> {
                result.set(agent.run(ctx, "printer on floor 3 is offline"));
                return result.get().answer();
            }, ExecutionOptions.none()).result();

            assertThat(answer).isEqualTo("done: paged FACILITIES");
            assertThat(result.get()).isEqualTo(new AgentResult("done: paged FACILITIES", AgentResult.StopReason.ANSWERED, 3, 2));
            assertThat(lookup.calls).hasValue(1);
            assertThat(pager.calls).hasValue(1);

            // The last request carried the whole conversation, with tool calls and their results linked by id.
            List<Message> msgs = model.last.get().prompt().messages();
            assertThat(msgs).extracting(Message::role).containsExactly(
                    Role.SYSTEM, Role.USER, Role.ASSISTANT, Role.TOOL, Role.ASSISTANT, Role.TOOL);
            assertThat(msgs.get(2).toolCalls()).singleElement().satisfies(c -> assertThat(c.id()).isEqualTo("call-1-0"));
            assertThat(msgs.get(3).toolCallId()).isEqualTo("call-1-0");
            assertThat(msgs.get(3).content()).isEqualTo("{\"id\":\"T-1\",\"summary\":\"printer offline\",\"priority\":1}");
            assertThat(model.last.get().tools()).extracting(t -> t.name()).containsExactly("lookup", "page");
        }
    }

    /** The whole conversation replays with zero model calls and zero tool executions. */
    @Test
    void aStrictReplayOfAnAgentRunMakesNoCalls() {
        PolicyModel model = new PolicyModel();
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory()).model(model).build()) {
            var run = runtime.execute(new TriageTask("T-1"), ExecutionOptions.none());
            run.result();
            int modelCalls = model.calls.get();

            assertThat(runtime.replay(run.id(), new TriageTask(null)).status()).isEqualTo(Status.COMPLETED);
            assertThat(model.calls).hasValue(modelCalls);
            assertThat(lookup.calls).hasValue(1);
            assertThat(pager.calls).hasValue(1);
        }
    }

    /** One task type for both nodes, so the survivor's registry resolves the recorded type; the agent is swapped per node. */
    static final AtomicReference<Agent> CURRENT = new AtomicReference<>();

    static final class SwappableTask implements Task<String> {
        @Override public String execute(Context ctx) {
            return CURRENT.get().run(ctx, ctx.effect("ticket", () -> "input")).answer();
        }
    }

    /**
     * A node that dies mid-conversation, after the lookup and before the second model turn, is
     * resumed elsewhere. The first turn and the lookup are substituted, and only turns 2 and 3 and the
     * page run.
     */
    @Test
    void aCrashMidLoopResumesWithoutRepeatingAModelOrToolCall() {
        EventLog log = EventLogs.inMemory();
        PolicyModel first = new PolicyModel();
        CatalystRuntime dying = CatalystRuntime.builder().log(log).model(first)
                .drainTimeout(java.time.Duration.ofSeconds(5)).build();
        // The lookup shuts its own node down. The next boundary (model turn 2) then unwinds without
        // recording anything, which leaves the execution RUNNING, just as a crash would.
        Tool<TicketQuery, Ticket> lookupThenDie = new Tool<>() {
            @Override public String name() { return "lookup"; }
            @Override public Class<TicketQuery> inputType() { return TicketQuery.class; }
            @Override public Ticket apply(TicketQuery q) {
                Ticket t = lookup.apply(q);
                new Thread(dying::close).start();
                while (!dying.isShuttingDown()) Thread.onSpinWait();
                return t;
            }
        };
        CURRENT.set(Agent.builder().instructions("You triage support tickets.")
                .tool(lookupThenDie, "Look up a ticket by its id").tool(pager, "Page a team's on-call engineer").maxSteps(5).build());
        var crashed = dying.execute(new SwappableTask(), ExecutionOptions.none());
        assertThatThrownBy(crashed::result).hasMessageContaining("shut down");
        ExecutionId id = crashed.id();
        assertThat(first.calls).hasValue(1);
        assertThat(lookup.calls).hasValue(1);

        CURRENT.set(agent);
        PolicyModel survivorModel = new PolicyModel();
        try (CatalystRuntime survivor = CatalystRuntime.builder().log(log).model(survivorModel)
                .task(SwappableTask.class, SwappableTask::new).build()) {
            assertThat(survivor.resume(id).result()).isEqualTo("done: paged FACILITIES");
        }
        assertThat(survivorModel.calls).as("turns 2 and 3 only").hasValue(2);
        assertThat(lookup.calls).as("the lookup is not repeated").hasValue(1);
        assertThat(pager.calls).hasValue(1);
    }

    /**
     * The node dies right after a tool failed, so the recorded failure is the last event. That is a
     * fact the agent already handled, not a doubt: the resumed run hands the model the same error and
     * carries on. Before, the trailing failure was routed through InDoubtPolicy, and the default FAIL
     * ended the agent FAILED.
     */
    @Test
    void aCrashRightAfterAToolFailureResumesAndTheAgentCarriesOn() {
        EventLog log = EventLogs.inMemory();
        PolicyModel first = new PolicyModel();
        first.ticket = "DOWN";
        CatalystRuntime dying = CatalystRuntime.builder().log(log).model(first)
                .drainTimeout(java.time.Duration.ofSeconds(5)).build();
        Tool<TicketQuery, Ticket> failThenDie = new Tool<>() {
            @Override public String name() { return "lookup"; }
            @Override public Class<TicketQuery> inputType() { return TicketQuery.class; }
            @Override public Ticket apply(TicketQuery q) {
                new Thread(dying::close).start();
                while (!dying.isShuttingDown()) Thread.onSpinWait();
                return lookup.apply(q); // throws: the failure is recorded, then the next turn abandons
            }
        };
        // The same agent definition as the survivor's (instructions, tool names, schemas), as two nodes
        // running one service would have; only the lookup's behaviour differs.
        CURRENT.set(Agent.builder().instructions("You triage support tickets.")
                .tool(failThenDie, "Look up a ticket by its id").tool(pager, "Page a team's on-call engineer").maxSteps(5).build());
        var crashed = dying.execute(new SwappableTask(), ExecutionOptions.none());
        assertThatThrownBy(crashed::result).hasMessageContaining("shut down");
        List<com.cajunsystems.catalyst.events.SequencedEvent> events = log.read(crashed.id());
        assertThat(events.get(events.size() - 1).event())
                .as("the recorded failure is the last event").isInstanceOfSatisfying(
                        com.cajunsystems.catalyst.events.CatalystEvent.ToolCompleted.class, tc -> assertThat(tc.error()).isNotNull());

        CURRENT.set(agent);
        PolicyModel survivorModel = new PolicyModel();
        survivorModel.ticket = "DOWN";
        try (CatalystRuntime survivor = CatalystRuntime.builder().log(log).model(survivorModel)
                .task(SwappableTask.class, SwappableTask::new).build()) {
            assertThat(survivor.resume(crashed.id()).result())
                    .isEqualTo("done: error: java.lang.IllegalStateException: ticketing system unavailable");
        }
        assertThat(lookup.calls).as("the failed lookup is not re-run").hasValue(1);
    }

    /**
     * A node dies while a lookup is in flight (its request recorded, no completion), and the survivor
     * runs under InDoubtPolicy.RETRY. The retried lookup fails again. That failure must be recorded,
     * completing the request, so the agent can hand it to the model and the run still replays
     * exactly. Unrecorded, the next model turn landed after a request that never completed: replay
     * diverged, and every resume found the lookup in doubt again.
     */
    @Test
    void aFailedInDoubtRetryIsRecordedSoTheAgentCarriesOnAndReplays() {
        // A real run gives the exact recorded prefix; a crash mid-lookup keeps it only up to the request.
        PolicyModel model = new PolicyModel();
        model.ticket = "DOWN";
        CURRENT.set(agent);
        EventLog full = EventLogs.inMemory();
        ExecutionId id;
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(full).model(model).build()) {
            var run = runtime.execute(new SwappableTask(), ExecutionOptions.none());
            run.result();
            id = run.id();
        }
        EventLog crashed = EventLogs.inMemory();
        for (var se : full.read(id)) {
            crashed.append(id, se.event());
            if (se.event() instanceof com.cajunsystems.catalyst.events.CatalystEvent.ToolRequested) break;
        }
        int lookupsBefore = lookup.calls.get();

        PolicyModel survivorModel = new PolicyModel();
        survivorModel.ticket = "DOWN";
        try (CatalystRuntime survivor = CatalystRuntime.builder().log(crashed).model(survivorModel)
                .inDoubtPolicy(com.cajunsystems.catalyst.engine.InDoubtPolicy.RETRY)
                .task(SwappableTask.class, SwappableTask::new).build()) {
            assertThat(survivor.resume(id).result())
                    .isEqualTo("done: error: java.lang.IllegalStateException: ticketing system unavailable");
            assertThat(lookup.calls).as("the in-doubt lookup was retried once").hasValue(lookupsBefore + 1);
            assertThat(crashed.read(id)).as("the retried failure completes the request")
                    .anySatisfy(se -> assertThat(se.event()).isInstanceOfSatisfying(
                            com.cajunsystems.catalyst.events.CatalystEvent.ToolCompleted.class,
                            tc -> assertThat(tc.error()).contains("ticketing system unavailable")));
            assertThat(survivor.replay(id, new SwappableTask()).status()).as("and the run replays exactly").isEqualTo(Status.COMPLETED);
        }
    }

    /**
     * A failing tool goes back to the model as an error it can act on, and the conversation replays
     * exactly: the replayed failure carries the recorded text, so the next prompt is byte-identical.
     */
    @Test
    void aToolFailureIsToldToTheModelAndReplaysIdentically() {
        PolicyModel model = new PolicyModel();
        model.ticket = "DOWN";
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory()).model(model).build()) {
            var run = runtime.execute(new TriageTask("DOWN"), ExecutionOptions.none());
            assertThat(run.result()).isEqualTo("done: error: java.lang.IllegalStateException: ticketing system unavailable");

            assertThat(runtime.replay(run.id(), new TriageTask(null)).status())
                    .as("no divergence at the prompt after the failure").isEqualTo(Status.COMPLETED);
        }
    }

    @Test
    void unknownToolsAndBadArgumentsAreToldToTheModel() {
        Model confused = new Model() {
            int turn;
            @Override public Completion complete(CompletionRequest request) {
                turn++;
                if (turn == 1) return new Completion("", List.of(new ToolCall("a", "nope", "{}")), Usage.ZERO, "tool_calls");
                if (turn == 2) return new Completion("", List.of(new ToolCall("b", "lookup", "not json")), Usage.ZERO, "tool_calls");
                List<Message> m = request.prompt().messages();
                return Completion.ofText(m.get(m.size() - 3).content() + " | " + m.get(m.size() - 1).content());
            }
        };
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory()).model(confused).build()) {
            String answer = runtime.execute(ctx -> agent.run(ctx, "go").answer(), ExecutionOptions.none()).result();
            assertThat(answer).startsWith("error: no tool named 'nope'; available: lookup, page | error: arguments do not fit lookup");
            assertThat(lookup.calls).as("refused before running").hasValue(0);
        }
    }

    /** The schema marks every field required; a call missing one is refused, not run on null. */
    @Test
    void aCallMissingARequiredArgumentIsRefused() {
        Model sloppy = new Model() {
            int turn;
            @Override public Completion complete(CompletionRequest request) {
                if (++turn == 1) return new Completion("", List.of(new ToolCall("a", "lookup", "{}")), Usage.ZERO, "tool_calls");
                List<Message> m = request.prompt().messages();
                return Completion.ofText(m.get(m.size() - 1).content());
            }
        };
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory()).model(sloppy).build()) {
            String answer = runtime.execute(ctx -> agent.run(ctx, "go").answer(), ExecutionOptions.none()).result();
            assertThat(answer).isEqualTo("error: arguments do not fit lookup: missing \"id\"");
            assertThat(lookup.calls).hasValue(0);
        }
    }

    @Test
    void theStepBudgetEndsALoopThatNeverAnswers() {
        Model insistent = request -> new Completion("thinking", List.of(new ToolCall(null, "page", "{\"value\":\"NETWORK\"}")), Usage.ZERO, "tool_calls");
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory()).model(insistent).build()) {
            AgentResult result = runtime.execute(ctx -> Agent.builder().tool(pager, "page").maxSteps(3).build().run(ctx, "go"),
                    ExecutionOptions.none()).result();
            assertThat(result).isEqualTo(new AgentResult("thinking", AgentResult.StopReason.STEP_LIMIT, 3, 3));
        }
    }

    /**
     * A failure that is not the call's recorded result (here the log refusing to record the tool's
     * completion) must not reach the model as a tool error. The outcome is not in the log, and the
     * next model turn would be recorded on top of that gap.
     */
    @Test
    void aFailedLogWriteIsNotHandedToTheModel() {
        EventLog inner = EventLogs.inMemory();
        EventLog refusing = new EventLog() {
            private void check(com.cajunsystems.catalyst.events.CatalystEvent e) {
                if (e instanceof com.cajunsystems.catalyst.events.CatalystEvent.ToolCompleted) {
                    throw new IllegalStateException("disk full");
                }
            }
            @Override public long append(ExecutionId id, com.cajunsystems.catalyst.events.CatalystEvent e) { check(e); return inner.append(id, e); }
            @Override public long append(ExecutionId id, com.cajunsystems.catalyst.events.CatalystEvent e, long expected) { check(e); return inner.append(id, e, expected); }
            @Override public boolean supportsConditionalAppend() { return inner.supportsConditionalAppend(); }
            @Override public List<com.cajunsystems.catalyst.events.SequencedEvent> read(ExecutionId id) { return inner.read(id); }
            @Override public long latestSeq(ExecutionId id) { return inner.latestSeq(id); }
            @Override public java.util.Optional<ExecutionId> findByKey(String k) { return inner.findByKey(k); }
            @Override public void putKey(String k, ExecutionId id) { inner.putKey(k, id); }
            @Override public void close() { }
        };
        PolicyModel model = new PolicyModel();
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(refusing).model(model).build()) {
            var run = runtime.execute(ctx -> agent.run(ctx, "go").answer(), ExecutionOptions.none());
            assertThatThrownBy(run::result).hasMessageContaining("disk full");
            assertThat(model.calls).as("no turn after the unrecorded call").hasValue(1);
        }
    }

    /** Engine signals unwind the run; they are never handed to the model as a tool error. */
    @Test
    void engineSignalsAreNotSwallowed() {
        Tool<TicketQuery, Ticket> refusing = new Tool<>() {
            @Override public String name() { return "lookup"; }
            @Override public Class<TicketQuery> inputType() { return TicketQuery.class; }
            @Override public Ticket apply(TicketQuery q) { throw new UnrecordedToolCallException("lookup", "h"); }
        };
        PolicyModel model = new PolicyModel();
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory()).model(model).build()) {
            var run = runtime.execute(ctx -> Agent.builder().tool(refusing, "x").build().run(ctx, "go").answer(), ExecutionOptions.none());
            assertThatThrownBy(run::result).isInstanceOf(UnrecordedToolCallException.class);
            assertThat(model.calls).as("the model never saw it").hasValue(1);
        }
    }

    @Test
    void schemasAreDerivedFromInputTypes() {
        assertThat(agent.toolSpecs().get(0).inputSchema())
                .isEqualTo("{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"]}");
        assertThat(agent.toolSpecs().get(1).inputSchema()).isEqualTo(
                "{\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"string\",\"enum\":[\"FACILITIES\",\"NETWORK\"]}},\"required\":[\"value\"]}");
        assertThat(agent.toolSpecs().get(0).description()).isEqualTo("Look up a ticket by its id");
    }

    record Nested(Ticket ticket) {}

    @Test
    void anUnsupportedInputTypeOrADuplicateNameIsRefusedAtBuild() {
        Tool<Nested, String> nested = new Tool<>() {
            @Override public String name() { return "nested"; }
            @Override public Class<Nested> inputType() { return Nested.class; }
            @Override public String apply(Nested n) { return ""; }
        };
        assertThatThrownBy(() -> Agent.builder().tool(nested, "x")).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ticket is a").hasMessageContaining("take a record of strings, numbers, booleans and enums");
        assertThatThrownBy(() -> Agent.builder().tool(lookup, "a").tool(new LookupTool(), "b"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("named 'lookup'");
    }

    /** A tool that takes the model's arguments as JSON with its own schema (the MCP shape). */
    static final class EchoJson implements Tool<com.fasterxml.jackson.databind.JsonNode, String> {
        @Override public String name() { return "echo"; }
        @Override public Class<com.fasterxml.jackson.databind.JsonNode> inputType() { return com.fasterxml.jackson.databind.JsonNode.class; }
        @Override public String apply(com.fasterxml.jackson.databind.JsonNode args) { return "got " + args; }
    }

    @Test
    void aToolWithItsOwnSchemaGetsTheArgumentsAsGivenUnderACanonicalSchema() {
        Agent a = Agent.builder().tool(new EchoJson(), "Echo",
                "{\"required\":[\"q\"],\"type\":\"object\",\"properties\":{\"q\":{\"type\":\"string\"}}}").build();
        Agent b = Agent.builder().tool(new EchoJson(), "Echo",
                "{\"type\":\"object\",\"properties\":{\"q\":{\"type\":\"string\"}},\"required\":[\"q\"]}").build();
        assertThat(a.toolSpecs().get(0).inputSchema()).as("key order does not change the request hash")
                .isEqualTo(b.toolSpecs().get(0).inputSchema())
                .isEqualTo("{\"properties\":{\"q\":{\"type\":\"string\"}},\"required\":[\"q\"],\"type\":\"object\"}");

        Model calls = new Model() {
            int turn;
            @Override public Completion complete(CompletionRequest request) {
                turn++;
                if (turn == 1) return new Completion("", List.of(new ToolCall("x", "echo", "{\"q\":\"hi\",\"extra\":1}")), Usage.ZERO, "tool_calls");
                if (turn == 2) return new Completion("", List.of(new ToolCall("y", "echo", "[1,2]")), Usage.ZERO, "tool_calls");
                List<Message> m = request.prompt().messages();
                return Completion.ofText(m.get(m.size() - 3).content() + " | " + m.get(m.size() - 1).content());
            }
        };
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory()).model(calls).build()) {
            assertThat(runtime.execute(ctx -> a.run(ctx, "go").answer(), ExecutionOptions.none()).result())
                    .isEqualTo("got {\"q\":\"hi\",\"extra\":1} | error: arguments do not fit echo: arguments must be a JSON object");
        }
    }

    @Test
    void anExplicitSchemaIsOnlyForToolsThatTakeJson() {
        assertThatThrownBy(() -> Agent.builder().tool((Tool) lookup, "x", "{\"type\":\"object\"}"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("inputType JsonNode");
        assertThatThrownBy(() -> Agent.builder().tool(new EchoJson(), "x", "[]"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not a JSON object schema");
    }
}
