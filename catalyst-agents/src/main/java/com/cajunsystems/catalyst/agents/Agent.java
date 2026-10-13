package com.cajunsystems.catalyst.agents;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.Tool;
import com.cajunsystems.catalyst.engine.CancellationSignal;
import com.cajunsystems.catalyst.engine.ExecutionAbandonedSignal;
import com.cajunsystems.catalyst.engine.ExecutionPausedSignal;
import com.cajunsystems.catalyst.engine.ExecutionWaitingSignal;
import com.cajunsystems.catalyst.engine.InDoubtException;
import com.cajunsystems.catalyst.engine.NonDeterministicReplayException;
import com.cajunsystems.catalyst.engine.RecordedToolFailureException;
import com.cajunsystems.catalyst.engine.UnrecordedToolCallException;
import com.cajunsystems.catalyst.log.StaleWriterException;
import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Message;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.model.Role;
import com.cajunsystems.catalyst.model.ToolCall;
import com.cajunsystems.catalyst.model.ToolSpec;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A tool-calling agent: the model decides which tools to call, the agent runs them and returns the
 * results, and that repeats until the model answers or the step budget runs out.
 *
 * <pre>{@code
 * Agent triage = Agent.builder()
 *         .instructions("You triage support tickets. Look the ticket up before deciding.")
 *         .tool(ticketLookup, "Look up a ticket by its id")
 *         .tool(pager, "Page the on-call engineer")
 *         .maxSteps(8)
 *         .build();
 *
 * final class TriageTask implements Task<String> {
 *     private final String ticket;
 *     TriageTask(String ticket) { this.ticket = ticket; }
 *     TriageTask() { this(null); }                 // for the registry: resume substitutes the input
 *     public String execute(Context ctx) {
 *         String input = ctx.effect("ticket", () -> ticket);
 *         return triage.run(ctx, input).answer();
 *     }
 * }
 * }</pre>
 *
 * <p><strong>An agent is ordinary task code.</strong> It runs inside a {@code Task} and touches the
 * world only through {@link Context}: every model turn is a recorded model call, and every tool call
 * a recorded tool boundary. So an agent run gets everything an execution gets:
 * <ul>
 *   <li>a crash mid-loop resumes at the step it reached, with no repeated model or tool calls;</li>
 *   <li>a strict replay re-runs the whole conversation with zero external calls;</li>
 *   <li>a branch, an eval or a timeline report works on it like any other execution.</li>
 * </ul>
 * The loop holds no state outside the conversation it rebuilds from those results, which is what
 * keeps it deterministic between boundaries.
 *
 * <p><strong>Tool failures go back to the model as data</strong>, so it can recover: an unknown tool,
 * arguments that do not fit, or the tool throwing. The failure text is the same live and on replay
 * ({@link RecordedToolFailureException}). Engine signals are never swallowed: cancel, shutdown, a
 * wait, a pause, a stale writer, a replay divergence, an in-doubt call, an unrecorded tool in an eval.
 * They unwind the run as they would any task.
 *
 * <p>Pass the task's input through a recorded boundary ({@code ctx.effect}, as above), not task vars,
 * since vars are not persisted. Then a resume by id and an evaluation see the same input.
 */
public final class Agent {

    private final String instructions;
    private final Map<String, AgentTool<?, ?>> tools;
    private final List<ToolSpec> specs;
    private final int maxSteps;

    private Agent(Builder b) {
        this.instructions = b.instructions;
        this.tools = Map.copyOf(b.tools);
        this.specs = b.tools.values().stream().map(AgentTool::spec).toList();
        this.maxSteps = b.maxSteps;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** The tool definitions every request carries, in the order they were added. */
    public List<ToolSpec> toolSpecs() {
        return specs;
    }

    /**
     * Runs the loop for {@code input} inside the task whose context is {@code ctx}. At most
     * {@code maxSteps} model turns; tool calls within a turn run one after another, in the order the
     * model listed them.
     */
    public AgentResult run(Context ctx, String input) {
        Objects.requireNonNull(ctx, "ctx");
        List<Message> conversation = new ArrayList<>();
        if (instructions != null && !instructions.isBlank()) conversation.add(new Message(Role.SYSTEM, instructions));
        conversation.add(new Message(Role.USER, input == null ? "" : input));

        int toolCalls = 0;
        String lastText = "";
        for (int turn = 1; turn <= maxSteps; turn++) {
            Completion completion = ctx.model().complete(
                    CompletionRequest.of(new Prompt(conversation)).withTools(specs));
            lastText = completion.message();
            if (completion.toolCalls().isEmpty()) {
                return new AgentResult(completion.message(), AgentResult.StopReason.ANSWERED, turn, toolCalls);
            }
            List<ToolCall> calls = withIds(completion.toolCalls(), turn);
            conversation.add(Message.assistantToolCalls(completion.message(), calls));
            for (ToolCall call : calls) {
                toolCalls++;
                conversation.add(Message.toolResult(call, perform(ctx, call)));
            }
        }
        return new AgentResult(lastText, AgentResult.StopReason.STEP_LIMIT, maxSteps, toolCalls);
    }

    /** Ids the model left out are filled in deterministically, so a replayed conversation is identical. */
    private static List<ToolCall> withIds(List<ToolCall> calls, int turn) {
        List<ToolCall> out = new ArrayList<>(calls.size());
        for (int i = 0; i < calls.size(); i++) {
            ToolCall c = calls.get(i);
            out.add(c.id() == null || c.id().isBlank() ? new ToolCall("call-" + turn + "-" + i, c.name(), c.argumentsJson()) : c);
        }
        return out;
    }

    /** Runs one call and returns what to tell the model. */
    private String perform(Context ctx, ToolCall call) {
        AgentTool<?, ?> tool = tools.get(call.name());
        if (tool == null) {
            return "error: no tool named '" + call.name() + "'; available: " + String.join(", ", tools.keySet().stream().sorted().toList());
        }
        return performTyped(ctx, tool, call);
    }

    private static <I, O> String performTyped(Context ctx, AgentTool<I, O> tool, ToolCall call) {
        I input;
        try {
            input = tool.decode(call.argumentsJson());
        } catch (IllegalArgumentException e) {
            return "error: " + e.getMessage();
        }
        try {
            return tool.encode(ctx.call(tool.tool(), input));
        } catch (RuntimeException e) {
            // Only a failure recorded as the call's result goes to the model. Anything else (an engine
            // signal, a failed log write) means the outcome is not in the log, and carrying on would
            // record the next turn on top of that gap.
            if (isEngineSignal(e) || !ctx.isRecordedToolFailure(e)) throw e;
            // The same text live and on replay: a live failure was recorded as String.valueOf(e).
            String error = e instanceof RecordedToolFailureException r ? r.recordedError() : String.valueOf(e);
            return "error: " + error;
        }
    }

    /** Raised by the engine, not by a tool: these unwind the task, never reach the model. */
    private static boolean isEngineSignal(RuntimeException e) {
        return e instanceof CancellationSignal || e instanceof ExecutionAbandonedSignal
                || e instanceof ExecutionWaitingSignal || e instanceof ExecutionPausedSignal
                || e instanceof StaleWriterException || e instanceof NonDeterministicReplayException
                || e instanceof InDoubtException || e instanceof UnrecordedToolCallException;
    }

    public static final class Builder {
        private String instructions;
        private final Map<String, AgentTool<?, ?>> tools = new LinkedHashMap<>();
        private int maxSteps = 10;

        /** The system prompt: what the agent is for and how to behave. */
        public Builder instructions(String instructions) {
            this.instructions = instructions;
            return this;
        }

        /** Offers {@code tool} to the model, described as {@code description}. */
        public <I, O> Builder tool(Tool<I, O> tool, String description) {
            AgentTool<I, O> agentTool = AgentTool.of(tool, description);
            if (tools.putIfAbsent(tool.name(), agentTool) != null) {
                throw new IllegalArgumentException("Two tools are named '" + tool.name() + "'");
            }
            return this;
        }

        /**
         * Offers a tool that takes the model's arguments as JSON and brings its own input schema, such
         * as one discovered from an MCP server ({@code catalyst-mcp}). See {@link AgentTool#withSchema}.
         */
        public <O> Builder tool(Tool<com.fasterxml.jackson.databind.JsonNode, O> tool, String description, String inputSchemaJson) {
            AgentTool<com.fasterxml.jackson.databind.JsonNode, O> agentTool = AgentTool.withSchema(tool, description, inputSchemaJson);
            if (tools.putIfAbsent(tool.name(), agentTool) != null) {
                throw new IllegalArgumentException("Two tools are named '" + tool.name() + "'");
            }
            return this;
        }

        /** The most model turns one run may take (default 10). */
        public Builder maxSteps(int maxSteps) {
            if (maxSteps < 1) throw new IllegalArgumentException("maxSteps must be at least 1");
            this.maxSteps = maxSteps;
            return this;
        }

        public Agent build() {
            return new Agent(this);
        }
    }
}
