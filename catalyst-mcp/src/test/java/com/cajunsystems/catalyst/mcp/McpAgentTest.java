package com.cajunsystems.catalyst.mcp;

import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.agents.Agent;
import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Message;
import com.cajunsystems.catalyst.model.Model;
import com.cajunsystems.catalyst.model.Role;
import com.cajunsystems.catalyst.model.ToolCall;
import com.cajunsystems.catalyst.model.Usage;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.cajunsystems.catalyst.runtime.EventLogs;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.ServerParameters;
import io.modelcontextprotocol.client.transport.StdioClientTransport;
import io.modelcontextprotocol.json.jackson2.JacksonMcpJsonMapper;
import io.modelcontextprotocol.spec.McpSchema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * MCP end to end: a real MCP server ({@link TicketMcpServer}) in its own JVM over stdio, its tools
 * discovered and handed to an {@link Agent}. The server's call counter shows that a strict replay of
 * the agent's run contacts the server zero times, because every call was a recorded boundary.
 */
@Timeout(120)
class McpAgentTest {

    static McpSyncClient client;

    @BeforeAll
    static void startServer() {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        ServerParameters server = ServerParameters.builder(java)
                .args("-cp", System.getProperty("java.class.path"), TicketMcpServer.class.getName())
                .build();
        client = McpClient.sync(new StdioClientTransport(server, new JacksonMcpJsonMapper(new ObjectMapper())))
                .requestTimeout(Duration.ofSeconds(30)).initializationTimeout(Duration.ofSeconds(60)).build();
        client.initialize();
    }

    @AfterAll
    static void stopServer() {
        if (client != null) client.closeGracefully();
    }

    /** The server's own count of real lookup/escalate calls (asked directly, not through the agent). */
    static int serverCalls() {
        return Integer.parseInt(McpTool.textOf(client.callTool(new McpSchema.CallToolRequest("call_count", Map.of()))));
    }

    /** A tool-calling stand-in for a model: call {@code tool} on ticket T-1 once, then answer with what came back. */
    static Model callsThenAnswers(String tool) {
        return request -> {
            List<Message> results = request.prompt().messages().stream().filter(m -> m.role() == Role.TOOL).toList();
            if (results.isEmpty()) {
                return new Completion("", List.of(new ToolCall(null, tool, "{\"id\":\"T-1\"}")), new Usage(20, 5), "tool_calls");
            }
            return new Completion("answer: " + results.get(0).content(), List.of(), new Usage(30, 8), "stop");
        };
    }

    @Test
    void theServersToolsAreDiscoveredWithTheirSchemas() {
        McpToolset tools = McpToolset.from(client);
        assertThat(tools.tools()).extracting(McpTool::name).containsExactly("call_count", "escalate", "lookup_ticket");
        McpTool lookup = tools.only("lookup_ticket").tools().get(0);
        assertThat(lookup.description()).isEqualTo("Look up a support ticket by its id");
        assertThat(lookup.inputSchemaJson()).contains("\"id\"").contains("\"required\"");
        assertThatThrownBy(() -> tools.only("lookup_ticket", "nope"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("[nope]");
    }

    @Test
    void anAgentUsesAnMcpToolAndItsReplayNeverContactsTheServer() {
        Agent agent = McpToolset.from(client).only("lookup_ticket").addTo(Agent.builder().instructions("Triage.")).build();
        Task<String> task = ctx -> agent.run(ctx, "triage T-1").answer();
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory())
                .model(callsThenAnswers("lookup_ticket")).build()) {
            int before = serverCalls();
            var run = runtime.execute(task, ExecutionOptions.none());
            assertThat(run.result()).isEqualTo("answer: {\"id\":\"T-1\",\"summary\":\"printer offline\",\"priority\":1}");
            assertThat(serverCalls()).as("one real call").isEqualTo(before + 1);

            assertThat(runtime.replay(run.id(), task).status()).isEqualTo(Status.COMPLETED);
            assertThat(serverCalls()).as("the replay never reached the server").isEqualTo(before + 1);
        }
    }

    /** A server-reported error is the call's recorded failure: told to the model, and replayed without the server. */
    @Test
    void anMcpErrorReachesTheModelAndReplaysIdentically() {
        Agent agent = McpToolset.from(client).only("escalate").addTo(Agent.builder()).build();
        Task<String> task = ctx -> agent.run(ctx, "escalate T-1").answer();
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(EventLogs.inMemory())
                .model(callsThenAnswers("escalate")).build()) {
            int before = serverCalls();
            var run = runtime.execute(task, ExecutionOptions.none());
            assertThat(run.result()).isEqualTo(
                    "answer: error: com.cajunsystems.catalyst.mcp.McpToolError: MCP tool 'escalate' reported an error: pager service down");
            assertThat(runtime.replay(run.id(), task).status()).isEqualTo(Status.COMPLETED);
            assertThat(serverCalls()).isEqualTo(before + 1);
        }
    }
}
