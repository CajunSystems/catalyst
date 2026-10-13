package com.cajunsystems.catalyst.mcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.json.jackson2.JacksonMcpJsonMapper;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A real MCP server over stdio, run as its own JVM by the tests: a ticketing system with a lookup, an
 * escalation that always reports an error, and a counter of the calls it has handled. The counter
 * is how a test proves a replay never reached the server.
 */
public final class TicketMcpServer {

    public static void main(String[] args) throws InterruptedException {
        JacksonMcpJsonMapper json = new JacksonMcpJsonMapper(new ObjectMapper());
        AtomicInteger handled = new AtomicInteger();

        McpSchema.Tool lookup = McpSchema.Tool.builder().name("lookup_ticket")
                .description("Look up a support ticket by its id")
                .inputSchema(json, "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\",\"description\":\"ticket id\"}},\"required\":[\"id\"]}")
                .build();
        McpSchema.Tool escalate = McpSchema.Tool.builder().name("escalate")
                .description("Escalate a ticket to the on-call engineer")
                .inputSchema(json, "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"]}")
                .build();
        McpSchema.Tool count = McpSchema.Tool.builder().name("call_count")
                .description("How many lookup and escalate calls this server has handled")
                .inputSchema(json, "{\"type\":\"object\",\"properties\":{}}")
                .build();

        McpServer.sync(new StdioServerTransportProvider(json))
                .serverInfo("tickets", "1.0")
                .tools(
                        new McpServerFeatures.SyncToolSpecification(lookup, (exchange, request) -> {
                            handled.incrementAndGet();
                            Object id = request.arguments().get("id");
                            return McpSchema.CallToolResult.builder()
                                    .addTextContent("{\"id\":\"" + id + "\",\"summary\":\"printer offline\",\"priority\":1}")
                                    .isError(false).build();
                        }),
                        new McpServerFeatures.SyncToolSpecification(escalate, (exchange, request) -> {
                            handled.incrementAndGet();
                            return McpSchema.CallToolResult.builder().addTextContent("pager service down").isError(true).build();
                        }),
                        new McpServerFeatures.SyncToolSpecification(count, (exchange, request) ->
                                McpSchema.CallToolResult.builder().addTextContent(String.valueOf(handled.get())).isError(false).build()))
                .build();

        new CountDownLatch(1).await(); // served until the client closes the process
    }
}
