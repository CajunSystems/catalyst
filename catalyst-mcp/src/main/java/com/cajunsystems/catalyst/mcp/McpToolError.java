package com.cajunsystems.catalyst.mcp;

/**
 * An MCP server answered a tool call with {@code isError: true}. Thrown from {@link McpTool#apply}, so
 * it is recorded as that call's failure: a replay reproduces it without contacting the server, and
 * an agent hands the message to the model like any tool failure.
 */
public class McpToolError extends RuntimeException {

    public McpToolError(String toolName, String message) {
        super("MCP tool '" + toolName + "' reported an error: " + message);
    }
}
