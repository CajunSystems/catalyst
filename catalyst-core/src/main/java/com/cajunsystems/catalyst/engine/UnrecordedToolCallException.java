package com.cajunsystems.catalyst.engine;

/**
 * A branch running with {@link BranchSpec.ToolReplay#RECORDED_ONLY} made a tool call its parent never
 * made with that input, so there is no recorded answer, and running it for real is not allowed. In an
 * evaluation this means the candidate took a path the recording cannot answer. That is a finding about
 * the candidate, not an error in the harness.
 */
public class UnrecordedToolCallException extends RuntimeException {

    private final String toolName;
    private final String inputHash;

    public UnrecordedToolCallException(String toolName, String inputHash) {
        super("Tool '" + toolName + "' was called with an input the recording never saw (" + inputHash
                + "), and this branch may not run tools live");
        this.toolName = toolName;
        this.inputHash = inputHash;
    }

    public String toolName() {
        return toolName;
    }

    public String inputHash() {
        return inputHash;
    }
}
