package com.cajunsystems.catalyst;

import java.util.Map;

/** Identity and metadata for an execution, exposed to task code via {@link Context#info()}. */
public record ExecutionInfo(ExecutionId id, int attempt, String taskType, Map<String, String> metadata) {

    public ExecutionInfo {
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }
}
