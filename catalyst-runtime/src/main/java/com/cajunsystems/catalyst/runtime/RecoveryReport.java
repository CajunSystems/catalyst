package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.Status;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * What {@link CatalystRuntime#recoverAll()} did with each open execution, in creation order. Every
 * execution the index listed lands in exactly one of the three maps.
 *
 * @param resumed      crash-interrupted executions now running forward, with their handles
 * @param leftAlone    open executions deliberately not resumed, with the status that decided it —
 *                     {@code PAUSED}/{@code WAITING} are held on purpose, and an execution already
 *                     running in this process ({@code RUNNING}) is not scheduled twice
 * @param notRecovered executions that should have resumed but could not, with the reason — most
 *                     often a task type with no registration. Reported, not thrown: one unknown
 *                     task type must not keep every other execution from recovering
 */
public record RecoveryReport(
        Map<ExecutionId, ExecutionHandle<?>> resumed,
        Map<ExecutionId, Status> leftAlone,
        Map<ExecutionId, String> notRecovered) {

    public RecoveryReport {
        resumed = Collections.unmodifiableMap(new LinkedHashMap<>(resumed));
        leftAlone = Collections.unmodifiableMap(new LinkedHashMap<>(leftAlone));
        notRecovered = Collections.unmodifiableMap(new LinkedHashMap<>(notRecovered));
    }

    @Override
    public String toString() {
        return "RecoveryReport[resumed=" + resumed.size() + ", leftAlone=" + leftAlone
                + ", notRecovered=" + notRecovered + "]";
    }
}
