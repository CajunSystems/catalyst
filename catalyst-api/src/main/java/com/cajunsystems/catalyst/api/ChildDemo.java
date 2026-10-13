package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.ExecutionWaitingSignal;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

/**
 * The child execution exit demo ({@code Demo child}), shared with {@code ChildAcceptanceTest}. A
 * coordinator delegates research and then drafting to two child executions. Each is its own
 * durable execution with its own stream. The runtime shuts down while the drafting child is
 * mid-way. A fresh runtime finishes the child, which wakes the coordinator, with no model call
 * repeated, and the coordinator then replays with zero calls.
 */
final class ChildDemo {

    private ChildDemo() {}

    static final class Research implements Task<String> {
        @Override public String execute(Context ctx) {
            return ctx.model().complete(CompletionRequest.of(Prompt.builder().user("research the outage").build())).message();
        }
    }

    /** Drafting waits on an approval window (a short durable sleep here) before writing. */
    static final class Draft implements Task<String> {
        @Override public String execute(Context ctx) {
            ctx.sleep(Duration.ofMillis(1200));
            return ctx.model().complete(CompletionRequest.of(Prompt.builder().user("draft the postmortem").build())).message();
        }
    }

    static final class Coordinator implements Task<String> {
        @Override public String execute(Context ctx) {
            String findings = ctx.child(new Research());
            String draft = ctx.child(new Draft());
            return "findings=" + findings + " draft=" + draft;
        }
    }

    record Outcome(ExecutionId parent, List<ExecutionId> children, Status parentWhileWaiting, String result,
                   int callsBeforeRestart, int callsAfterRestart, int replayCalls) {}

    private static CatalystRuntime.Builder builder(Path dir, MockModel model) {
        return Catalyst.builder(dir).model(model)
                .task(Coordinator.class, Coordinator::new).task(Research.class, Research::new).task(Draft.class, Draft::new);
    }

    static Outcome run(Path dir) {
        MockModel first = MockModel.alwaysReturn("db failover");
        ExecutionId parent;
        Status whileWaiting;
        try (CatalystRuntime runtime = Catalyst.builder().log(GumboEventLog.at(dir)).model(first)
                .task(Coordinator.class, Coordinator::new).task(Research.class, Research::new).task(Draft.class, Draft::new).build()) {
            var run = runtime.execute(new Coordinator(), ExecutionOptions.withKey("incident-7"));
            try {
                run.result(Duration.ofSeconds(10));
            } catch (ExecutionWaitingSignal parked) {
                // the coordinator waits on its first child with no thread held
            }
            parent = run.id();
            // Wait until the coordinator is waiting on the drafting child (which is asleep).
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (children(runtime, parent).size() < 2 || runtime.inspect(children(runtime, parent).get(1)).status() != Status.WAITING) {
                if (System.nanoTime() > deadline) throw new AssertionError("the drafting child never started");
                sleepQuietly(20);
            }
            whileWaiting = runtime.inspect(parent).status();
        } // shut down mid-way: the coordinator and the drafting child are both waiting

        MockModel second = MockModel.alwaysReturn("postmortem v1");
        try (CatalystRuntime runtime = builder(dir, second).recoverOnStart(true).build()) {
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (runtime.inspect(parent).status() != Status.COMPLETED) {
                if (System.nanoTime() > deadline) throw new AssertionError("the coordinator never finished");
                sleepQuietly(20);
            }
            String result = String.valueOf(runtime.resume(parent).result());
            int callsAfter = second.callCount();
            if (runtime.replay(parent, new Coordinator()).status() != Status.COMPLETED) {
                throw new AssertionError("the coordinator did not replay exactly");
            }
            return new Outcome(parent, children(runtime, parent), whileWaiting, result, first.callCount(), callsAfter,
                    second.callCount() - callsAfter);
        }
    }

    private static List<ExecutionId> children(CatalystRuntime runtime, ExecutionId parent) {
        return runtime.log().read(parent).stream().map(se -> se.event())
                .filter(e -> e instanceof CatalystEvent.ChildStarted)
                .map(e -> ExecutionId.of(((CatalystEvent.ChildStarted) e).childId())).distinct().toList();
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static void main(Path dir) {
        Outcome o = run(dir);
        System.out.println("[child] coordinator " + o.parent().value() + " delegated to " + o.children().size() + " child executions: "
                + o.children().stream().map(ExecutionId::value).toList());
        System.out.println("[child] while its children ran, the coordinator was " + o.parentWhileWaiting() + " (no thread held); the runtime shut down mid-way");
        System.out.println("[child] a fresh runtime finished it: " + o.result());
        System.out.println("[child] model calls: before the restart " + o.callsBeforeRestart() + ", after " + o.callsAfterRestart());
        System.out.println("[child] calls during a strict replay of the coordinator: " + o.replayCalls());
        if (o.children().size() != 2 || o.parentWhileWaiting() != Status.WAITING || o.callsBeforeRestart() != 1
                || o.callsAfterRestart() != 1 || o.replayCalls() != 0) {
            throw new AssertionError("FAIL: child executions did not behave");
        }
        System.out.println("[child] Child execution exit criterion holds: delegated work ran as separate durable executions,"
                + " survived a restart without repeating a call, and the coordinator replays exactly.");
    }
}
