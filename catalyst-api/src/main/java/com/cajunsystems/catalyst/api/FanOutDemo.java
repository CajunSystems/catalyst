package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.ChildHandle;
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
 * The fan-out exit demo ({@code Demo fanout}), shared with {@code FanOutAcceptanceTest}. A
 * coordinator spawns three research children at once, each waiting on a short durable timer (an
 * upstream rate limit, say) before its model call, and awaits them all. The runtime shuts down while
 * all three are waiting. A fresh runtime finishes them, which wakes the coordinator, with no model
 * call repeated, and the coordinator then replays with zero calls.
 */
final class FanOutDemo {

    private FanOutDemo() {}

    abstract static class Research implements Task<String> {
        private final String topic;
        Research(String topic) { this.topic = topic; }
        @Override public String execute(Context ctx) {
            ctx.sleep(Duration.ofMillis(1200));
            return topic + "=" + ctx.model().complete(CompletionRequest.of(Prompt.builder().user("research " + topic).build())).message();
        }
    }

    // One task type per topic, so a resume by id after the restart knows which topic it was.
    static final class Logs extends Research { Logs() { super("logs"); } }
    static final class Metrics extends Research { Metrics() { super("metrics"); } }
    static final class Traces extends Research { Traces() { super("traces"); } }

    static final class Coordinator implements Task<String> {
        @Override public String execute(Context ctx) {
            List<ChildHandle<String>> research = List.of(ctx.spawn(new Logs()), ctx.spawn(new Metrics()), ctx.spawn(new Traces()));
            return String.join("; ", ctx.awaitAll(research));
        }
    }

    record Outcome(ExecutionId parent, List<ExecutionId> children, List<Status> childrenAtShutdown, String result,
                   int callsBeforeRestart, int callsAfterRestart, int replayCalls) {}

    private static CatalystRuntime.Builder builder(Path dir, MockModel model) {
        return CatalystRuntime.builder().log(GumboEventLog.at(dir)).model(model)
                .task(Coordinator.class, Coordinator::new).task(Logs.class, Logs::new)
                .task(Metrics.class, Metrics::new).task(Traces.class, Traces::new);
    }

    static Outcome run(Path dir) {
        MockModel first = MockModel.alwaysReturn("ok");
        ExecutionId parent;
        List<Status> atShutdown;
        try (CatalystRuntime runtime = builder(dir, first).build()) {
            var run = runtime.execute(new Coordinator(), ExecutionOptions.withKey("incident-review"));
            try {
                run.result(Duration.ofSeconds(10));
            } catch (ExecutionWaitingSignal parked) {
                // the coordinator waits on its children with no thread held
            }
            parent = run.id();
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (children(runtime, parent).size() < 3
                    || !children(runtime, parent).stream().allMatch(c -> runtime.inspect(c).status() == Status.WAITING)) {
                if (System.nanoTime() > deadline) throw new AssertionError("the children never all started");
                sleepQuietly(10);
            }
            atShutdown = children(runtime, parent).stream().map(c -> runtime.inspect(c).status()).toList();
        } // shut down while all three children are waiting at once

        MockModel second = MockModel.alwaysReturn("ok");
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
            return new Outcome(parent, children(runtime, parent), atShutdown, result, first.callCount(), callsAfter,
                    second.callCount() - callsAfter);
        }
    }

    private static List<ExecutionId> children(CatalystRuntime runtime, ExecutionId parent) {
        return runtime.log().read(parent).stream().map(se -> se.event())
                .filter(e -> e instanceof CatalystEvent.ChildSpawned)
                .map(e -> ExecutionId.of(((CatalystEvent.ChildSpawned) e).childId())).toList();
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
        System.out.println("[fanout] coordinator " + o.parent().value() + " spawned " + o.children().size() + " children at once: "
                + o.children().stream().map(ExecutionId::value).toList());
        System.out.println("[fanout] at shutdown the children were " + o.childrenAtShutdown() + " (all running together)");
        System.out.println("[fanout] a fresh runtime finished them: " + o.result());
        System.out.println("[fanout] model calls: before the restart " + o.callsBeforeRestart() + ", after " + o.callsAfterRestart());
        System.out.println("[fanout] calls during a strict replay of the coordinator: " + o.replayCalls());
        if (o.children().size() != 3 || !o.childrenAtShutdown().equals(List.of(Status.WAITING, Status.WAITING, Status.WAITING))
                || o.callsBeforeRestart() != 0 || o.callsAfterRestart() != 3 || o.replayCalls() != 0) {
            throw new AssertionError("FAIL: fan-out did not behave");
        }
        System.out.println("[fanout] Fan-out exit criterion holds: children ran at the same time as separate durable executions,"
                + " survived a restart without repeating a call, and the coordinator replays exactly.");
    }
}
