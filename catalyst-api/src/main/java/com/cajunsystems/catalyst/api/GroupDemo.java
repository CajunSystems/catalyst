package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.ExecutionWaitingSignal;
import com.cajunsystems.catalyst.engine.Supervision;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The supervised-group exit demo ({@code Demo group}), shared with {@code GroupAcceptanceTest}. A
 * coordinator runs three children that only make sense together (a draft, its fact-check and its
 * summary of one snapshot) with {@code ctx.supervise(Supervision.restart(2).oneForAll().backoff(...))}.
 * The fact-check fails on its first run, so its two siblings, still waiting on a short timer, are
 * cancelled. The runtime then shuts down during the backoff. A fresh runtime restarts all three, the
 * group completes, no model call is repeated, and the coordinator replays exactly.
 */
final class GroupDemo {

    private GroupDemo() {}

    static final Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();

    abstract static class Part implements Task<String> {
        private final String name;
        Part(String name) { this.name = name; }
        @Override public String execute(Context ctx) {
            int run = runs.computeIfAbsent(name, k -> new AtomicInteger()).incrementAndGet();
            if (name.equals("factcheck") && run == 1) throw new IllegalStateException("source snapshot was stale");
            if (!name.equals("factcheck")) ctx.sleep(Duration.ofMillis(400));
            return name + "=" + ctx.model().complete(CompletionRequest.of(Prompt.builder().user(name).build())).message();
        }
    }

    static final class Draft extends Part { Draft() { super("draft"); } }
    static final class FactCheck extends Part { FactCheck() { super("factcheck"); } }
    static final class Summary extends Part { Summary() { super("summary"); } }

    static final class Coordinator implements Task<String> {
        @Override public String execute(Context ctx) {
            return String.join("; ", ctx.supervise(Supervision.restart(2).oneForAll().backoff(Duration.ofMillis(800)),
                    List.of(new Draft(), new FactCheck(), new Summary())));
        }
    }

    record Outcome(ExecutionId parent, List<Status> childStatuses, String result,
                   int callsBeforeRestart, int callsAfterRestart, int replayCalls) {}

    private static CatalystRuntime.Builder builder(Path dir, MockModel model) {
        return CatalystRuntime.builder().log(GumboEventLog.at(dir)).model(model)
                .task(Coordinator.class, Coordinator::new).task(Draft.class, Draft::new)
                .task(FactCheck.class, FactCheck::new).task(Summary.class, Summary::new);
    }

    static Outcome run(Path dir) {
        runs.clear();
        MockModel first = MockModel.alwaysReturn("ok");
        ExecutionId parent;
        try (CatalystRuntime runtime = builder(dir, first).build()) {
            var run = runtime.execute(new Coordinator(), ExecutionOptions.withKey("postmortem-pack"));
            try {
                run.result(Duration.ofSeconds(10));
            } catch (ExecutionWaitingSignal parked) {
                // waiting on its group
            }
            parent = run.id();
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (runtime.inspect(parent).awaitingTimer().isEmpty()) {
                if (System.nanoTime() > deadline) throw new AssertionError("the group never started its restart backoff");
                sleepQuietly(10);
            }
        } // shut down during the backoff before the whole group restarts

        MockModel second = MockModel.alwaysReturn("ok");
        try (CatalystRuntime runtime = builder(dir, second).recoverOnStart(true).build()) {
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (runtime.inspect(parent).status() != Status.COMPLETED) {
                if (System.nanoTime() > deadline) throw new AssertionError("the coordinator never finished");
                sleepQuietly(20);
            }
            String result = String.valueOf(runtime.resume(parent).result());
            int callsAfter = second.callCount();
            List<Status> statuses = runtime.log().read(parent).stream().map(se -> se.event())
                    .filter(e -> e instanceof CatalystEvent.ChildSpawned)
                    .map(e -> runtime.inspect(ExecutionId.of(((CatalystEvent.ChildSpawned) e).childId())).status()).toList();
            if (runtime.replay(parent, new Coordinator()).status() != Status.COMPLETED) {
                throw new AssertionError("the coordinator did not replay exactly");
            }
            return new Outcome(parent, statuses, result, first.callCount(), callsAfter, second.callCount() - callsAfter);
        }
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
        System.out.println("[group] coordinator " + o.parent().value() + " supervised its group one-for-all: " + o.childStatuses());
        System.out.println("[group] the fact-check failed, so its siblings were cancelled; the runtime shut down during the backoff");
        System.out.println("[group] a fresh runtime restarted the whole group: " + o.result());
        System.out.println("[group] model calls: before the restart " + o.callsBeforeRestart() + ", after " + o.callsAfterRestart());
        System.out.println("[group] calls during a strict replay of the coordinator: " + o.replayCalls());
        List<Status> expected = List.of(Status.CANCELLED, Status.FAILED, Status.CANCELLED,
                Status.COMPLETED, Status.COMPLETED, Status.COMPLETED);
        if (!o.childStatuses().equals(expected) || o.callsBeforeRestart() != 0 || o.callsAfterRestart() != 3 || o.replayCalls() != 0) {
            throw new AssertionError("FAIL: the supervised group did not behave");
        }
        System.out.println("[group] Supervised group exit criterion holds: one failure restarted the whole group,"
                + " across a restart of the runtime, and the coordinator replays exactly.");
    }
}
