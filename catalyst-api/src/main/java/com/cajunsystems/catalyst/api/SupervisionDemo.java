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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The supervision exit demo ({@code Demo supervise}), shared with {@code SupervisionAcceptanceTest}.
 * A coordinator delegates to a flaky child under {@code Supervision.restart(3).backoff(...)}. The
 * child fails twice. The runtime shuts down during the first backoff, and a fresh runtime carries
 * on: each restart is a fresh child execution, the third succeeds, and the coordinator then replays
 * with zero calls, walking the same recorded restarts.
 */
final class SupervisionDemo {

    private SupervisionDemo() {}

    static final AtomicInteger runs = new AtomicInteger();

    /** Fails on its first two runs (a flaky upstream), then does the work. */
    static final class FlakyFetch implements Task<String> {
        @Override public String execute(Context ctx) {
            int n = runs.incrementAndGet();
            if (n <= 2) throw new IllegalStateException("upstream timeout (run " + n + ")");
            return ctx.model().complete(CompletionRequest.of(Prompt.builder().user("summarise the feed").build())).message();
        }
    }

    static final class Coordinator implements Task<String> {
        @Override public String execute(Context ctx) {
            return "summary=" + ctx.child(new FlakyFetch(), Supervision.restart(3).backoff(Duration.ofMillis(800)));
        }
    }

    record Outcome(ExecutionId parent, List<String> refs, List<Status> childStatuses, Status parentAtShutdown,
                   String result, int callsBeforeRestart, int callsAfterRestart, int replayCalls) {}

    private static CatalystRuntime.Builder builder(GumboEventLog log, MockModel model) {
        return CatalystRuntime.builder().log(log).model(model)
                .task(Coordinator.class, Coordinator::new).task(FlakyFetch.class, FlakyFetch::new);
    }

    static Outcome run(Path dir) {
        runs.set(0);
        MockModel first = MockModel.alwaysReturn("feed ok");
        ExecutionId parent;
        Status atShutdown;
        try (CatalystRuntime runtime = builder(GumboEventLog.at(dir), first).build()) {
            var run = runtime.execute(new Coordinator(), ExecutionOptions.withKey("feed-digest"));
            try {
                run.result(Duration.ofSeconds(10));
            } catch (ExecutionWaitingSignal parked) {
                // waiting on its child
            }
            parent = run.id();
            // Wait for the first failure to be recorded and the backoff to start.
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (runtime.inspect(parent).awaitingTimer().isEmpty()) {
                if (System.nanoTime() > deadline) throw new AssertionError("the first restart's backoff never started");
                sleepQuietly(10);
            }
            atShutdown = runtime.inspect(parent).status();
        } // shut down during the backoff before the first restart

        MockModel second = MockModel.alwaysReturn("feed ok");
        try (CatalystRuntime runtime = builder(GumboEventLog.at(dir), second).recoverOnStart(true).build()) {
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (runtime.inspect(parent).status() != Status.COMPLETED) {
                if (System.nanoTime() > deadline) throw new AssertionError("the coordinator never finished");
                sleepQuietly(20);
            }
            String result = String.valueOf(runtime.resume(parent).result());
            int callsAfter = second.callCount();
            List<CatalystEvent.ChildStarted> started = runtime.log().read(parent).stream().map(se -> se.event())
                    .filter(e -> e instanceof CatalystEvent.ChildStarted).map(e -> (CatalystEvent.ChildStarted) e).toList();
            List<Status> statuses = started.stream().map(c -> runtime.inspect(ExecutionId.of(c.childId())).status()).toList();
            if (runtime.replay(parent, new Coordinator()).status() != Status.COMPLETED) {
                throw new AssertionError("the coordinator did not replay exactly");
            }
            return new Outcome(parent, started.stream().map(CatalystEvent.ChildStarted::childRef).toList(), statuses,
                    atShutdown, result, first.callCount(), callsAfter, second.callCount() - callsAfter);
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
        System.out.println("[supervise] coordinator " + o.parent().value() + " supervised its child: " + o.refs() + " -> " + o.childStatuses());
        System.out.println("[supervise] the runtime shut down during a restart backoff (coordinator " + o.parentAtShutdown() + ")");
        System.out.println("[supervise] a fresh runtime carried on: " + o.result());
        System.out.println("[supervise] model calls: before the restart " + o.callsBeforeRestart() + ", after " + o.callsAfterRestart());
        System.out.println("[supervise] calls during a strict replay of the coordinator: " + o.replayCalls());
        if (o.refs().size() != 3 || !o.childStatuses().equals(List.of(Status.FAILED, Status.FAILED, Status.COMPLETED))
                || o.callsBeforeRestart() != 0 || o.callsAfterRestart() != 1 || o.replayCalls() != 0) {
            throw new AssertionError("FAIL: supervision did not behave");
        }
        System.out.println("[supervise] Supervision exit criterion holds: a failed child was restarted as fresh executions,"
                + " across a restart of the runtime, and the coordinator replays exactly.");
    }
}
