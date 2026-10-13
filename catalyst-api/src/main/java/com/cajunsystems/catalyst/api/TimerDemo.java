package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.ExecutionWaitingSignal;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

/**
 * The durable timer exit demo ({@code Demo timer}), shared with {@code TimerAcceptanceTest}. A
 * follow-up task drafts a message, sleeps, then sends the follow-up. Its runtime shuts down while it
 * sleeps. A fresh runtime with {@code recoverOnStart} finds the sleeper, wakes it when the timer
 * comes due, and finishes it without repeating the first model call. The whole run then replays
 * with zero calls.
 */
final class TimerDemo {

    private TimerDemo() {}

    static final Duration SLEEP = Duration.ofMillis(1500);
    private static final CompletionRequest DRAFT = CompletionRequest.of(Prompt.builder().user("draft the welcome").build());
    private static final CompletionRequest FOLLOW_UP = CompletionRequest.of(Prompt.builder().user("draft the follow-up").build());

    static final class FollowUpTask implements Task<String> {
        @Override public String execute(Context ctx) {
            String welcome = ctx.model().complete(DRAFT).message();
            ctx.sleep(SLEEP); // in production: Duration.ofDays(3)
            return welcome + " | " + ctx.model().complete(FOLLOW_UP).message();
        }
    }

    record Outcome(ExecutionId id, Status whileSleeping, boolean leftAloneAtStartup, String result,
                   int firstRunCalls, int secondRunCalls, long wokeAfterMillis, int replayCalls) {}

    static Outcome run(Path dir) {
        MockModel first = MockModel.alwaysReturn("hello");
        ExecutionId id;
        Status whileSleeping;
        Instant started = Instant.now();
        try (CatalystRuntime runtime = Catalyst.builder().log(GumboEventLog.at(dir)).model(first)
                .task(FollowUpTask.class, FollowUpTask::new).build()) {
            var run = runtime.execute(new FollowUpTask(), ExecutionOptions.withKey("user-42"));
            try {
                run.result(Duration.ofSeconds(10));
                throw new AssertionError("the task was supposed to sleep");
            } catch (ExecutionWaitingSignal sleeping) {
                // parked: the attempt ended and no thread is held
            }
            id = run.id();
            whileSleeping = runtime.inspect(id).status();
        } // shut down while it sleeps

        MockModel second = MockModel.alwaysReturn("checking in");
        try (CatalystRuntime runtime = Catalyst.builder(dir).model(second)
                .task(FollowUpTask.class, FollowUpTask::new).recoverOnStart(true).build()) {
            boolean leftAlone = runtime.startupRecovery().orElseThrow().leftAlone().get(id) == Status.WAITING;
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (runtime.inspect(id).status() != Status.COMPLETED) {
                if (System.nanoTime() > deadline) throw new AssertionError("the timer never fired");
                sleepQuietly(20);
            }
            long woke = Duration.between(started, Instant.now()).toMillis();
            String result = String.valueOf(runtime.resume(id).result());
            int callsBefore = second.callCount();
            if (runtime.replay(id, new FollowUpTask()).status() != Status.COMPLETED) {
                throw new AssertionError("the run did not replay exactly");
            }
            return new Outcome(id, whileSleeping, leftAlone, result, first.callCount(), callsBefore, woke,
                    second.callCount() - callsBefore);
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
        System.out.println("[timer] execution " + o.id().value() + " slept as " + o.whileSleeping() + " (no thread held); its runtime shut down");
        System.out.println("[timer] a fresh runtime found it at startup and left it to sleep: " + o.leftAloneAtStartup());
        System.out.println("[timer] woke and finished " + o.wokeAfterMillis() + " ms after it started (timer " + SLEEP.toMillis() + " ms): " + o.result());
        System.out.println("[timer] model calls: before the sleep " + o.firstRunCalls() + ", after the wake " + o.secondRunCalls());
        System.out.println("[timer] calls during a strict replay: " + o.replayCalls());
        if (o.whileSleeping() != Status.WAITING || !o.leftAloneAtStartup() || o.firstRunCalls() != 1
                || o.secondRunCalls() != 1 || o.replayCalls() != 0 || o.wokeAfterMillis() < SLEEP.toMillis()) {
            throw new AssertionError("FAIL: the durable timer did not behave");
        }
        System.out.println("[timer] Durable timer exit criterion holds: the sleep survived a restart, woke on time,"
                + " repeated no call, and replays exactly.");
    }
}
