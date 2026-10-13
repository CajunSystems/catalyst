package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.Tool;
import com.cajunsystems.catalyst.engine.CallPolicy;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Model;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The per-call retries and timeouts exit demo ({@code Demo calls}), shared with
 * {@code CallsAcceptanceTest}. A provider answers 503 twice before succeeding, and a lookup tool hangs
 * on its first try. With {@code callPolicy(CallPolicy.retries(3, ...).timeout(...))} each call is
 * retried in place: the task finishes in one attempt, with no whole-task retry, and the log holds one
 * request and one result per call. A strict replay then makes no call at all.
 */
final class CallsDemo {

    private CallsDemo() {}

    static final AtomicInteger providerCalls = new AtomicInteger();
    static final AtomicInteger lookups = new AtomicInteger();

    /** Fails the first two calls with a 503, as an overloaded provider does. */
    static final Model FLAKY_PROVIDER = request -> {
        if (providerCalls.incrementAndGet() <= 2) throw new IllegalStateException("503 Service Unavailable");
        return Completion.ofText("refund approved");
    };

    /** Hangs on its first call, as a stuck downstream service does. */
    static final Tool<String, String> LOOKUP = new Tool<>() {
        @Override public String name() { return "lookup-order"; }
        @Override public Class<String> inputType() { return String.class; }
        @Override public String apply(String order) throws Exception {
            if (lookups.incrementAndGet() == 1) Thread.sleep(30_000);
            return order + ": delivered, damaged";
        }
    };

    static final class Refund implements Task<String> {
        @Override public String execute(Context ctx) {
            String order = ctx.call(LOOKUP, "order-1138");
            String decision = ctx.model().complete(CompletionRequest.of(Prompt.builder().user("refund? " + order).build())).message();
            return order + " -> " + decision;
        }
    }

    record Outcome(ExecutionId id, String result, int providerCalls, int lookups, long callRetries,
                   long taskRetries, Duration took, int replayProviderCalls, int replayLookups) {}

    static Outcome run(Path dir) {
        providerCalls.set(0);
        lookups.set(0);
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(GumboEventLog.at(dir)).model(FLAKY_PROVIDER)
                .callPolicy(CallPolicy.retries(3, Duration.ofMillis(100)).timeout(Duration.ofSeconds(1)))
                .task(Refund.class, Refund::new).build()) {
            long t0 = System.nanoTime();
            var run = runtime.execute(new Refund(), ExecutionOptions.withKey("refund-1138"));
            String result = String.valueOf(run.result(Duration.ofSeconds(20)));
            Duration took = Duration.ofNanos(System.nanoTime() - t0);
            long callRetries = runtime.log().read(run.id()).stream().filter(se -> se.event() instanceof CatalystEvent.CallRetried).count();
            long taskRetries = runtime.log().read(run.id()).stream().filter(se -> se.event() instanceof CatalystEvent.RetryRequested).count();
            int calls = providerCalls.get();
            int looks = lookups.get();
            if (runtime.replay(run.id(), new Refund()).status() != Status.COMPLETED) {
                throw new AssertionError("the execution did not replay exactly");
            }
            return new Outcome(run.id(), result, calls, looks, callRetries, taskRetries, took,
                    providerCalls.get() - calls, lookups.get() - looks);
        }
    }

    static void main(Path dir) {
        Outcome o = run(dir);
        System.out.println("[calls] execution " + o.id().value() + ": " + o.result());
        System.out.println("[calls] the lookup hung once and timed out; the provider answered 503 twice: "
                + o.lookups() + " lookups, " + o.providerCalls() + " provider calls, " + o.callRetries() + " call retries");
        System.out.println("[calls] whole-task retries: " + o.taskRetries() + "; took " + o.took().toMillis() + " ms (the hang was cut at 1s)");
        System.out.println("[calls] calls during a strict replay: " + (o.replayProviderCalls() + o.replayLookups()));
        if (o.lookups() != 2 || o.providerCalls() != 3 || o.callRetries() != 3 || o.taskRetries() != 0
                || o.took().compareTo(Duration.ofSeconds(10)) >= 0 || o.replayProviderCalls() + o.replayLookups() != 0) {
            throw new AssertionError("FAIL: per-call retries did not behave");
        }
        System.out.println("[calls] Per-call retry exit criterion holds: each call was retried in place, a hung call"
                + " timed out, the task ran in one attempt, and it replays with no calls.");
    }
}
