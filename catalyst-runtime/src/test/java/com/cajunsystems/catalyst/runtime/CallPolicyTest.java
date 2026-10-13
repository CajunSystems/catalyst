package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.Tool;
import com.cajunsystems.catalyst.engine.CallPolicy;
import com.cajunsystems.catalyst.engine.CallTimeoutException;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Model;
import com.cajunsystems.catalyst.model.Prompt;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Per-call retries and timeouts: {@code CallPolicy} retries one model or tool call inside the attempt
 * that makes it, recording only the outcome (plus an informational {@code CallRetried} per retry),
 * so a replay substitutes it without retrying. A timeout gives up on a hung call.
 */
@Timeout(30)
class CallPolicyTest {

    private static final CompletionRequest ASK = CompletionRequest.of(Prompt.builder().user("ask").build());

    /** A tool that fails its first {@code failFirst} invocations, or hangs on its first {@code hangFirst}. */
    static final AtomicInteger invocations = new AtomicInteger();
    static volatile int failFirst = 0;
    static volatile int hangFirst = 0;
    static volatile String failWith = "transient";

    static final Tool<String, String> FLAKY = new Tool<>() {
        @Override public String name() { return "flaky"; }
        @Override public Class<String> inputType() { return String.class; }
        @Override public String apply(String input) throws Exception {
            int n = invocations.incrementAndGet();
            if (n <= hangFirst) Thread.sleep(10_000);
            if (n <= failFirst) throw new IllegalStateException(failWith + " " + n);
            return "done:" + input;
        }
    };

    static volatile CallPolicy policy = CallPolicy.none();

    static final class UsesTool implements Task<String> {
        @Override public String execute(Context ctx) {
            try {
                return ctx.call(FLAKY, "x", policy);
            } catch (RuntimeException e) {
                return "failed: " + e.getMessage();
            }
        }
    }

    static final class UsesToolDefault implements Task<String> {
        @Override public String execute(Context ctx) {
            return ctx.call(FLAKY, "x");
        }
    }

    static final class UsesModel implements Task<String> {
        @Override public String execute(Context ctx) {
            return ctx.model(policy).complete(ASK).message();
        }
    }

    static final class Streams implements Task<String> {
        @Override public String execute(Context ctx) {
            StringBuilder seen = new StringBuilder();
            try {
                return ctx.model(policy).stream(ASK, seen::append).message() + "|" + seen;
            } catch (RuntimeException e) {
                return "failed after [" + seen + "]: " + e.getMessage();
            }
        }
    }

    @BeforeEach
    void reset() {
        invocations.set(0);
        failFirst = 0;
        hangFirst = 0;
        failWith = "transient";
        policy = CallPolicy.none();
    }

    private static CatalystRuntime runtime(EventLog log, Model model, CallPolicy defaults) {
        return CatalystRuntime.builder().log(log).model(model).callPolicy(defaults)
                .task(UsesTool.class, UsesTool::new).task(UsesToolDefault.class, UsesToolDefault::new)
                .task(UsesModel.class, UsesModel::new).task(Streams.class, Streams::new).build();
    }

    private static long count(EventLog log, ExecutionId id, Class<? extends CatalystEvent> type) {
        return log.read(id).stream().filter(se -> type.isInstance(se.event())).count();
    }

    @Test
    void aFailingToolIsRetriedInPlaceAndOnlyItsOutcomeIsRecorded() {
        failFirst = 2;
        policy = CallPolicy.retries(3, Duration.ofMillis(1));
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log, MockModel.alwaysReturn("M"), CallPolicy.none())) {
            var run = runtime.execute(new UsesTool(), ExecutionOptions.none());
            assertThat(run.result(Duration.ofSeconds(10))).isEqualTo("done:x");
            assertThat(invocations.get()).isEqualTo(3);
            assertThat(count(log, run.id(), CatalystEvent.ToolRequested.class)).isEqualTo(1);
            assertThat(count(log, run.id(), CatalystEvent.ToolCompleted.class)).isEqualTo(1);
            assertThat(count(log, run.id(), CatalystEvent.CallRetried.class)).isEqualTo(2);
            assertThat(count(log, run.id(), CatalystEvent.RetryRequested.class)).as("no whole-task retry").isZero();
            assertThat(runtime.replay(run.id(), new UsesTool()).status()).isEqualTo(Status.COMPLETED);
            assertThat(invocations.get()).as("a replay substitutes the outcome").isEqualTo(3);
        }
    }

    @Test
    void whenRetriesRunOutTheLastFailureIsRecordedAndSurfaces() {
        failFirst = 100;
        policy = CallPolicy.retries(2, Duration.ofMillis(1));
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log, MockModel.alwaysReturn("M"), CallPolicy.none())) {
            var run = runtime.execute(new UsesTool(), ExecutionOptions.none());
            assertThat(run.result(Duration.ofSeconds(10))).isEqualTo("failed: transient 3");
            assertThat(invocations.get()).isEqualTo(3);
            assertThat(log.read(run.id()).stream().map(se -> se.event())
                    .filter(e -> e instanceof CatalystEvent.ToolCompleted).map(e -> ((CatalystEvent.ToolCompleted) e).error()).toList())
                    .singleElement().asString().contains("transient 3");
            assertThat(runtime.replay(run.id(), new UsesTool()).status()).isEqualTo(Status.COMPLETED);
        }
    }

    @Test
    void onlyFailuresThePredicateAcceptsAreRetried() {
        failFirst = 100;
        failWith = "permanent";
        policy = CallPolicy.retries(5, Duration.ofMillis(1)).retryOn(e -> !String.valueOf(e.getMessage()).startsWith("permanent"));
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log, MockModel.alwaysReturn("M"), CallPolicy.none())) {
            var run = runtime.execute(new UsesTool(), ExecutionOptions.none());
            assertThat(run.result(Duration.ofSeconds(10))).isEqualTo("failed: permanent 1");
            assertThat(invocations.get()).isEqualTo(1);
        }
    }

    @Test
    void theRuntimeDefaultAppliesToCallsThatNameNoPolicy() {
        failFirst = 1;
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log, MockModel.alwaysReturn("M"), CallPolicy.retries(2, Duration.ofMillis(1)))) {
            var run = runtime.execute(new UsesToolDefault(), ExecutionOptions.none());
            assertThat(run.result(Duration.ofSeconds(10))).isEqualTo("done:x");
            assertThat(invocations.get()).isEqualTo(2);
        }
    }

    @Test
    void aFailingModelCallIsRetriedUnderOneRequest() {
        AtomicInteger calls = new AtomicInteger();
        Model flaky = request -> {
            if (calls.incrementAndGet() <= 2) throw new IllegalStateException("503 from provider");
            return Completion.ofText("answer");
        };
        policy = CallPolicy.retries(3, Duration.ofMillis(1));
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log, flaky, CallPolicy.none())) {
            var run = runtime.execute(new UsesModel(), ExecutionOptions.none());
            assertThat(run.result(Duration.ofSeconds(10))).isEqualTo("answer");
            assertThat(calls.get()).isEqualTo(3);
            assertThat(count(log, run.id(), CatalystEvent.CompletionRequested.class)).isEqualTo(1);
            assertThat(count(log, run.id(), CatalystEvent.CompletionReceived.class)).isEqualTo(1);
            assertThat(runtime.replay(run.id(), new UsesModel()).status()).isEqualTo(Status.COMPLETED);
            assertThat(calls.get()).as("a replay makes no model call").isEqualTo(3);
        }
    }

    @Test
    void aHungCallTimesOutAndCanBeRetried() {
        hangFirst = 1;
        policy = CallPolicy.retries(1, Duration.ofMillis(1)).timeout(Duration.ofMillis(200));
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log, MockModel.alwaysReturn("M"), CallPolicy.none())) {
            long t0 = System.nanoTime();
            var run = runtime.execute(new UsesTool(), ExecutionOptions.none());
            assertThat(run.result(Duration.ofSeconds(10))).isEqualTo("done:x");
            assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofSeconds(5));
            assertThat(log.read(run.id()).stream().map(se -> se.event())
                    .filter(e -> e instanceof CatalystEvent.CallRetried).map(e -> ((CatalystEvent.CallRetried) e).error()).toList())
                    .singleElement().asString().contains(CallTimeoutException.class.getSimpleName());
        }
    }

    @Test
    void aTimeoutWithoutRetriesSurfacesAsTheCallsFailure() {
        hangFirst = 1;
        policy = CallPolicy.timeoutAfter(Duration.ofMillis(200));
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log, MockModel.alwaysReturn("M"), CallPolicy.none())) {
            var run = runtime.execute(new UsesTool(), ExecutionOptions.none());
            assertThat(run.result(Duration.ofSeconds(10))).startsWith("failed: tool flaky did not finish within");
        }
    }

    @Test
    void aStreamIsRetriedOnlyBeforeAnyChunkWasDelivered() {
        AtomicInteger calls = new AtomicInteger();
        Model failsBeforeAnyChunk = new Model() {
            @Override public Completion complete(CompletionRequest request) {
                return Completion.ofText("whole");
            }
            @Override public Completion stream(CompletionRequest request, com.cajunsystems.catalyst.model.TokenSink sink) {
                if (calls.incrementAndGet() == 1) throw new IllegalStateException("connect failed");
                sink.accept("wh");
                sink.accept("ole");
                return Completion.ofText("whole");
            }
        };
        policy = CallPolicy.retries(3, Duration.ofMillis(1));
        try (CatalystRuntime runtime = runtime(EventLogs.inMemory(), failsBeforeAnyChunk, CallPolicy.none())) {
            assertThat(runtime.execute(new Streams(), ExecutionOptions.none()).result(Duration.ofSeconds(10))).isEqualTo("whole|whole");
            assertThat(calls.get()).isEqualTo(2);
        }

        AtomicInteger midway = new AtomicInteger();
        Model failsMidStream = new Model() {
            @Override public Completion complete(CompletionRequest request) {
                return Completion.ofText("whole");
            }
            @Override public Completion stream(CompletionRequest request, com.cajunsystems.catalyst.model.TokenSink sink) {
                midway.incrementAndGet();
                sink.accept("wh");
                throw new IllegalStateException("connection reset");
            }
        };
        try (CatalystRuntime runtime = runtime(EventLogs.inMemory(), failsMidStream, CallPolicy.none())) {
            assertThat(runtime.execute(new Streams(), ExecutionOptions.none()).result(Duration.ofSeconds(10)))
                    .isEqualTo("failed after [wh]: connection reset");
            assertThat(midway.get()).as("re-delivering text would corrupt what the task assembled").isEqualTo(1);
        }
    }

    @Test
    void aCancelStopsTheRetriesPromptly() {
        failFirst = 100;
        EventLog log = EventLogs.inMemory();
        try (CatalystRuntime runtime = runtime(log, MockModel.alwaysReturn("M"), CallPolicy.retries(5, Duration.ofSeconds(5)))) {
            var run = runtime.execute(new UsesToolDefault(), ExecutionOptions.none());
            await(() -> invocations.get() == 1);
            long t0 = System.nanoTime();
            runtime.cancel(run.id());
            await(() -> runtime.inspect(run.id()).status() == Status.CANCELLED);
            assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofSeconds(3));
            assertThat(invocations.get()).isEqualTo(1);
            assertThat(log.read(run.id()).stream().map(se -> se.event())
                    .filter(e -> e instanceof CatalystEvent.ToolCompleted).map(e -> ((CatalystEvent.ToolCompleted) e).error()).toList())
                    .as("the call's real failure is recorded, not the cancel").singleElement().asString().contains("transient 1");
        }
    }

    private static void await(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError("condition not met within 10s");
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }
}
