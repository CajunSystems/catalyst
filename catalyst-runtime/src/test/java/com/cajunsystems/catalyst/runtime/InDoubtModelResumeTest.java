package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.Hashing;
import com.cajunsystems.catalyst.engine.InDoubtException;
import com.cajunsystems.catalyst.engine.InDoubtPolicy;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.CatalystEvent.*;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The crash window this closes, exercised through the real {@code resume(id)} path rather than a
 * hand-seeded context: a node dies between {@code CompletionRequested} and {@code CompletionReceived},
 * so the provider may have produced and billed a completion the log never saw.
 *
 * <p>The measurement that motivated this (recorded in {@code docs/distribution.md}) was that such a
 * resume called the model a second time, silently. "Zero duplicate model calls" held under graceful
 * resume but not under node failure, even though the tool side had routed the same window through
 * {@link InDoubtPolicy} since M0.
 */
class InDoubtModelResumeTest {

    private static final Instant T = Instant.EPOCH;
    private static final CompletionRequest REQUEST =
            CompletionRequest.of(Prompt.builder().user("hi").build());

    /** A named task (a lambda's class name is not stable across processes) that makes one model call. */
    static final class OneCompletion implements Task<String> {
        public String execute(com.cajunsystems.catalyst.Context ctx) {
            return ctx.model().complete(REQUEST).message();
        }
    }

    /**
     * Hand-builds the log a crash mid-completion leaves: created, started, the prompt and the request —
     * and then nothing. This is the shape the distribution probe measured; it cannot be produced by
     * running a task in-process, because the runtime would survive to record the outcome.
     */
    private static ExecutionId crashedMidCompletion(InMemoryEventLog log, String configFingerprint) {
        ExecutionId id = ExecutionId.random();
        log.append(id, new ExecutionCreated(T, OneCompletion.class.getName(), "args", configFingerprint, null));
        log.append(id, new ExecutionStarted(T, 1, "node-a"));
        log.append(id, new PromptBuilt(T, "ph", new TextNode("hi")));
        log.append(id, new CompletionRequested(T, Hashing.canonicalRequestHash(REQUEST)));
        return id;
    }

    private static long count(InMemoryEventLog log, ExecutionId id, Class<? extends CatalystEvent> type) {
        return log.read(id).stream().filter(se -> type.isInstance(se.event())).count();
    }

    @Test
    void theDefaultPolicySurfacesTheInDoubtCompletionInsteadOfReissuingIt() {
        MockModel model = MockModel.alwaysReturn("second call");
        InMemoryEventLog log = new InMemoryEventLog();
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(log).model(model).task(new OneCompletion()).build()) {
            ExecutionId id = crashedMidCompletion(log, "replayMode=STRICT;inDoubt=FAIL");

            Throwable t = catchThrowable(() -> runtime.resume(id).result());

            assertThat(t).isInstanceOf(InDoubtException.class);
            assertThat(model.callCount())
                    .as("the accepted-but-unrecorded call is NOT re-issued")
                    .isZero();
            assertThat(runtime.inspect(id).status()).isEqualTo(Status.FAILED);
        }
    }

    @Test
    void retryPolicyReissuesOnceAndTheRepairedLogPairsOneRequestWithOneResponse() {
        MockModel model = MockModel.alwaysReturn("recovered");
        InMemoryEventLog log = new InMemoryEventLog();
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(log).model(model).inDoubtPolicy(InDoubtPolicy.RETRY)
                .task(new OneCompletion()).build()) {
            ExecutionId id = crashedMidCompletion(log, "replayMode=STRICT;inDoubt=RETRY");

            ExecutionHandle<?> h = runtime.resume(id);

            assertThat(h.result()).isEqualTo("recovered");
            assertThat(model.callCount()).as("re-issued exactly once, by explicit policy").isEqualTo(1);
            assertThat(runtime.inspect(id).status()).isEqualTo(Status.COMPLETED);

            // The dangling request is completed in place rather than re-requested, so the repaired
            // stream has the same one-request-one-response shape as a log that never crashed — which is
            // what lets it replay like one.
            assertThat(count(log, id, CompletionRequested.class)).isEqualTo(1);
            assertThat(count(log, id, CompletionReceived.class)).isEqualTo(1);
        }
    }

    @Test
    void askPolicyPausesTheExecutionForAnOperator() {
        MockModel model = MockModel.alwaysReturn("never called");
        InMemoryEventLog log = new InMemoryEventLog();
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(log).model(model).inDoubtPolicy(InDoubtPolicy.ASK)
                .task(new OneCompletion()).build()) {
            ExecutionId id = crashedMidCompletion(log, "replayMode=STRICT;inDoubt=ASK");

            catchThrowable(() -> runtime.resume(id).result());

            assertThat(model.callCount()).isZero();
            assertThat(runtime.inspect(id).status()).isEqualTo(Status.PAUSED);
            assertThat(count(log, id, ExecutionPaused.class)).isEqualTo(1);
        }
    }

    @Test
    void aGracefulResumeWithTheCompletionRecordedStillSubstitutesIt() {
        // The control: when the completion did reach the log, nothing is in doubt and the M0 guarantee
        // holds by substitution exactly as before. This is the case the in-doubt path must not disturb.
        MockModel model = MockModel.alwaysReturn("should not be called");
        InMemoryEventLog log = new InMemoryEventLog();
        try (CatalystRuntime runtime = CatalystRuntime.builder()
                .log(log).model(model).task(new OneCompletion()).build()) {
            ExecutionId id = crashedMidCompletion(log, "replayMode=STRICT;inDoubt=FAIL");
            log.append(id, new CompletionReceived(T,
                    com.cajunsystems.catalyst.events.EventJson.shared()
                            .valueToTree(com.cajunsystems.catalyst.model.Completion.ofText("recorded")),
                    1, 1, 5, 0.0, "stop"));

            assertThat(runtime.resume(id).result()).isEqualTo("recorded");
            assertThat(model.callCount()).isZero();
        }
    }
}
