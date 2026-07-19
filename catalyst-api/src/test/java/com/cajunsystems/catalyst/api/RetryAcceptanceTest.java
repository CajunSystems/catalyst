package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.Tool;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.RetryPolicy;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.cajunsystems.catalyst.runtime.ExecutionHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * v0.2 retry exit demo (roadmap §13.3): a transient tool failure is retried as a new attempt on the
 * same durable stream. The failing boundary re-runs live while the successful prefix (the model call)
 * is substituted, bounded by the policy — and the retried log still replays exactly. Retry is opt-in:
 * the default {@link RetryPolicy#none()} leaves the failure terminal. Exercised end-to-end over a real
 * {@link GumboEventLog}.
 */
class RetryAcceptanceTest {

    record In(String v) {}

    /** Fails its first {@code failures} live invocations, then succeeds; counts invocations. */
    static final class FlakyTool implements Tool<In, String> {
        final AtomicInteger invocations = new AtomicInteger();
        private final int failures;
        FlakyTool(int failures) { this.failures = failures; }
        public String name() { return "flaky"; }
        public Class<In> inputType() { return In.class; }
        public String apply(In in) {
            if (invocations.incrementAndGet() <= failures) throw new RuntimeException("transient");
            return "ok:" + in.v();
        }
    }

    private static Task<String> modelThenFlaky(FlakyTool tool) {
        return ctx -> {
            String m = ctx.model().complete(
                    CompletionRequest.of(Prompt.builder().user("go").build())).message();
            return m + "|" + ctx.call(tool, new In("x"));
        };
    }

    @Test
    void transientFailureRecoversPrefixSubstitutedAndReplaysExactly(@TempDir Path dir) {
        MockModel model = MockModel.alwaysReturn("M");
        FlakyTool tool = new FlakyTool(2); // fails twice, then succeeds
        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir))
                .model(model)
                .retryPolicy(RetryPolicy.maxRetries(3, Duration.ofMillis(5)))
                .build()) {

            ExecutionHandle<String> handle = runtime.execute(modelThenFlaky(tool), ExecutionOptions.withKey("k"));
            String result = handle.result();
            ExecutionState state = runtime.inspect(handle.id());

            assertThat(result).isEqualTo("M|ok:x");
            assertThat(state.status()).isEqualTo(Status.COMPLETED);
            assertThat(state.retries()).as("two failure retries folded").isEqualTo(2);
            assertThat(tool.invocations).as("failing boundary re-ran live: 2 fail + 1 ok").hasValue(3);
            assertThat(model.callCount()).as("successful prefix substituted, not re-run").isEqualTo(1);

            // The retried log still replays exactly — no boundary re-executes.
            int modelAfter = model.callCount();
            int toolAfter = tool.invocations.get();
            ExecutionState replayed = runtime.replay(handle.id(), modelThenFlaky(tool));
            assertThat(replayed.status()).isEqualTo(Status.COMPLETED);
            assertThat(model.callCount()).as("no live model call during replay").isEqualTo(modelAfter);
            assertThat(tool.invocations).as("no live tool call during replay").hasValue(toolAfter);
        }
    }

    @Test
    void defaultPolicyLeavesTheFailureTerminal(@TempDir Path dir) {
        MockModel model = MockModel.alwaysReturn("M");
        FlakyTool tool = new FlakyTool(2);
        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir))
                .model(model)
                .build()) { // no retry policy → none()

            ExecutionHandle<String> handle = runtime.execute(modelThenFlaky(tool), ExecutionOptions.withKey("k"));
            catchThrowable(handle::result);

            assertThat(runtime.inspect(handle.id()).status()).isEqualTo(Status.FAILED);
            assertThat(tool.invocations).as("not retried under the default policy").hasValue(1);
        }
    }
}
