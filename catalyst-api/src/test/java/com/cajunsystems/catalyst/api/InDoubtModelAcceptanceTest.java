package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.InDoubtException;
import com.cajunsystems.catalyst.engine.InDoubtPolicy;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.CatalystEvent.*;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.Hashing;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The v1 in-doubt-model-completions exit criterion, over a durable Gumbo log.
 *
 * <p>A node that dies between {@code CompletionRequested} and {@code CompletionReceived} leaves the
 * provider call <em>in doubt</em>: it may have been accepted, produced and billed, but no result
 * reached the log. Catalyst has always routed the equivalent tool window through
 * {@link InDoubtPolicy}; model completions had no counterpart, so a resume silently re-issued the
 * call — and left a second {@code CompletionRequested} orphaned in the stream. This closes that
 * asymmetry, which is what "zero duplicate model calls" needs in order to hold under node failure
 * rather than only under graceful resume.
 */
class InDoubtModelAcceptanceTest {

    private static final Instant T = Instant.EPOCH;
    private static final String KEY = "incident:7";
    private static final CompletionRequest REQUEST =
            CompletionRequest.of(Prompt.builder().user("summarise the incident report").build());

    /** A named task: a lambda's synthetic class name is not stable across processes. */
    static final class SummariseOnce implements Task<String> {
        public String execute(Context ctx) {
            return ctx.model().complete(REQUEST).message();
        }
    }

    /**
     * Writes the log a node death mid-completion leaves behind: the request is durable, the response
     * never arrived. Nothing after it — not even a failure — because the process did not survive to
     * record one.
     */
    private static ExecutionId crashMidCompletion(Path dir) {
        ExecutionId id = ExecutionId.random();
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            log.putKey(KEY, id);
            log.append(id, new ExecutionCreated(T, SummariseOnce.class.getName(), "h", "cfg", KEY));
            log.append(id, new ExecutionStarted(T, 1, "node-a"));
            log.append(id, new PromptBuilt(T, "ph", new TextNode("summarise the incident report")));
            log.append(id, new CompletionRequested(T, Hashing.canonicalRequestHash(REQUEST)));
        }
        return id;
    }

    private static long count(CatalystRuntime runtime, ExecutionId id, Class<? extends CatalystEvent> type) {
        return runtime.log().read(id).stream().filter(se -> type.isInstance(se.event())).count();
    }

    @Test
    void theDefaultPolicyRefusesToSilentlyPayTwice(@TempDir Path dir) {
        ExecutionId id = crashMidCompletion(dir);
        MockModel model = MockModel.alwaysReturn("a second, billable completion");

        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir)).model(model).task(new SummariseOnce()).build()) {

            assertThatThrownBy(() -> runtime.resume(id).result())
                    .isInstanceOf(InDoubtException.class);

            assertThat(model.callCount())
                    .as("the in-doubt call is surfaced, not re-issued")
                    .isZero();
            assertThat(runtime.inspect(id).status()).isEqualTo(Status.FAILED);
            assertThat(count(runtime, id, CompletionRequested.class))
                    .as("no orphaned second request")
                    .isEqualTo(1);
        }
    }

    @Test
    void retryReissuesOnceAndTheRepairedLogStillReplaysExactly(@TempDir Path dir) {
        ExecutionId id = crashMidCompletion(dir);
        MockModel model = MockModel.alwaysReturn("recovered summary");

        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir)).model(model)
                .inDoubtPolicy(InDoubtPolicy.RETRY).task(new SummariseOnce()).build()) {

            assertThat(runtime.resume(id).result()).isEqualTo("recovered summary");
            assertThat(model.callCount()).as("re-issued exactly once, by explicit opt-in").isEqualTo(1);
            assertThat(runtime.inspect(id).status()).isEqualTo(Status.COMPLETED);

            // Completed in place: the dangling request is answered rather than replaced, so the stream
            // has the same shape as one that never crashed.
            assertThat(count(runtime, id, CompletionRequested.class)).isEqualTo(1);
            assertThat(count(runtime, id, CompletionReceived.class)).isEqualTo(1);

            // And that shape is what makes it replayable: a strict replay of the repaired log contacts
            // no provider. Recovery produced a well-formed stream, not merely a finished one.
            int before = model.callCount();
            ExecutionState replayed = runtime.replay(id, new SummariseOnce());
            assertThat(replayed.status()).isEqualTo(Status.COMPLETED);
            assertThat(model.callCount() - before).as("zero external calls on replay").isZero();
        }
    }

    @Test
    void askPausesForAnOperatorInsteadOfDeciding(@TempDir Path dir) {
        ExecutionId id = crashMidCompletion(dir);
        MockModel model = MockModel.alwaysReturn("never called");

        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir)).model(model)
                .inDoubtPolicy(InDoubtPolicy.ASK).task(new SummariseOnce()).build()) {

            assertThatThrownBy(() -> runtime.resume(id).result());

            assertThat(model.callCount()).isZero();
            assertThat(runtime.inspect(id).status()).isEqualTo(Status.PAUSED);
        }
    }

    @Test
    void aCompletionThatReachedTheLogIsUnaffected(@TempDir Path dir) {
        // The control: when the response was recorded, nothing is in doubt and the M0 guarantee holds
        // by substitution exactly as before. The in-doubt path must not touch this case.
        ExecutionId id = crashMidCompletion(dir);
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            log.append(id, new CompletionReceived(T,
                    com.cajunsystems.catalyst.events.EventJson.shared()
                            .valueToTree(com.cajunsystems.catalyst.model.Completion.ofText("recorded summary")),
                    1, 1, 5, 0.0, "stop"));
        }
        MockModel model = MockModel.alwaysReturn("should not be called");

        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir)).model(model).task(new SummariseOnce()).build()) {

            assertThat(runtime.resume(id).result()).isEqualTo("recorded summary");
            assertThat(model.callCount()).isZero();
        }
    }
}
