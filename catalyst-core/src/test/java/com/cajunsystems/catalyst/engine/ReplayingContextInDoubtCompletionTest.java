package com.cajunsystems.catalyst.engine;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionInfo;
import com.cajunsystems.catalyst.ReplayMode;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.CatalystEvent.*;
import com.cajunsystems.catalyst.events.EventJson;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Message;
import com.cajunsystems.catalyst.model.Model;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.model.Role;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A provider call that was requested and never received is <em>in doubt</em>: it may have been
 * accepted and billed with only the result lost on the way back.
 *
 * <p>Catalyst has always treated a tool this way and did not treat a model call this way at all — a
 * trailing {@code CompletionRequested} set the pending request hash and was otherwise ignored, so
 * the resume simply called the provider again. That asymmetry is what these tests close, on the one
 * boundary where getting it wrong costs money rather than time.
 */
class ReplayingContextInDoubtCompletionTest {

    private final ExecutionId id = ExecutionId.of("exec-1");
    private static final Instant T = Instant.EPOCH;
    private static final CompletionRequest REQUEST =
            CompletionRequest.of(new Prompt(List.of(new Message(Role.USER, "why?"))));
    private static final CompletionRequest OTHER_REQUEST =
            CompletionRequest.of(new Prompt(List.of(new Message(Role.USER, "something else entirely"))));

    /** A model that counts how many times a provider would actually have been called. */
    private static final class CountingModel implements Model {
        final AtomicInteger calls = new AtomicInteger();
        @Override public Completion complete(CompletionRequest request) {
            calls.incrementAndGet();
            return Completion.ofText("fresh answer");
        }
    }

    /**
     * The measured bug. Before this change the same log resumed and called the provider a second
     * time, with nothing recorded to say it had happened once already.
     */
    @Test
    void aCompletionRequestedWithNoResultIsInDoubtRatherThanReIssued() {
        List<SequencedEvent> recorded = seq(
                new PromptBuilt(T, "ph", null),
                new CompletionRequested(T, hashOf(REQUEST)));

        CountingModel model = new CountingModel();
        assertThatThrownBy(() -> newContext(recorded, model, InDoubtPolicy.FAIL).model().complete(REQUEST))
                .isInstanceOf(InDoubtException.class)
                .hasMessageContaining("billed");

        assertThat(model.calls).as("the provider is not called again under FAIL").hasValue(0);
    }

    /**
     * Under RETRY the call is re-issued once and <em>completes the request already in the log</em>
     * rather than opening a second one. That matters beyond tidiness: the recovered log has to be
     * one request paired with one result, or it no longer replays like an execution that never
     * crashed.
     */
    @Test
    void aRetryCompletesTheDanglingRequestInsteadOfOpeningANewOne() {
        List<SequencedEvent> recorded = seq(
                new PromptBuilt(T, "ph", null),
                new CompletionRequested(T, hashOf(REQUEST)));

        CountingModel model = new CountingModel();
        InMemoryLog log = new InMemoryLog(recorded);
        Completion completion = newContext(log, recorded, model, InDoubtPolicy.RETRY).model().complete(REQUEST);

        assertThat(completion.message()).isEqualTo("fresh answer");
        assertThat(model.calls).as("re-issued exactly once").hasValue(1);
        assertThat(log.read(id)).extracting(se -> se.event().getClass().getSimpleName())
                .as("one request, one result — no second CompletionRequested")
                .containsExactly("PromptBuilt", "CompletionRequested", "CompletionReceived");
    }

    @Test
    void askPausesAndRecordsWhyRatherThanDeciding() {
        List<SequencedEvent> recorded = seq(new CompletionRequested(T, hashOf(REQUEST)));

        CountingModel model = new CountingModel();
        InMemoryLog log = new InMemoryLog(recorded);
        assertThatThrownBy(() -> newContext(log, recorded, model, InDoubtPolicy.ASK).model().complete(REQUEST))
                .isInstanceOf(ExecutionPausedSignal.class);

        assertThat(model.calls).hasValue(0);
        assertThat(log.read(id)).last()
                .satisfies(se -> assertThat(se.event()).isInstanceOfSatisfying(ExecutionPaused.class,
                        p -> assertThat(p.reason()).contains("in-doubt model completion")));
    }

    /**
     * Recovering an in-doubt call only means anything if it is the same call. A task that arrives
     * here asking something else has diverged, and attaching the recovery to the wrong question
     * would be worse than either policy outcome.
     */
    @Test
    void aDifferentRequestAtTheInDoubtBoundaryIsADivergence() {
        List<SequencedEvent> recorded = seq(new CompletionRequested(T, hashOf(REQUEST)));

        CountingModel model = new CountingModel();
        assertThatThrownBy(() ->
                newContext(recorded, model, InDoubtPolicy.RETRY).model().complete(OTHER_REQUEST))
                .isInstanceOf(NonDeterministicReplayException.class);
        assertThat(model.calls).hasValue(0);
    }

    /**
     * The regression this feature could most easily cause. A model call that <em>threw</em> records
     * nothing, so it also leaves a `CompletionRequested` with no result — but the process lived long
     * enough to append a `RetryRequested`, which is a verdict, not a doubt. Retry-as-attempt re-runs
     * that boundary live by design, and it must keep doing so: under FAIL, treating it as in-doubt
     * would turn every retried model failure into an InDoubtException.
     */
    @Test
    void aModelFailureThatWasRetriedRunsLiveAndIsNotInDoubt() {
        List<SequencedEvent> recorded = seq(
                new PromptBuilt(T, "ph", null),
                new CompletionRequested(T, hashOf(REQUEST)),
                new RetryRequested(T, "provider timeout", 10, -1));

        CountingModel model = new CountingModel();
        Completion completion = newContext(recorded, model, InDoubtPolicy.FAIL).model().complete(REQUEST);

        assertThat(completion.message()).isEqualTo("fresh answer");
        assertThat(model.calls).as("the retry re-runs the boundary live").hasValue(1);
    }

    /** A completed boundary is substituted as before — the in-doubt path is not on the happy road. */
    @Test
    void aCompletedBoundaryStillSubstitutesWithoutCallingTheProvider() {
        CountingModel model = new CountingModel();
        List<SequencedEvent> recorded = seq(
                new CompletionRequested(T, hashOf(REQUEST)),
                new CompletionReceived(T, EventJson.shared().valueToTree(Completion.ofText("recorded answer")),
                        1, 2, 3, 0.0, "stop"));

        Completion completion = newContext(recorded, model, InDoubtPolicy.FAIL).model().complete(REQUEST);

        assertThat(completion.message()).isEqualTo("recorded answer");
        assertThat(model.calls).hasValue(0);
    }

    private static String hashOf(CompletionRequest request) {
        return Hashing.canonicalRequestHash(request);
    }

    private ReplayingContext newContext(List<SequencedEvent> recorded, Model model, InDoubtPolicy policy) {
        return newContext(new InMemoryLog(recorded), recorded, model, policy);
    }

    private ReplayingContext newContext(EventLog log, List<SequencedEvent> recorded, Model model,
                                        InDoubtPolicy policy) {
        return new ReplayingContext(id, log, model,
                new ExecutionInfo(id, 2, "TaskA", Map.of()), Map.of(),
                EventJson.shared(), new PayloadCodec(), policy, CostModel.free(),
                ReplayMode.STRICT, null, Clock.systemUTC(), org.slf4j.LoggerFactory.getLogger("test"),
                recorded, /* appendEnabled */ true);
    }

    private static List<SequencedEvent> seq(CatalystEvent... events) {
        List<SequencedEvent> out = new ArrayList<>();
        for (int i = 0; i < events.length; i++) out.add(new SequencedEvent(i, events[i]));
        return out;
    }

    /** A minimal append-only log: seeds from a recorded prefix and appends past its tail. */
    private static final class InMemoryLog implements EventLog {
        private final List<SequencedEvent> events;
        InMemoryLog(List<SequencedEvent> seed) { this.events = new ArrayList<>(seed); }
        public long append(ExecutionId executionId, CatalystEvent event) {
            long seq = events.size();
            events.add(new SequencedEvent(seq, event));
            return seq;
        }
        public List<SequencedEvent> read(ExecutionId executionId) { return List.copyOf(events); }
        public long latestSeq(ExecutionId executionId) { return events.size() - 1; }
        public Optional<ExecutionId> findByKey(String idempotencyKey) { return Optional.empty(); }
        public void putKey(String idempotencyKey, ExecutionId executionId) { }
        public void close() { }
    }
}
