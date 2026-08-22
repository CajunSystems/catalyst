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
import com.cajunsystems.catalyst.model.Model;
import com.cajunsystems.catalyst.model.Prompt;
import com.fasterxml.jackson.databind.node.TextNode;
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
 * In-doubt <em>model completions</em>: a {@code CompletionRequested} with no {@code CompletionReceived}
 * is a provider call that was in flight when the process died. The provider may have accepted, produced
 * and billed it, so a resume must not silently re-issue it — it routes through {@link InDoubtPolicy},
 * closing the asymmetry with the tool side (which has always done this).
 */
class ReplayingContextInDoubtModelTest {

    private final ExecutionId id = ExecutionId.of("exec-1");
    private static final Instant T = Instant.EPOCH;

    private static final CompletionRequest REQUEST =
            CompletionRequest.of(Prompt.builder().user("hello").build());
    private static final CompletionRequest OTHER_REQUEST =
            CompletionRequest.of(Prompt.builder().user("a different question").build());

    /** A model that counts how many times a provider call actually happened. */
    private static final class CountingModel implements Model {
        final AtomicInteger calls = new AtomicInteger();
        public Completion complete(CompletionRequest request) {
            calls.incrementAndGet();
            return Completion.ofText("fresh");
        }
    }

    /** The log tail a crash between request and response leaves behind. */
    private static List<SequencedEvent> crashedMidCompletion() {
        return seq(new PromptBuilt(T, "ph", new TextNode("hello")),
                new CompletionRequested(T, Hashing.canonicalRequestHash(REQUEST)),
                new ExecutionResumed(T, 2));
    }

    @Test
    void anInFlightCompletionIsNotSilentlyReissued() {
        // The regression this whole change exists for: before it, seed() dropped the pending request and
        // the resume called the provider again — a second billable completion, with nothing in the log
        // to show it happened.
        CountingModel model = new CountingModel();

        assertThatThrownBy(() -> newContext(crashedMidCompletion(), model, InDoubtPolicy.FAIL)
                .model().complete(REQUEST))
                .isInstanceOf(InDoubtException.class)
                .hasMessageContaining("In-doubt model completion");

        assertThat(model.calls).as("in-doubt under FAIL — the provider is not called again").hasValue(0);
    }

    @Test
    void retryPolicyReissuesAndCompletesTheDanglingRequestInPlace() {
        CountingModel model = new CountingModel();
        InMemoryLog log = new InMemoryLog(crashedMidCompletion());

        Completion out = newContext(log, model, InDoubtPolicy.RETRY).model().complete(REQUEST);

        assertThat(out.message()).isEqualTo("fresh");
        assertThat(model.calls).as("RETRY is the policy that accepts the duplicate-spend risk").hasValue(1);

        // Only the missing half is appended: the recorded PromptBuilt/CompletionRequested pair still
        // stands, so the repaired stream pairs one request with one response and replays like a log
        // that never crashed.
        List<CatalystEvent> appended = log.appendedAfter(3);
        assertThat(appended).singleElement().isInstanceOf(CompletionReceived.class);
    }

    @Test
    void askPolicyPausesForAnOperatorRatherThanDeciding() {
        CountingModel model = new CountingModel();
        InMemoryLog log = new InMemoryLog(crashedMidCompletion());

        assertThatThrownBy(() -> newContext(log, model, InDoubtPolicy.ASK).model().complete(REQUEST))
                .isInstanceOf(ExecutionPausedSignal.class);

        assertThat(model.calls).hasValue(0);
        assertThat(log.appendedAfter(3)).singleElement().isInstanceOf(ExecutionPaused.class);
    }

    @Test
    void aDifferentRequestAtTheInDoubtBoundaryIsADivergenceNotADecision() {
        // The task reached a model boundary, but not the one that was in flight. The recorded request
        // can no longer be completed by this call, so no policy applies — this is nondeterministic task
        // code, and reporting it as in-doubt would hide the real fault.
        CountingModel model = new CountingModel();

        assertThatThrownBy(() -> newContext(crashedMidCompletion(), model, InDoubtPolicy.RETRY)
                .model().complete(OTHER_REQUEST))
                .isInstanceOf(NonDeterministicReplayException.class);

        assertThat(model.calls).hasValue(0);
    }

    @Test
    void aCompletedCompletionIsSubstitutedAndNeverInDoubt() {
        CountingModel model = new CountingModel();
        List<SequencedEvent> recorded = seq(
                new PromptBuilt(T, "ph", new TextNode("hello")),
                new CompletionRequested(T, Hashing.canonicalRequestHash(REQUEST)),
                new CompletionReceived(T, EventJson.shared().valueToTree(Completion.ofText("recorded")),
                        1, 1, 5, 0.0, "stop"));

        Completion out = newContext(recorded, model, InDoubtPolicy.FAIL).model().complete(REQUEST);

        assertThat(out.message()).as("substituted from the log").isEqualTo("recorded");
        assertThat(model.calls).hasValue(0);
    }

    @Test
    void aRetriedModelFailureRunsLiveInsteadOfBecomingInDoubt() {
        // A provider call that threw leaves the same trace as one that crashed — no CompletionReceived,
        // because a failed completion records no result. The RetryRequested is what distinguishes them:
        // the process survived to record its retry decision, so the RetryPolicy owns this boundary. Were
        // the pending request left dangling, the default FAIL policy would convert every model retry into
        // an InDoubtException — this is the guard for that.
        CountingModel model = new CountingModel();
        List<SequencedEvent> recorded = seq(
                new PromptBuilt(T, "ph", new TextNode("hello")),
                new CompletionRequested(T, Hashing.canonicalRequestHash(REQUEST)),
                new RetryRequested(T, "provider exploded", 10, -1),
                new ExecutionResumed(T, 2));

        Completion out = newContext(recorded, model, InDoubtPolicy.FAIL).model().complete(REQUEST);

        assertThat(out.message()).isEqualTo("fresh");
        assertThat(model.calls).as("re-run live by the retry, not surfaced as in-doubt").hasValue(1);
    }

    @Test
    void aBranchSupersedesAnInDoubtCompletionFromTheRecordedTail() {
        // Forking abandons the recorded tail, so the in-doubt request goes with it: the branch is a new
        // line of history and must not inherit the old one's unfinished business.
        CountingModel model = new CountingModel();
        List<SequencedEvent> recorded = seq(
                new EffectRecorded(T, "label", new TextNode("recorded")),
                new CompletionRequested(T, Hashing.canonicalRequestHash(REQUEST)));

        ReplayingContext ctx = newContext(new InMemoryLog(recorded), model, InDoubtPolicy.FAIL,
                ReplayMode.BRANCH, recorded);
        ctx.effect("diverged", () -> "live"); // structural divergence → fork
        Completion out = ctx.model().complete(REQUEST);

        assertThat(out.message()).isEqualTo("fresh");
        assertThat(model.calls).as("live on the branch, not an in-doubt decision").hasValue(1);
    }

    // ── harness ────────────────────────────────────────────────────────────────

    private ReplayingContext newContext(List<SequencedEvent> recorded, Model model, InDoubtPolicy policy) {
        return newContext(new InMemoryLog(recorded), model, policy, ReplayMode.STRICT, recorded);
    }

    private ReplayingContext newContext(InMemoryLog log, Model model, InDoubtPolicy policy) {
        return newContext(log, model, policy, ReplayMode.STRICT, log.read(id));
    }

    private ReplayingContext newContext(InMemoryLog log, Model model, InDoubtPolicy policy,
                                        ReplayMode mode, List<SequencedEvent> recorded) {
        return new ReplayingContext(id, log, model,
                new ExecutionInfo(id, 2, "TaskA", Map.of()), Map.of(),
                EventJson.shared(), new PayloadCodec(), policy, CostModel.free(),
                mode, null, Clock.systemUTC(), org.slf4j.LoggerFactory.getLogger("test"),
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
        /** Events appended past the seeded prefix of {@code seededSize} entries. */
        List<CatalystEvent> appendedAfter(int seededSize) {
            return events.stream().skip(seededSize).map(SequencedEvent::event).toList();
        }
    }
}
