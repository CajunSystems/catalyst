package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.StreamCursor;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.EventLog;
import com.cajunsystems.catalyst.log.StaleWriterException;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Dist-1: the runtime writes through the log's fence. Two runtimes over one log are two writers — the
 * situation a lease handover produces when the old owner is not quite dead. The one whose view of the
 * stream is stale has its next append refused, and records nothing on its way out.
 */
class FencedWritesTest {

    private static final Instant T = Instant.parse("2026-01-01T00:00:00Z");
    private static final CompletionRequest STEP1 = CompletionRequest.of(Prompt.builder().user("step one").build());
    private static final CompletionRequest STEP2 = CompletionRequest.of(Prompt.builder().user("step two").build());

    /** Two steps with an optional gate between them, where a zombie can stall. */
    static final class GatedTask implements Task<String> {
        private final CountDownLatch reachedGate;
        private final CountDownLatch openGate;
        private final boolean swallowFailures;

        GatedTask(CountDownLatch reachedGate, CountDownLatch openGate, boolean swallowFailures) {
            this.reachedGate = reachedGate;
            this.openGate = openGate;
            this.swallowFailures = swallowFailures;
        }

        /** The ungated instance a healthy runtime reconstructs from the registry. */
        GatedTask() {
            this(null, null, false);
        }

        @Override public String execute(Context ctx) throws Exception {
            String one = ctx.model().complete(STEP1).message();
            if (openGate != null) {
                reachedGate.countDown();
                openGate.await();
            }
            try {
                return one + "|" + ctx.model().complete(STEP2).message();
            } catch (RuntimeException e) {
                if (swallowFailures) return one + "|fallback";
                throw e;
            }
        }
    }

    @Test
    void aZombieAttemptIsFencedOutAndRecordsNothing() throws Exception {
        assertZombieIsFencedOut(false);
    }

    /** Even a task that swallows the rejection and returns normally gets nothing recorded. */
    @Test
    void aZombieThatSwallowsTheRejectionStillRecordsNothing() throws Exception {
        assertZombieIsFencedOut(true);
    }

    private void assertZombieIsFencedOut(boolean swallow) throws Exception {
        InMemoryEventLog log = new InMemoryEventLog();
        assertThat(log.supportsConditionalAppend()).isTrue();
        CountDownLatch reachedGate = new CountDownLatch(1);
        CountDownLatch openGate = new CountDownLatch(1);

        CatalystRuntime zombie = CatalystRuntime.builder().log(log).model(MockModel.alwaysReturn("Z")).build();
        CatalystRuntime owner = CatalystRuntime.builder().log(log).model(MockModel.alwaysReturn("B"))
                .task(GatedTask.class, GatedTask::new).build();
        try {
            ExecutionHandle<String> stalled = zombie.execute(
                    new GatedTask(reachedGate, openGate, swallow), ExecutionOptions.withKey("job-1"));
            assertThat(reachedGate.await(5, TimeUnit.SECONDS)).isTrue();
            ExecutionId id = stalled.id();

            // The stream is taken over and finished elsewhere while the first writer is stalled.
            assertThat(owner.resume(id).result()).isEqualTo("Z|B");
            List<SequencedEvent> afterTakeover = log.read(id);

            openGate.countDown();
            assertThatThrownBy(stalled::result).isInstanceOf(StaleWriterException.class);

            assertThat(log.read(id)).as("the zombie appended nothing — not even ExecutionFailed")
                    .isEqualTo(afterTakeover);
            assertThat(owner.inspect(id).status()).isEqualTo(Status.COMPLETED);
            assertThat(owner.replay(id, new GatedTask()).status()).as("the owner's history replays exactly")
                    .isEqualTo(Status.COMPLETED);
        } finally {
            openGate.countDown();
            owner.close();
            zombie.close();
        }
    }

    /** The cursor's contract: fenced on what it read, and fenced out for good once rejected. */
    @Test
    void aCursorIsFencedOnWhatItReadAndStaysFencedOut() {
        InMemoryEventLog log = new InMemoryEventLog();
        ExecutionId id = ExecutionId.random();
        log.append(id, new CatalystEvent.ExecutionCreated(T, "Task", "h", "cfg", ""));

        StreamCursor cursor = StreamCursor.after(log, id, log.read(id));
        assertThat(cursor.append(new CatalystEvent.ExecutionStarted(T, 1, "a"))).isEqualTo(1);

        log.append(id, new CatalystEvent.ExecutionStarted(T, 2, "someone-else"));   // another writer moves on

        assertThatThrownBy(() -> cursor.append(new CatalystEvent.ExecutionPaused(T, "late")))
                .isInstanceOf(StaleWriterException.class);
        assertThat(cursor.fencedOut()).isNotNull();
        assertThatThrownBy(() -> cursor.append(new CatalystEvent.ExecutionPaused(T, "later still")))
                .as("refused without consulting the log again").isInstanceOf(StaleWriterException.class);
        assertThat(log.read(id)).hasSize(3);

        // A fresh read is a fresh writer's view, and is accepted.
        assertThat(StreamCursor.after(log, id, log.read(id)).append(new CatalystEvent.ExecutionPaused(T, "ok")))
                .isEqualTo(3);
    }

    /** A log that cannot fence keeps the plain appends it always had. */
    @Test
    void aLogWithoutConditionalAppendStillRuns() {
        EventLog unfenced = new UnfencedLog();
        try (CatalystRuntime runtime = CatalystRuntime.builder().log(unfenced).model(MockModel.alwaysReturn("x")).build()) {
            assertThat(runtime.executeAndWait(new GatedTask())).isEqualTo("x|x");
        }
    }

    private static final class UnfencedLog implements EventLog {
        private final InMemoryEventLog delegate = new InMemoryEventLog();
        public long append(ExecutionId id, CatalystEvent event) { return delegate.append(id, event); }
        public List<SequencedEvent> read(ExecutionId id) { return delegate.read(id); }
        public long latestSeq(ExecutionId id) { return delegate.latestSeq(id); }
        public Optional<ExecutionId> findByKey(String key) { return delegate.findByKey(key); }
        public void putKey(String key, ExecutionId id) { delegate.putKey(key, id); }
        public void close() { }
    }
}
