package com.cajunsystems.catalyst.gumbo;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.log.Lease;
import com.cajunsystems.catalyst.log.QueuedExecution;
import com.cajunsystems.catalyst.log.WorkQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The queue and lease primitives on their own, away from the runtime.
 *
 * <p>{@code DistributedAcceptanceTest} proves the end-to-end behaviour but is a race by nature: it
 * shows that two workers <em>did not</em> collide on a given run. These are the deterministic
 * statements underneath it -- what a claim does when a lease is held, expired, or held by the same
 * node -- expressed with an explicit clock so expiry is decided rather than waited for.
 */
class GumboWorkQueueTest {

    private static final Duration TTL = Duration.ofSeconds(30);

    private static CatalystEvent created() {
        return new CatalystEvent.ExecutionCreated(Instant.now(), "T", "h", "cfg", "");
    }

    @Test
    void submittingIsOneAppendThatLandsInBothStreams(@TempDir Path dir) {
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            WorkQueue queue = log.workQueue("default").orElseThrow();
            ExecutionId id = ExecutionId.random();

            long seq = queue.submit(id, created());

            // In the execution's own stream this is the first event, so seq 0 -- the number a fold
            // depends on. A dual-tagged entry reporting the queue's position here is the defect this
            // pins: the queue is at 0 too on the first submit, so the second one is what separates them.
            assertThat(seq).isZero();
            assertThat(log.read(id)).hasSize(1);
            assertThat(log.read(id).get(0).seq()).isZero();

            ExecutionId second = ExecutionId.random();
            assertThat(queue.submit(second, created()))
                    .as("every execution's own stream starts at 0, whatever the queue is at")
                    .isZero();

            List<QueuedExecution> entries = queue.poll(-1, 10);
            assertThat(entries).extracting(QueuedExecution::execution).containsExactly(id, second);
            assertThat(entries).extracting(QueuedExecution::cursor)
                    .as("the queue is dense from zero in its own right")
                    .containsExactly(0L, 1L);
        }
    }

    @Test
    void aCursorReadsOnlyWhatFollowsIt(@TempDir Path dir) {
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            WorkQueue queue = log.workQueue("default").orElseThrow();
            for (int i = 0; i < 5; i++) queue.submit(ExecutionId.random(), created());

            assertThat(queue.poll(-1, 10)).hasSize(5);
            assertThat(queue.poll(2, 10)).extracting(QueuedExecution::cursor).containsExactly(3L, 4L);
            assertThat(queue.poll(4, 10)).isEmpty();
            assertThat(queue.poll(-1, 2)).hasSize(2);
        }
    }

    @Test
    void twoQueuesAreSeparateStreams(@TempDir Path dir) {
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            WorkQueue a = log.workQueue("alpha").orElseThrow();
            WorkQueue b = log.workQueue("beta").orElseThrow();
            ExecutionId onA = ExecutionId.random();
            a.submit(onA, created());
            b.submit(ExecutionId.random(), created());

            assertThat(a.poll(-1, 10)).extracting(QueuedExecution::execution).containsExactly(onA);
            assertThat(b.poll(-1, 10)).hasSize(1);
            assertThat(b.poll(-1, 10).get(0).execution()).isNotEqualTo(onA);
        }
    }

    @Test
    void aHeldLeaseExcludesEveryoneElseUntilItExpires(@TempDir Path dir) {
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            WorkQueue queue = log.workQueue("default").orElseThrow();
            ExecutionId id = ExecutionId.random();
            queue.submit(id, created());

            Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
            Optional<Lease> mine = queue.claim(id, "node-a", TTL, t0);
            assertThat(mine).isPresent();
            assertThat(mine.get().nodeId()).isEqualTo("node-a");

            // Held: refused, right up to the last instant before expiry.
            assertThat(queue.claim(id, "node-b", TTL, t0)).isEmpty();
            assertThat(queue.claim(id, "node-b", TTL, t0.plus(TTL).minusMillis(1))).isEmpty();

            // Expired: taken, with the fencing token advanced so a contested execution is legible.
            Optional<Lease> stolen = queue.claim(id, "node-b", TTL, t0.plus(TTL));
            assertThat(stolen).isPresent();
            assertThat(stolen.get().nodeId()).isEqualTo("node-b");
            assertThat(stolen.get().token()).isGreaterThan(mine.get().token());
        }
    }

    @Test
    void aNodeCanReclaimItsOwnLeaseWithoutWaitingForExpiry(@TempDir Path dir) {
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            WorkQueue queue = log.workQueue("default").orElseThrow();
            ExecutionId id = ExecutionId.random();
            queue.submit(id, created());
            Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

            assertThat(queue.claim(id, "node-a", TTL, t0)).isPresent();

            // A worker that crashed and came back under the same node id must not be locked out by
            // the lease its previous life took -- otherwise a restart is punished with a full TTL of
            // its own work being unavailable, which is the opposite of what the lease is for.
            assertThat(queue.claim(id, "node-a", TTL, t0.plusSeconds(1)))
                    .as("a claim by the same node is re-entrant")
                    .isPresent();
        }
    }

    @Test
    void aRenewalAfterTheLeaseWasStolenFails(@TempDir Path dir) {
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            WorkQueue queue = log.workQueue("default").orElseThrow();
            ExecutionId id = ExecutionId.random();
            queue.submit(id, created());
            Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

            Lease mine = queue.claim(id, "node-a", TTL, t0).orElseThrow();
            assertThat(queue.renew(mine, TTL, t0.plusSeconds(1))).isPresent();

            // Stolen after expiry, then the original holder tries to renew. It must be told it lost
            // the lease rather than quietly overwriting the new holder's claim -- which is the whole
            // point of comparing against the exact bytes it last saw.
            Lease stolen = queue.claim(id, "node-b", TTL, t0.plus(TTL).plusSeconds(60)).orElseThrow();
            assertThat(queue.renew(mine, TTL, t0.plusSeconds(2))).isEmpty();
            assertThat(queue.leaseOf(id)).contains(stolen);
        }
    }

    @Test
    void releasingHandsTheExecutionOverImmediately(@TempDir Path dir) {
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            WorkQueue queue = log.workQueue("default").orElseThrow();
            ExecutionId id = ExecutionId.random();
            queue.submit(id, created());
            Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

            Lease mine = queue.claim(id, "node-a", TTL, t0).orElseThrow();
            assertThat(queue.claim(id, "node-b", TTL, t0)).isEmpty();

            assertThat(queue.release(mine)).isTrue();
            assertThat(queue.leaseOf(id)).isEmpty();
            assertThat(queue.claim(id, "node-b", TTL, t0))
                    .as("a released execution is claimable at once, not after the TTL")
                    .isPresent();

            // Releasing a lease already lost is a normal outcome, not an error: it is what finishing
            // work whose claim was stolen mid-run looks like.
            assertThat(queue.release(mine)).isFalse();
        }
    }

    @Test
    void leasesSurviveReopeningTheLog(@TempDir Path dir) {
        ExecutionId id = ExecutionId.random();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            WorkQueue queue = log.workQueue("default").orElseThrow();
            queue.submit(id, created());
            assertThat(queue.claim(id, "node-a", TTL, t0)).isPresent();
        }
        // A lease that lived only in memory would let a restarted process immediately re-run work
        // another node is still running -- the failure mode leases exist to make rare.
        try (GumboEventLog reopened = GumboEventLog.at(dir)) {
            WorkQueue queue = reopened.workQueue("default").orElseThrow();
            assertThat(queue.leaseOf(id)).isPresent();
            assertThat(queue.claim(id, "node-b", TTL, t0)).isEmpty();
            assertThat(queue.poll(-1, 10)).extracting(QueuedExecution::execution).containsExactly(id);
        }
    }
}
