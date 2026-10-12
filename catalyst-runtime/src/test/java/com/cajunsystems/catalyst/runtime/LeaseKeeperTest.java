package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.log.Lease;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a release does with a cancel request that arrived after this node's last renewal. It must not
 * delete a request nobody honoured, or a cancel issued just as the owner parked would be lost. A
 * request on an execution that already finished is moot, so its row goes.
 */
@org.junit.jupiter.api.Timeout(30) // a lease bug tends to hang (a parked attempt nobody stops), not fail
class LeaseKeeperTest {

    private final SharedLog log = SharedLog.distributable();
    private final LeaseTest.TestClock clock = new LeaseTest.TestClock();

    private LeaseKeeper keeper(boolean terminal) {
        return new LeaseKeeper(log, "node-a", clock, Duration.ofSeconds(30), Duration.ofHours(1), id -> terminal);
    }

    private Lease requestCancelBehindItsBack(ExecutionId id) {
        Lease mine = log.readLease(id).orElseThrow();
        assertThat(log.compareAndSetLease(id, mine, mine.withCancelRequest("stop"))).isTrue();
        return mine;
    }

    @Test
    void aReleaseKeepsAnUnhonouredCancelOnALapsedRowForTheNextClaimer() {
        ExecutionId id = ExecutionId.random();
        try (LeaseKeeper keeper = keeper(false)) {
            keeper.acquire(id);
            requestCancelBehindItsBack(id);
            keeper.release(id);
        }
        Lease left = log.readLease(id).orElseThrow();
        assertThat(left.cancelRequested()).contains("stop");
        assertThat(left.liveAt(clock.instant())).as("lapsed, so any node may claim it").isFalse();
    }

    @Test
    void aReleaseDropsACancelOnAFinishedExecution() {
        ExecutionId id = ExecutionId.random();
        try (LeaseKeeper keeper = keeper(true)) {
            keeper.acquire(id);
            requestCancelBehindItsBack(id);
            keeper.release(id);
        }
        assertThat(log.readLease(id)).isEmpty();
    }

    /** Honoured means recorded, which makes the execution terminal: then the row goes. */
    @Test
    void aReleaseAfterHonouringTheCancelDeletesTheRow() {
        ExecutionId id = ExecutionId.random();
        try (LeaseKeeper keeper = keeper(true)) {
            keeper.acquire(id);
            requestCancelBehindItsBack(id);
            keeper.onCancel(id, reason -> true);
            keeper.renewAll(); // absorbs the request, and the attempt takes it
            keeper.release(id);
        }
        assertThat(log.readLease(id)).isEmpty();
    }

    @Test
    void aRenewalAbsorbsACancelRequestAndKeepsTheLease() {
        ExecutionId id = ExecutionId.random();
        String[] cancelled = {null};
        try (LeaseKeeper keeper = keeper(false)) {
            keeper.acquire(id);
            keeper.onCancel(id, reason -> {
                cancelled[0] = reason;
                return true;
            });
            requestCancelBehindItsBack(id);
            clock.advance(Duration.ofSeconds(5));
            keeper.renewAll();
            assertThat(cancelled[0]).isEqualTo("stop");
            assertThat(keeper.holds(id)).isTrue();
            Lease row = log.readLease(id).orElseThrow();
            assertThat(row.expiresAt()).isEqualTo(clock.instant().plus(Duration.ofSeconds(30)));
            assertThat(row.cancelRequested()).contains("stop");
        }
    }

    /**
     * A takeover carries a request over. Releasing without honouring it (a node that lacks the task
     * type, or a pause) must leave it for the next claimer, not delete it with the row this node wrote.
     */
    @Test
    void aReleaseKeepsACarriedCancelItNeverHonoured() {
        ExecutionId id = ExecutionId.random();
        assertThat(log.compareAndSetLease(id, null, new Lease("node-dead", clock.instant().toEpochMilli() - 1, "stop"))).isTrue();
        try (LeaseKeeper keeper = keeper(false)) {
            keeper.acquire(id);
            assertThat(log.readLease(id).orElseThrow().cancelRequested()).as("carried over").contains("stop");
            keeper.release(id);
        }
        Lease left = log.readLease(id).orElseThrow();
        assertThat(left.cancelRequested()).contains("stop");
        assertThat(left.liveAt(clock.instant())).isFalse();
    }

    /**
     * Absorbed while no attempt is registered (a pause holding the lease, say): not honoured, so the
     * release keeps it. Before, the renewal counted it as honoured and the release deleted it.
     */
    @Test
    void aRequestAbsorbedWithNoAttemptToTakeItIsKeptOnRelease() {
        ExecutionId id = ExecutionId.random();
        try (LeaseKeeper keeper = keeper(false)) {
            keeper.acquire(id);
            requestCancelBehindItsBack(id);
            keeper.renewAll();
            keeper.release(id);
        }
        assertThat(log.readLease(id).orElseThrow().cancelRequested()).contains("stop");
    }

    /** An attempt that registers after the request was absorbed is cancelled at registration. */
    @Test
    void anAttemptRegisteringAfterTheRequestWasAbsorbedIsCancelledAtOnce() {
        ExecutionId id = ExecutionId.random();
        String[] cancelled = {null};
        try (LeaseKeeper keeper = keeper(true)) {
            keeper.acquire(id);
            requestCancelBehindItsBack(id);
            keeper.renewAll();
            keeper.onCancel(id, reason -> {
                cancelled[0] = reason;
                return true;
            });
            assertThat(cancelled[0]).isEqualTo("stop");
            keeper.release(id);
        }
        assertThat(log.readLease(id)).as("honoured (terminal), so released").isEmpty();
    }

    /** A finished attempt cannot take a cancel: it is not counted as honoured, and the release keeps it. */
    @Test
    void aFinishedAttemptDeclinesTheCancelAndTheRequestIsKept() {
        ExecutionId id = ExecutionId.random();
        try (LeaseKeeper keeper = keeper(false)) {
            keeper.acquire(id);
            keeper.onCancel(id, reason -> false);
            requestCancelBehindItsBack(id);
            keeper.renewAll();
            keeper.release(id);
        }
        assertThat(log.readLease(id).orElseThrow().cancelRequested()).contains("stop");
    }

    /**
     * The attempt took the cancel, but the trip landed after it had decided to park or abandon, so
     * nothing was recorded and the execution is not terminal. The request must stay for the next
     * holder. Before, "handed to an attempt" counted as honoured and the release deleted it.
     */
    @Test
    void aCancelAnAttemptTookButNeverRecordedIsKept() {
        ExecutionId id = ExecutionId.random();
        try (LeaseKeeper keeper = keeper(false)) {
            keeper.acquire(id);
            keeper.onCancel(id, reason -> true);
            requestCancelBehindItsBack(id);
            keeper.renewAll();
            keeper.release(id);
        }
        assertThat(log.readLease(id).orElseThrow().cancelRequested()).contains("stop");
    }
}
