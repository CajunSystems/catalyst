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

    @Test
    void aReleaseAfterHonouringTheCancelDeletesTheRow() {
        ExecutionId id = ExecutionId.random();
        try (LeaseKeeper keeper = keeper(false)) {
            keeper.acquire(id);
            requestCancelBehindItsBack(id);
            keeper.renewAll(); // absorbs the request
            assertThat(keeper.carriedCancel(id)).contains("stop");
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
            keeper.onCancel(id, reason -> cancelled[0] = reason);
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
}
