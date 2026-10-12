package com.cajunsystems.catalyst.gumbo;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.FileBlobStore;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.log.StaleWriterException;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Dist-0, Catalyst's half: {@link GumboEventLog#foundationDB} on a real cluster, with <strong>two
 * logs on one root</strong> — two connections, two sequencers, two sets of per-process state, which
 * is what two nodes are. Every property here is one a single-writer fixture cannot show: a log
 * opened once only ever sees its own writes, which is how per-process caches pass every test until
 * a second process exists.
 *
 * <p>Skips without a cluster; fails instead under {@code CATALYST_REQUIRE_FDB=true}, as CI runs it
 * (see {@link FdbSupport}). Locally: {@code scripts/fdb-tests.sh}.
 */
class FoundationDBEventLogTest {

    private static final Instant T = Instant.parse("2026-01-01T00:00:00Z");

    @TempDir Path blobs;
    private String root;

    @BeforeEach
    void cluster() {
        FdbSupport.assumeCluster();
        root = FdbSupport.freshRoot();
    }

    /** Each node opens its own log on the shared root; the blob directory stands in for shared storage. */
    private GumboEventLog node() {
        return FdbSupport.open(root, FileBlobStore.at(blobs));
    }

    private static CatalystEvent.ExecutionCreated created() {
        return new CatalystEvent.ExecutionCreated(T, "Task", "h", "cfg", "");
    }

    @Test
    void reportsBothHalvesOfWhatDistributionNeeds() {
        try (GumboEventLog log = node()) {
            assertThat(log.supportsConditionalAppend()).as("fenced").isTrue();
            assertThat(log.supportsMultiWriter())
                    .as("multi-writer — reported by Gumbo for FDB storage plus the FDB sequencer, not asserted here")
                    .isTrue();
        }
    }

    /** The fence holds across connections: the zombie's append is refused and writes nothing. */
    @Test
    void aWriterOnAnotherNodeThatTheStreamHasMovedPastIsRefused() {
        ExecutionId id = ExecutionId.random();
        try (GumboEventLog zombie = node(); GumboEventLog owner = node()) {
            // expectedSeq is the seq the event will take — one past what the writer read.
            zombie.append(id, created(), 0);
            zombie.append(id, new CatalystEvent.ExecutionStarted(T, 1, "node-a"), 1);

            // The owner read through seq 1 and takes over; the zombie still believes the next seq is 2.
            assertThat(owner.append(id, new CatalystEvent.ExecutionResumed(T, 2), 2)).isEqualTo(2);
            assertThatThrownBy(() -> zombie.append(id, new CatalystEvent.ExecutionCompleted(T, new TextNode("late")), 2))
                    .isInstanceOfSatisfying(StaleWriterException.class,
                            e -> assertThat(e.getMessage()).contains(id.value()));

            List<SequencedEvent> history = owner.read(id);
            assertThat(history).extracting(SequencedEvent::seq).containsExactly(0L, 1L, 2L);
            assertThat(history.get(2).event()).isInstanceOf(CatalystEvent.ExecutionResumed.class);
            assertThat(zombie.read(id)).as("both nodes read the same history").isEqualTo(history);
        }
    }

    /**
     * {@code latestSeq} is read through on a multi-writer log. Node A caches its own tip after an
     * append; node B then appends, and A must report B's position — a cache advanced only by A's own
     * writes would still say 1.
     */
    @Test
    void latestSeqIncludesAnotherNodesAppends() {
        ExecutionId id = ExecutionId.random();
        try (GumboEventLog a = node(); GumboEventLog b = node()) {
            a.append(id, created());
            a.append(id, new CatalystEvent.ExecutionStarted(T, 1, "node-a"));
            assertThat(a.latestSeq(id)).isEqualTo(1);

            b.append(id, new CatalystEvent.ExecutionResumed(T, 2), 2);
            b.append(id, new CatalystEvent.EffectRecorded(T, "x", new TextNode("v")), 3);

            assertThat(a.latestSeq(id)).as("A sees B's appends").isEqualTo(3);
            assertThat(b.latestSeq(id)).isEqualTo(3);
        }
    }

    /** Unfenced appends from two nodes racing on one stream still get dense, distinct seqs. */
    @Test
    void twoNodesAppendingConcurrentlyKeepTheSeqDense() throws Exception {
        ExecutionId id = ExecutionId.random();
        int each = 50;
        try (GumboEventLog a = node(); GumboEventLog b = node()) {
            CountDownLatch start = new CountDownLatch(1);
            CompletableFuture<long[]> fromA = CompletableFuture.supplyAsync(() -> appendMany(a, id, "a", each, start));
            CompletableFuture<long[]> fromB = CompletableFuture.supplyAsync(() -> appendMany(b, id, "b", each, start));
            start.countDown();

            long[] seqs = LongStream.concat(LongStream.of(fromA.get()), LongStream.of(fromB.get())).sorted().toArray();
            assertThat(seqs).as("every append got its own seq, none skipped").isEqualTo(LongStream.range(0, 2L * each).toArray());
            assertThat(a.read(id)).extracting(SequencedEvent::seq)
                    .as("read back in seq order").containsExactlyElementsOf(LongStream.range(0, 2L * each).boxed().toList());
        }
    }

    private static long[] appendMany(GumboEventLog log, ExecutionId id, String node, int n, CountDownLatch start) {
        try {
            start.await();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
        long[] seqs = new long[n];
        for (int i = 0; i < n; i++) seqs[i] = log.append(id, new CatalystEvent.EffectRecorded(T, node + i, new TextNode(node)));
        return seqs;
    }

    /**
     * The execution index spans nodes: opens and closes from either node fold into one open set,
     * and a node that starts later — a restarted one, or a new one — sees all of it.
     */
    @Test
    void theOpenSetFoldsBothNodesOpensAndCloses() {
        ExecutionId fromA = ExecutionId.random();
        ExecutionId fromB = ExecutionId.random();
        ExecutionId closedByA = ExecutionId.random();
        try (GumboEventLog a = node(); GumboEventLog b = node()) {
            a.append(fromA, created());
            b.append(fromB, created());
            b.append(closedByA, created());
            // A never opened it, so the close is queued only because A reads its stream's first event.
            a.append(closedByA, new CatalystEvent.ExecutionCompleted(T, new TextNode("ok")));

            assertThat(b.openExecutions()).containsExactly(fromA, fromB);
            try (GumboEventLog late = node()) {
                assertThat(late.openExecutions()).as("a node that starts later").containsExactly(fromA, fromB);
            }

            // A's checkpoint is behind B's now; folding from it must still land on the same set.
            a.append(fromB, new CatalystEvent.ExecutionFailed(T, "boom", 0));
            assertThat(a.openExecutions()).containsExactly(fromA);
            assertThat(b.openExecutions()).containsExactly(fromA);
        }
    }

    @Test
    void anIdempotencyKeyRegisteredOnOneNodeResolvesOnAnother() {
        ExecutionId id = ExecutionId.random();
        try (GumboEventLog a = node(); GumboEventLog b = node()) {
            a.putKey("order-7", id);
            assertThat(b.findByKey("order-7")).contains(id);
        }
    }

    /**
     * FoundationDB refuses a value over 100 KB, which is why the factory insists on a blob store: a
     * 200 KB completion is offloaded, and read back — rehydrated — by the other node.
     */
    @Test
    void aPayloadOverFoundationDBsValueLimitIsOffloadedAndReadByTheOtherNode() {
        ExecutionId id = ExecutionId.random();
        String big = "x".repeat(200 * 1024);
        try (GumboEventLog a = node(); GumboEventLog b = node()) {
            a.append(id, new CatalystEvent.EffectRecorded(T, "big", new TextNode(big)));
            CatalystEvent.EffectRecorded read = (CatalystEvent.EffectRecorded) b.read(id).get(0).event();
            assertThat(read.value().asText()).isEqualTo(big);
        }
    }

    /**
     * The claim Dist-3's leases rest on, across connections: two nodes race to claim the same
     * execution's lease, 20 rounds, and exactly one wins each round.
     */
    @Test
    void exactlyOneOfTwoNodesClaimsALease() throws Exception {
        try (GumboEventLog a = node(); GumboEventLog b = node()) {
            assertThat(a.supportsLeases()).isTrue();
            for (int round = 0; round < 20; round++) {
                ExecutionId id = ExecutionId.random();
                CountDownLatch start = new CountDownLatch(1);
                CompletableFuture<Boolean> fromA = CompletableFuture.supplyAsync(() -> claim(a, id, "node-a", start));
                CompletableFuture<Boolean> fromB = CompletableFuture.supplyAsync(() -> claim(b, id, "node-b", start));
                start.countDown();
                boolean wonA = fromA.get();
                boolean wonB = fromB.get();
                assertThat(wonA ^ wonB).as("round %d: exactly one winner", round).isTrue();
                assertThat(b.readLease(id).orElseThrow().holder()).isEqualTo(wonA ? "node-a" : "node-b");
            }
        }
    }

    private static boolean claim(GumboEventLog log, ExecutionId id, String node, CountDownLatch start) {
        try {
            start.await();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
        return log.compareAndSetLease(id, null, new com.cajunsystems.catalyst.log.Lease(node, Long.MAX_VALUE));
    }
}
