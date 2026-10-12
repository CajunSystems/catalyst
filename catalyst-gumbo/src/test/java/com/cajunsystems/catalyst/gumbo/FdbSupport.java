package com.cajunsystems.catalyst.gumbo;

import com.cajunsystems.catalyst.events.BlobStore;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * How the FoundationDB tests find a cluster, and what they do when there is none — Gumbo's
 * {@code FdbTestSupport} rule, under Catalyst's own variable.
 *
 * <p>Without a cluster they <em>skip</em>, so a plain {@code mvn install} stays green on a machine
 * without FoundationDB. CI sets {@value #REQUIRE_ENV}{@code =true}, and then an unreachable cluster is
 * a <strong>failure</strong>: a job that quietly skipped every FoundationDB test would read exactly
 * like one that ran them. The cluster file comes from {@code FDB_CLUSTER_FILE}, or FoundationDB's
 * default when unset; {@code scripts/fdb-tests.sh} starts one in Docker and sets both.
 */
final class FdbSupport {

    static final String REQUIRE_ENV = "CATALYST_REQUIRE_FDB";

    private FdbSupport() {}

    static String clusterFile() {
        return System.getenv("FDB_CLUSTER_FILE");
    }

    /** A root no other test is using, so tests never see each other's streams. */
    static String freshRoot() {
        return "catalyst-test-" + UUID.randomUUID();
    }

    /** Opens a log on {@code root}; every log on the same root is another writer on the same data. */
    static GumboEventLog open(String root, BlobStore blobs) {
        return GumboEventLog.foundationDB(clusterFile(), root, blobs);
    }

    /** Skips the calling test without a cluster, or fails it when {@value #REQUIRE_ENV} says there must be one. */
    static void assumeCluster() {
        assumeTrue(available(), "FoundationDB cluster not available — skipping");
    }

    private static Boolean available;

    private static synchronized boolean available() {
        if (available == null) available = probe();
        return available;
    }

    /**
     * Time-bounded: a FoundationDB client facing an unreachable cluster retries forever rather than
     * failing, so an unbounded probe would hang the build instead of skipping.
     */
    private static boolean probe() {
        try {
            CompletableFuture.runAsync(() -> open(freshRoot(), BlobStore.inMemory()).close())
                    .get(20, TimeUnit.SECONDS);
            return true;
        } catch (Throwable e) {
            // Missing fdb-java natives, no cluster file, cluster unreachable — all land here.
            if ("true".equalsIgnoreCase(System.getenv(REQUIRE_ENV))) {
                throw new AssertionError(REQUIRE_ENV + "=true but no FoundationDB cluster is reachable"
                        + " (FDB_CLUSTER_FILE=" + clusterFile() + "); refusing to skip", e);
            }
            return false;
        }
    }
}
