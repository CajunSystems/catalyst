package com.cajunsystems.catalyst.runtime;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.log.Lease;
import com.cajunsystems.catalyst.log.QueuedExecution;
import com.cajunsystems.catalyst.log.WorkQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Claims executions from a {@link WorkQueue} and runs them on a {@link CatalystRuntime}.
 *
 * <p>This is the whole of "start another instance and throughput rises": workers do not know about
 * each other, form no cluster, elect no leader and share no membership. Each one cursors the queue,
 * takes what it can claim, and runs it with {@code resume(id)} -- the same recovery path a crash
 * already used, pointed at another node's work instead of this process's own past.
 *
 * <h2>What makes it safe</h2>
 * <p>Not the lease. A lease is a hint about who <em>intends</em> to run something, and can be wrong
 * in every way a distributed lock can be wrong -- most simply, a holder can stall past its expiry in
 * a GC pause while another worker legitimately steals the claim, leaving two processes convinced
 * they own one execution. Safety comes from the log: a worker running claimed work appends
 * conditionally on the seq it believes the stream is at, so the stalled one is rejected by storage
 * the moment it writes. The lease only stops that race from being the common case rather than the
 * rare one. This is why {@link #start()} refuses a log that cannot fence, and why it will happily
 * run against one that reports it is not multi-writer.
 *
 * <h2>Reclaiming a dead worker's executions</h2>
 * <p>There is no reaper, no scan for orphans and no liveness table. An entry this worker has seen
 * but has not run to completion stays in a small pending set, and every cycle retries claiming it.
 * While a peer is alive and renewing, those claims fail and cost one compare-and-set. When the peer
 * dies its lease simply stops being renewed, the next retry succeeds, and the execution
 * <em>resumes</em> -- substituting its recorded prefix and continuing at the boundary it reached,
 * not restarting. A worker also begins at the head of the queue rather than its tail, so a restart
 * re-examines everything and picks up whatever was stranded while it was gone.
 *
 * <p>That is the whole reclaim mechanism, and it is small because the queue is a cursored stream
 * rather than a destructive take: reading an entry consumes nothing on anybody's behalf, so there is
 * no delivery to time out and no dead-letter state to reconcile.
 */
public final class Worker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Worker.class);

    private final CatalystRuntime runtime;
    private final WorkQueue queue;
    private final WorkerConfig config;

    private final AtomicBoolean running = new AtomicBoolean();
    private final CountDownLatch stopped = new CountDownLatch(1);
    private final Map<ExecutionId, Lease> held = new ConcurrentHashMap<>();

    /**
     * Queue entries seen but not yet known finished, retried every cycle. Ordered so reclaim
     * attempts stay in submission order; bounded in practice by how much work is in flight.
     *
     * <p>Guarded by its own monitor. It is read by the claim loop and written by every completing
     * execution's thread, so the ordering that makes it a {@code LinkedHashSet} is exactly what an
     * unsynchronized access would corrupt -- and the symptom would be a worker that quietly stops
     * retrying one stranded execution, which is indistinguishable from an idle queue.
     */
    private final Set<QueuedExecution> pending = new LinkedHashSet<>();

    private volatile Thread loopThread;
    private long cursor = -1;

    Worker(CatalystRuntime runtime, WorkQueue queue, WorkerConfig config) {
        this.runtime = runtime;
        this.queue = queue;
        this.config = config;
    }

    /** This worker's node id, as it appears in the leases it takes. */
    public String nodeId() {
        return config.nodeId();
    }

    /** The executions this worker currently holds a claim on. */
    public Set<ExecutionId> claimed() {
        return Set.copyOf(held.keySet());
    }

    /**
     * Starts claiming and running work on a virtual thread. Idempotent.
     *
     * @throws IllegalStateException if the log cannot fence appends. Refused rather than degraded:
     *         running claimed work on a log that cannot reject a stale writer means the first time
     *         two workers meet on one execution, the damage is a corrupted stream rather than a
     *         rejected append -- and nothing observes that until the execution fails to fold, long
     *         after the run that caused it.
     */
    public Worker start() {
        if (!runtime.log().supportsConditionalAppend()) {
            throw new IllegalStateException(
                    "cannot run claimed work on a log that cannot fence appends ("
                    + runtime.log().getClass().getSimpleName() + "): a stale writer could not be"
                    + " rejected, so two workers meeting on one execution would corrupt it");
        }
        if (!running.compareAndSet(false, true)) return this;
        if (!runtime.log().supportsMultiWriter()) {
            // Not fatal, and worth saying out loud rather than either failing or staying silent. A
            // fenced-but-single-writer log is exactly the shape of every log Catalyst builds today,
            // and it is genuinely useful -- several workers in one process, and correct. What it
            // cannot do is span processes, and a deployment that assumed otherwise would look fine
            // right up until the second node started.
            log.info("Worker {} on queue '{}': log reports it is not multi-writer, so this worker is"
                    + " safe alongside others in this process but not across processes",
                    config.nodeId(), queue.name());
        }
        // A platform thread, deliberately, where the rest of the runtime uses virtual ones.
        //
        // This loop spends its life blocked in the log's lease key-value store, whose adapter methods
        // are synchronized -- and a virtual thread that blocks inside `synchronized` cannot unmount,
        // so it holds its carrier for the whole call. A worker polls, claims and renews continuously,
        // so on a small machine two of them are enough to pin every carrier the scheduler has; the
        // executions those workers started then cannot be scheduled to finish, and nothing progresses.
        // Measured: eight concurrent executions across two workers deadlocked the JVM outright.
        //
        // Virtual threads buy nothing here anyway. There is one of these per worker, it is long-lived,
        // and it is blocking by design -- the exact shape a platform thread is for.
        loopThread = Thread.ofPlatform()
                .name("catalyst-worker-" + config.nodeId())
                .daemon(true)
                .start(this::loop);
        return this;
    }

    private void loop() {
        try {
            while (running.get()) {
                boolean didWork = cycle();
                if (!didWork) {
                    Thread.sleep(config.pollInterval().toMillis());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Throwable t) {
            // A worker loop that dies silently looks exactly like an idle queue, which is the single
            // most confusing failure this component can have.
            log.error("Worker {} loop terminated", config.nodeId(), t);
        } finally {
            releaseAll();
            stopped.countDown();
        }
    }

    /** One pass: renew what we hold, retry what is pending, then take on new work. Returns true if anything happened. */
    private boolean cycle() {
        renewHeld();
        List<QueuedExecution> retry;
        synchronized (pending) {
            retry = new ArrayList<>(pending);
        }
        boolean didWork = attempt(retry);
        if (held.size() >= config.maxConcurrent()) return didWork;

        List<QueuedExecution> fresh = queue.poll(cursor, config.batchSize());
        if (fresh.isEmpty()) return didWork;
        synchronized (pending) {
            for (QueuedExecution entry : fresh) {
                cursor = Math.max(cursor, entry.cursor());
                pending.add(entry);
            }
        }
        return attempt(fresh) || didWork;
    }

    /** Tries to claim and run each entry, returning true if any was claimed. */
    private boolean attempt(List<QueuedExecution> entries) {
        boolean any = false;
        for (QueuedExecution entry : entries) {
            if (held.size() >= config.maxConcurrent()) break;
            if (held.containsKey(entry.execution())) continue;
            if (claimAndRun(entry)) any = true;
        }
        return any;
    }

    private boolean claimAndRun(QueuedExecution entry) {
        ExecutionId id = entry.execution();
        Optional<Lease> claim = queue.claim(id, config.nodeId(), config.leaseTtl(), Instant.now());
        if (claim.isEmpty()) return false; // someone else holds it; retried next cycle

        Lease lease = claim.get();
        held.put(id, lease);

        // Already finished by whoever held it before us -- claim it, notice, hand it back. Checked
        // after claiming rather than before because before is a guess: the state can turn terminal
        // between the read and the claim, and acting on the earlier answer is what makes a race.
        if (runtime.inspect(id).isTerminal()) {
            finish(id);
            return true;
        }

        try {
            ExecutionHandle<?> handle = runtime.resume(id, ExecutionOptions.none(), /* fenced */ true);
            // Wait for it on its own virtual thread rather than here: the claim loop has to keep
            // cursoring, renewing the leases it already holds and reclaiming peers' stranded work
            // while this execution runs, and a worker that blocked on each execution in turn would
            // do none of that -- and would hold maxConcurrent at one however it was configured.
            // Platform, for the same reason as the loop: this thread releases the lease when the
            // execution ends, which is another synchronized key-value write. Bounded by maxConcurrent.
            Thread.ofPlatform().daemon(true).name("catalyst-claimed-" + id.value()).start(() -> {
                try {
                    handle.result();
                } catch (RuntimeException | Error e) {
                    // The outcome -- including failure -- is already recorded in the execution's own
                    // log by the attempt that produced it. Nothing to add here; the worker's only
                    // remaining job is to stop holding the claim.
                    log.debug("Worker {} finished {} exceptionally", config.nodeId(), id, e);
                } finally {
                    finish(id);
                }
            });
            return true;
        } catch (RuntimeException e) {
            // Most likely: this worker has no factory registered for the task type, so it cannot run
            // this execution -- but another worker in a differently-configured deployment might.
            // Release rather than fail the execution, and leave it pending so the claim is retried.
            log.warn("Worker {} claimed {} but cannot run it; releasing", config.nodeId(), id, e);
            finish(id);
            return false;
        }
    }

    /** Drops an execution: release its lease and stop retrying it. */
    private void finish(ExecutionId id) {
        Lease lease = held.remove(id);
        if (lease != null) {
            try {
                queue.release(lease);
            } catch (RuntimeException e) {
                // Releasing early is an optimisation over waiting out the TTL; failing to is survivable.
                log.debug("Worker {} could not release the lease on {}", config.nodeId(), id, e);
            }
        }
        synchronized (pending) {
            pending.removeIf(q -> q.execution().equals(id));
        }
    }

    private void renewHeld() {
        Instant now = Instant.now();
        Duration ttl = config.leaseTtl();
        for (Map.Entry<ExecutionId, Lease> e : held.entrySet()) {
            Optional<Lease> renewed = queue.renew(e.getValue(), ttl, now);
            if (renewed.isPresent()) {
                held.put(e.getKey(), renewed.get());
            } else {
                // Lost the claim while still running it. Nothing to abort: the execution keeps going
                // and its appends keep being fenced, so if another worker really has taken over, this
                // attempt is rejected at its next write rather than corrupting anything. Dropping it
                // from `held` only stops us renewing a lease we no longer own.
                log.warn("Worker {} lost its lease on {} while running it; its appends will be"
                        + " rejected if another worker has taken over", config.nodeId(), e.getKey());
                held.remove(e.getKey());
            }
        }
    }

    private void releaseAll() {
        for (Lease lease : List.copyOf(held.values())) {
            try {
                queue.release(lease);
            } catch (RuntimeException ignored) {
                // shutting down; the TTL will expire the lease
            }
        }
        held.clear();
    }

    /**
     * Stops claiming new work and releases held leases, so peers can pick those executions up at
     * once rather than after the TTL. In-flight executions are not cancelled -- they are running on
     * the runtime, which owns their lifecycle.
     */
    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) return;
        Thread t = loopThread;
        if (t != null) t.interrupt();
        try {
            stopped.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
