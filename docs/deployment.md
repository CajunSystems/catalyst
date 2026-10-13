# Deploying Catalyst

This guide covers running Catalyst as a service: one node first, then several nodes sharded by
idempotency key, then several nodes sharing one log. It describes what the runtime does today
(Deploy-1 to Deploy-6 and Dist-0 to Dist-4 in [`ROADMAP.md`](../ROADMAP.md)). Distributed execution,
where several nodes share a FoundationDB-backed log and take over each other's work, works end to
end and is a preview: see [Several nodes sharing one log](#several-nodes-sharing-one-log-preview)
below, and [`distribution.md`](distribution.md) for the design.

## The shape of a node

A Catalyst node is a JVM running one `CatalystRuntime` over an embedded, file-backed Gumbo log:

```java
CatalystRuntime runtime = Catalyst.builder(Path.of("/var/lib/catalyst"))
        .model(model)
        .task(new SummarizeTask())          // every task type a restart may need to resume
        .task(new TriageTask())
        .recoverOnStart(true)               // resume what the previous process left running
        .drainTimeout(Duration.ofSeconds(20))
        .build();
```

The log directory is the node's state. Everything the runtime knows — every execution, the
idempotency index, the execution index, snapshots — is in it, and the runtime keeps nothing it cannot
rebuild from it. A node is therefore a **single writer over a persistent directory**, and the rest of
this guide follows from that.

## One node

### Storage: a block volume, never a shared filesystem

Gumbo's file log takes an **exclusive lock on its directory**, and that lock is the whole of the
single-writer guarantee. A second process opening the same directory is refused with a clear error
rather than allowed to interleave writes.

- **Use a `ReadWriteOnce` block volume** (EBS, Persistent Disk, Azure Disk, local SSD). One volume,
  one node.
- **Do not use NFS, EFS, SMB or any shared filesystem.** File locking over network filesystems is
  advisory or unreliable, so two pods could both believe they hold the lock and write the same log.
  Nothing downstream can repair a log two writers have interleaved.

On Kubernetes that is a `StatefulSet` with `replicas: 1` and a `volumeClaimTemplate`. Use a
StatefulSet rather than a Deployment: during a rolling update a Deployment can briefly run the old and
new pod at once, which a `ReadWriteOnce` volume usually prevents but a shared one would not. With a
StatefulSet the old pod is gone before its replacement starts.

### Startup: register every task type, then recover

`recoverOnStart(true)` makes `build()` scan the execution index and resume every execution the
previous process left **crash-interrupted** (`NEW`, `STARTING`, `RUNNING`, `RESUMING`). Executions
that are `PAUSED` stay paused: that state is recorded on purpose, by `pause(id)` or by
`InDoubtPolicy.ASK`, and a restart must not undo it. `WAITING` ones stay put too (a signal, a timer
or a child), except that a sleeper whose timer is due and a parent whose child has finished are
woken.

- **Register every task type before `build()`.** Recovery reconstructs tasks from the registry by
  their recorded type. An execution whose type isn't registered is reported in
  `runtime.startupRecovery().get().notRecovered()` and left alone; startup still succeeds. Use named
  `Task` classes: a lambda's class name is not stable across builds.
- **Log the recovery report.** The runtime logs a one-line summary and a warning per execution it
  could not recover. A non-empty `notRecovered` after a deploy usually means a task class was renamed
  or removed.
- **Task input vars are not persisted**, only their hash. A task that branches on `ctx.var(...)` is
  resumed without them. Resume such executions individually with `resume(id, opts)` instead.
- `recoverOnStart` requires a log with an execution index and refuses to start without one, rather
  than run a node that silently recovers nothing. Executions recorded before the index existed are
  not in it.

### Shutdown: drain, don't kill

`close()` stops accepting work and **abandons** running executions rather than failing them:

1. New submissions are refused; `runtime.isShuttingDown()` turns true. Use it in your readiness probe
   so the load balancer stops sending work.
2. Each running execution is allowed to finish the model or tool call it is in, which is recorded,
   and then stops at its next step **without recording anything**. It stays `RUNNING` in the log, and
   the next start's recovery resumes it with zero duplicate calls.
3. After at most `drainTimeout`, the log is closed and the directory lock released.

Set the timeouts so the drain finishes before the platform kills the process:

```
slowest model/tool call  <  drainTimeout  <  terminationGracePeriodSeconds
```

Kubernetes sends `SIGTERM`, waits `terminationGracePeriodSeconds` (30s by default), then sends
`SIGKILL`. Call `runtime.close()` from your `SIGTERM` handler (a JVM shutdown hook, or your framework's
lifecycle hook). If a call is still running when `drainTimeout` expires, the log is closed under it,
which leaves exactly what a crash at that moment would — see the next section.

### Node loss: choosing the policies

A crash (`kill -9`, node failure, OOM) is always recoverable: everything recorded is kept, and the
next start resumes from it. What remains is a call that was **in flight**: its request was recorded,
its result never was. Was a model call billed? Did a tool's side effect happen? Catalyst cannot know,
and `InDoubtPolicy` decides:

| `InDoubtPolicy` | On resume, an in-flight call is… | Use when |
|---|---|---|
| `FAIL` (default) | failed with `InDoubtException` | you would rather investigate than risk a duplicate |
| `RETRY` | re-issued, completing the original request | the call is idempotent, or a duplicate model call is an acceptable cost |
| `ASK` | paused, for an operator to decide | a duplicate side effect is unacceptable and someone is on call |

A graceful shutdown (above) leaves a call in flight only if it outlives `drainTimeout`; otherwise only a
crash does.

`RetryPolicy` is separate. It governs calls that **failed**, such as a provider 503 or a tool
exception: `none()` (default), `maxRetries(n, backoff)`, or `exponential(...)`. A retry's budget is
recorded in the log, so it survives restarts. Something like `exponential` with a handful of retries
is a reasonable production default for model calls.

### Backup and disaster recovery

The log directory is the backup unit. Take **volume snapshots** (EBS snapshots, CSI
`VolumeSnapshot`s). Gumbo fsyncs each append before acknowledging it, so a block-level snapshot of a
running node holds every event the runtime saw acknowledged. Restoring a snapshot is equivalent to a
crash at the snapshot moment, which is exactly the state `recoverOnStart` is built to resume from:

- Executions in flight at the snapshot are resumed.
- Anything recorded after the snapshot is lost. That includes an idempotency key created after it, so
  a client re-submitting that key starts the work again.
- Calls in flight at the snapshot follow `InDoubtPolicy`.

Large payloads (over 64 KiB by default) are stored outside the log in a blob store, under
`<dir>/blobs` by default, so a volume snapshot covers them. If you configure a remote store with
`Catalyst.builder(dir).blobStore(store)`, back that store up too: a log restored without its blobs
cannot rehydrate the payloads it references.

## Several nodes: sharding by idempotency key

One node is one writer. To scale out, run **independent nodes, each with its own volume and log**,
and route work between them. Nothing is shared between nodes, and each node is exactly the
single-node setup above.

### Route every submission by idempotency key

The routing key must be the **idempotency key**, because it is the only thing a client holds before
submitting. The execution id is minted inside `execute()`, and each node's idempotency index lives in
that node's own log. If the same key could reach two nodes, a re-submission (a client retry, a
redelivered message) would miss the first node's index, create a second execution on the other node,
and **duplicate every model call**.

`ShardRing` is the routing rule:

```java
ShardRing ring = ShardRing.of("shard-0", "shard-1", "shard-2");

// In the router, or the client, in front of the nodes:
String shard = ring.shardFor(idempotencyKey);
nodes.get(shard).execute(task, ExecutionOptions.withKey(idempotencyKey));
```

It uses rendezvous hashing over SHA-256. It is deterministic for a given shard set in every process
and language, so routers need no coordination.

### Make each node enforce it

Build each node as one shard of the same ring:

```java
CatalystRuntime runtime = Catalyst.builder(dataDir)
        .shard(System.getenv("SHARD_ID"), ShardRing.of("shard-0", "shard-1", "shard-2"))
        ...
        .build();
```

A sharded node checks the routing contract itself, before anything is written:

- **A submission without an idempotency key is refused.** There is nothing to route it on.
- **A key that the ring routes to another shard is refused.** A router with a different ring, or a
  bug, fails loudly instead of quietly creating a duplicate.
- **Every id it mints carries its shard** (`exec-<uuid>@shard-1`). Route id-addressed calls
  (`inspect`, `resume`, `cancel`, `pause`, `replay`, `branch`) with `ShardRing.shardOf(id)`; no lookup
  table is needed.
- **An id stamped with another shard is refused** by those calls, instead of reading as "unknown" or
  being resumed by the wrong node. Ids minted before the deployment was sharded carry no shard and are
  accepted.

On Kubernetes, a StatefulSet with `replicas: N` gives each pod a stable name (`catalyst-0`,
`catalyst-1`, …) and its own volume. Derive `SHARD_ID` from the pod name, and route with a headless
Service.

### The shard set is fixed

Rendezvous hashing moves as few keys as possible when the set changes: adding a shard moves only the
keys the new shard wins. But **any key that moves is a duplication risk**. Its execution and its
index entry stay on the old shard, while its re-submissions now go to the new one.

So treat the shard set as fixed for the life of the data. If you must change it, do so only when no
re-submission of an existing key can arrive, for example after every client has finished retrying
the keys it has issued. Do not remove a shard that still holds executions anyone may address: its
ids still route to it by their stamp.

### Moving from one node to several

Sharding an existing single node is a change of shard set too, from one implicit shard to N. Two
things follow:

- **Keys re-route.** Each existing key now routes to whichever shard the ring picks, which is usually
  not the original node. A re-submission of an old key would create a duplicate there. Make the
  original node one of the shards, and only switch the router once no re-submission of an existing
  key can arrive.
- **Old ids carry no shard.** Ids minted before sharding are unstamped, so `ShardRing.shardOf` returns
  empty for them and every node accepts them, and on any node but the original they read as unknown.
  Route them to the original node yourself, for example by keeping a note of which node held the
  pre-sharding data.

### What sharding does not give you

Each shard is still a single node. If a shard's node is down, its executions wait until it comes back
(on Kubernetes, until its pod is rescheduled with its volume). No other shard can take them over,
because no other node can open that shard's log. Failover between nodes needs a log that several
nodes can write safely, which is the next section.

## Several nodes sharing one log (preview)

```java
EventLog log = GumboEventLog.foundationDB(clusterFile, "catalyst", sharedBlobStore);
CatalystRuntime runtime = Catalyst.builder().log(log).model(model)
        .task(TriageTask.class, TriageTask::new)
        .nodeId("node-a").distributed().recoverOnStart(true).build();
```

Every node runs the same build against the same FoundationDB root, each with its own `nodeId`.
`distributed()` refuses a log that cannot fence writes, hold leases and take several writers, and it
can't be combined with `shard(...)`.
- **The log:** `GumboEventLog.foundationDB(...)` is a log several nodes can write without
  corrupting a stream.
- **Leases:** a runtime built with `.nodeId("node-a").distributed()` claims a lease before writing to
  an execution and renews it while running. It refuses executions another node is running
  (`LeaseHeldException`), and its startup recovery takes over only what dead nodes left.
- **Lease timing:** `lease(duration, heartbeat)`, defaults 30s and 10s. A dead node's work waits
  out the lease duration. Don't set it short to speed up failover: a live node that pauses past its
  lease loses the execution, and the call it had in flight is found in doubt by the node that takes
  over. Under the default `InDoubtPolicy.FAIL`, that fails the execution.
- **Claim loop (`claimInterval`, default 10s):** every node scans the shared index and takes over
  what no live node holds. A crashed node's work moves within about one lease duration plus one
  interval, with no operator action.
- **Cancel from any node:** `cancel(id)` on any node reaches the node running the execution, which
  records it at its next heartbeat. Pausing an execution that is running elsewhere is refused, as
  it is on one node.
- **Keys:** two nodes taking the same new idempotency key create one execution.

This is new; treat multi-node runs as a preview. The `distributed-a`/`distributed-b` demo (two JVMs
on one FoundationDB root; CI's `fdb` job, and `scripts/fdb-tests.sh --demo` locally) kills a node
mid-run and shows the rest of the cluster finishing its work.
