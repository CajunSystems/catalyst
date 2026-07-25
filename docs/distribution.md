# Distributed execution

Catalyst runs on one node today. This document records the design for running it on many — the v1
roadmap item — and, more importantly, records *where the correctness lives*, because that is the
decision which is expensive to revisit later.

The short version: **the log is the arbiter, not a coordination service.** Nodes do not form a
cluster, do not elect a leader, and do not need to know each other exists. They compete for work
through shared storage, and the storage layer — not a lock — is what prevents two nodes from
corrupting one execution.

## The invariant

Exactly one writer may append to an `ExecutionId`'s stream at a time. Two concurrent writers
interleave events into a stream that is dense, ordered and per-execution, and the result does not
fold, replay or resume. Everything else in this document exists to protect that one sentence.

In-process, this is already handled: `KeyedLock` guards the schedule-attempt decision and `inFlight`
prevents a second concurrent attempt. Both are JVM-local, and both stop meaning anything the moment
a second node exists.

## Why not a distributed lock

The obvious move is to keep the same shape and make the lock distributed — a cluster-singleton actor
per `ExecutionId`, or a lease in ZooKeeper/etcd, or Cajun's `ClusterActorSystem` assignment map. An
actor mailbox genuinely *is* per-execution locking expressed as a queue, so the fit looks natural.

It is the wrong place to put the guarantee. A lock service tells a node "you hold the lock"; it
cannot tell the node "you *still* hold it" at the instant the node actually writes. Between those two
moments a GC pause, a scheduler stall or a network partition can expire the lease while the holder
remains convinced it is the owner. The lock service then hands the lock to a second node, both write,
and the stream is corrupt. This is not a defect in any particular implementation — it is what
distributed locks do without fencing, and it applies equally to Cajun, ZooKeeper and etcd.

So the guarantee moves to where the write actually lands.

## The design: conditional append

Give `EventLog` a conditional append:

```java
long append(ExecutionId id, CatalystEvent event, long expectedSeq);  // rejects on mismatch
```

Catalyst's `seq` is already dense and per-execution, which makes it a natural fence. A writer says
"append this, and only if the stream is still at seq N". Two writers race; storage accepts one and
rejects the other. No consensus, no clock assumptions, no reliance on anything outside the store.

This is the whole correctness story, and it has a property worth stating plainly:

> With conditional append, a stale writer cannot corrupt an execution **even if the coordination
> layer is wrong**. Placement becomes an optimisation — avoiding wasted duplicate work — rather than
> a correctness dependency.

That is what makes the choice of actor system a late, reversible decision instead of a foundational
one.

## Claiming work

Correctness is settled by the paragraph above; the rest is efficiency and operations.

1. **Claim** — a node compare-and-sets a lease row for an execution: `(executionId → nodeId,
   fencingToken, expiresAt)`.
2. **Run** — `runtime.resume(id)`. This already exists and is CI-gated: a node recovers an execution
   *from its id alone* via the `TaskRegistry`, substitutes every recorded boundary, and continues at
   the exact point it left off.
3. **Heartbeat** — the holder renews the lease while it runs.
4. **Reclaim** — if a node dies, its lease expires and another node claims. Conditional append means
   a zombie that still believes it holds the lease cannot do damage; its writes are simply rejected.

The reclaim path is where Catalyst beats a conventional job queue. A reclaimed execution does not
restart from scratch — it substitutes its recorded prefix and resumes at the boundary it reached,
with zero duplicate model calls. That is the M0 exit criterion, already proven, operating across
nodes instead of across a crash.

## What "just start an instance" looks like

The operational model this buys is the one worth having:

```
$ java -jar my-agent.jar    # node A
$ java -jar my-agent.jar    # node B — no config, no seed list, no join
```

Both nodes poll for claimable work against the shared log and take what they can. Start more
instances and throughput rises; kill one and its executions are reclaimed and resumed. There is no
membership protocol, no seed nodes, no split-brain to reason about, and no cluster state that can
disagree with itself — because there is no cluster, only a shared log.

This is deliberately *less* than BEAM-style clustering, and it is less because Catalyst needs less.
Distributed Erlang has to form a real mesh because a process is an addressable, in-memory, stateful
thing: losing the node loses the state, so you need membership, a global registry and handoff.
Catalyst's state is not in the process — it is in the log, and `resume(id)` can reconstitute it
anywhere. Solving the weaker problem is what lets the operational story be this small.

## Prerequisite: Gumbo is not multi-writer safe today

This was measured, not assumed. Two JVMs were pointed at one Gumbo directory and each appended three
events to the same execution:

```
PROBE[A] assigned seqs: 0 1 2      PROBE[A] read back 3 events
PROBE[B] assigned seqs: 0 1 2      PROBE[B] read back 3 events
(a third process opening afterwards)  read back 3 events
```

Six appends went in; the log reports three. Both writers were handed **the same `seq` values** for
the same execution, which is precisely the invariant at the top of this document being violated.

Inspecting the raw files shows the damage is narrower than it looks, and therefore fixable:

```
log.dat (966 bytes) contains: AAA#0 AAA#1 AAA#2 BBB#0 BBB#1 BBB#2   ← all six survive
```

**Nothing was physically lost.** Every event is on disk. What broke is the *index* and the *id
space*:

1. **`localId` assignment is process-local.** `SharedLogService.localIdFor(tag)` keeps a
   `ConcurrentHashMap<LogTag, AtomicLong>`, seeded once and lazily from
   `adapter.getLocalIdCountForTag(tag)` — which in both the file adapter *and* the FoundationDB
   adapter reads an in-memory `AtomicLong`, never storage. Two processes therefore seed identically
   and diverge silently. Note this is not fixed by the FDB backend: `FoundationDBSequencer` sequences
   only the **global `seqnum`** through a single `{root}/"seq"` key. `localId` never passes through
   the `Sequencer` at all — and `localId` is exactly what Catalyst uses as `seq`.
2. **`index.dat` is written per-process**, so the last process to close clobbers the other's view.
3. **No cross-process guard.** There is no file lock on the directory; a second process opens a live
   log without complaint.

The single-process restart path is sound, and worth stating because it shows the recovery machinery
already exists: a fresh process correctly continued at `3 4 5` after a previous one wrote `0 1 2`,
because the index is rebuilt from a log scan on open. The gap is concurrency, not durability.

### Why the gap exists — and why it is narrow

Gumbo's tags come from [Boki](https://github.com/ut-osa/boki), where they are *virtual log-stream
identifiers*: one physical log serving many logical streams. That is exactly the right partitioning
for per-execution streams, and Catalyst's one-tag-per-execution is the intended usage. The
abstraction is not what needs fixing.

The defect is visible in Gumbo's own Boki mapping table:

| Boki concept | Gumbo |
|---|---|
| **Per-engine** `localid` | `LogEntry.localId()` — **per-tag** counter assigned at append time |

`localId` was repurposed from per-*engine* to per-*tag* while keeping per-engine assignment. In Boki
a node-local `AtomicLong` is correct by construction — each engine has its own localid space, and the
sequencer reconciles them into the global `seqnum` afterwards. Under the redefinition, `localId`
became a per-entity cursor shared by every writer of that tag, but the assignment stayed
process-local. The semantics moved; the implementation did not.

That also explains why `seqnum` is fine and `localId` is not. The distributed story for `seqnum` was
anticipated and delivered (`FoundationDBSequencer`); `localId` never got the same treatment because
in Boki it never needed it.

The practical consequence is that the fix is not novel design work — it is applying the existing
`Sequencer` pattern at tag granularity. And because the tag's current `localId` is already the
tracked quantity, `expectedSeq` is a comparison against something the store holds anyway, which is
what makes conditional append cheap to add rather than a new subsystem.

### What this implies for conditional append

It relocates the fix. Catalyst cannot layer conditional append *above* Gumbo, because Gumbo assigns
the id: a Catalyst-side `expectedSeq` check would race the assignment underneath it — classic
time-of-check/time-of-use. The comparison and the assignment have to be atomic **in the same store**.

So `append(…, expectedSeq)` must be a **Gumbo primitive**, with `EventLog` merely exposing it. That
reinforces, on correctness grounds, the conclusion the previous section reached on reuse grounds:
these primitives belong in the log, not in Catalyst and not in an actor system.

## SPI changes

Three gaps, all in storage. None of them require an actor system.

| Gap | Today | Needed |
|---|---|---|
| Conditional append | `append(id, event)` — unconditional | `append(id, event, expectedSeq)`, rejecting on mismatch |
| Lease storage | `findByKey` / `putKey` — no CAS, no expiry | compare-and-set with TTL |
| Claimable work | per-execution reads + `findByKey` only | an index of non-terminal, unleased executions |

The first is additive: a default method that degrades to unconditional keeps every existing
`EventLog` implementation compiling and single-node behaviour unchanged.

The third is the least obvious and the most consequential — there is currently **no way to ask the
log "what needs running?"**. Every existing read path starts from an `ExecutionId` you already have.
A distributed Catalyst needs the inverse query, and that shape should be settled before anything is
built on it.

On the Gumbo side, conditional append is natural for the FoundationDB adapter (real transactions)
and enforceable with a local lock in the file adapter, which is sufficient because that adapter is
single-process by construction.

## Where the CajunSystems actor systems fit

Both sibling projects were surveyed for this design. Neither provides a ready-made "start a node and
it joins" story for Catalyst today, which is part of why the shared-storage design above is not
merely the safer option but the only one that does not block on another project's roadmap.

| | Cajun `ClusterActorSystem` | Bayou |
|---|---|---|
| Clustering | Yes — metadata store, leader election, actor→node assignment, `EXACTLY_ONCE` delivery | **None** — single process, no node identity or remote concept |
| Durable substrate | Its own `MetadataStore` + `MessagingSystem` | **Gumbo** — the same log Catalyst uses |
| Erlang-style primitives | Actors, supervision, messaging | Supervision trees, death watch, linking, timers, back-pressure, PubSub |

**Cajun** is the closest fit for placement: `register(actorClass, executionId)` plus
`routeMessage(executionId, …)` would give one owner node per execution and a mailbox that serialises
writes. It is the natural eventual home for step 1 of the claim loop, replacing polling with
push-based assignment — lower latency, less wasted scanning. It is explicitly *not* where the
correctness lives, so it can be adopted when it is ready and backed out cheaply if it is not.

**Bayou** does not distribute, but it shares Gumbo with Catalyst, and that matters twice over:

- It is immediately useful *inside* a node — supervising the claim loop, timers for lease renewal,
  death watch on worker crashes. That is the Erlang programming model at node level, which is
  separable from the clustering question.
- Because the primitives above land in **Gumbo**, not in Catalyst, Bayou could later consume the same
  conditional append and lease CAS to become clustered itself. The work compounds across the stack
  instead of being duplicated in it.

That is the argument for putting distribution primitives in the log rather than in an actor system:
Gumbo is the layer Catalyst and Bayou already share.

## What this does not give you

Being clear about the limits, since the comparison to BEAM invites them:

- **No hot code reload, no preemptive scheduling, no supervision trees across nodes, no per-process
  isolated heaps.** Those are runtime properties of the JVM, not design choices Catalyst can make.
- **In-flight non-boundary computation is lost when a node dies.** Work since the last recorded
  boundary is redone on resume. This is the trade Catalyst already made for determinism, and replay
  makes it cheap.
- **Polling latency.** Until push-based assignment exists, a freshly submitted execution waits up to
  one poll interval. This is the specific thing Cajun would later fix.

## Open questions

- **Claimable-index shape.** A scan over a status index, a durable work topic, or a separate queue
  tag? This is the SPI decision worth settling first.
- **Lease duration and heartbeat interval.** Too short causes spurious reclaims under GC pressure;
  too long delays failover. Needs a default plus a knob.
- **Fencing token vs. `expectedSeq`.** They may be the same thing: `expectedSeq` already fences the
  write, so a separate token may be redundant. Worth resolving before implementing leases.
- **Cost of per-tag sequencing.** Applying the `Sequencer` pattern at tag granularity is the natural
  fix (see above), but a distinct FDB key per execution is a very different access pattern from one
  global counter. Worth weighing against deriving `localId` from a persisted per-tag counter updated
  in the *same transaction* as the append — which would also make `expectedSeq` free, since the
  comparison and the increment become one operation.
- **Index durability under concurrent writers.** `index.dat` is currently written per-process and
  clobbers. Rebuilding from a log scan on open already works; the question is whether that is
  sufficient at scale or whether the index must become append-only.

## Phasing

0. **Gumbo multi-writer safety** — cross-process `localId` assignment and a non-clobbering index.
   Measured above; this is a hard prerequisite and it is Gumbo-side work, not Catalyst work. Until it
   lands, two Catalyst nodes on one log corrupt each other's executions, and no amount of care above
   the log prevents it.
1. **Conditional append**, implemented as a Gumbo primitive and exposed through the `EventLog` SPI.
   Additive, testable single-node today, and the piece the correctness argument rests on.
2. **Shared-log Gumbo cluster** — the durability substrate for more than one node.
3. **Claim loop** — lease CAS plus the claimable index, driving the existing `resume(id)`. This is
   the point at which "start another instance" begins to work.
4. **Push-based placement** — Cajun, or anything else, swapped in behind `KeyedLock`, which was
   deliberately left as the single seam for exactly this.

Steps 0 and 1 are where the design risk is, and both are in Gumbo. Step 4 is where the convenience
is, and it is optional.

A useful consequence of the ordering: steps 0 and 1 are worth doing regardless of whether Catalyst
ever distributes. A log that assigns ids safely across writers and supports compare-and-append is
simply a better log, and Bayou would benefit from the same work.
