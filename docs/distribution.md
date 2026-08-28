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
restart from scratch — it substitutes its recorded prefix and resumes at the boundary it reached.
That is the M0 exit criterion, already proven, operating across nodes instead of across a crash.

**The guarantee is "no duplicate *recorded* boundaries", not "no duplicate provider calls."** A
boundary that was in flight when the node died is *in doubt*: the provider may have accepted the
request and produced a billable completion that never reached the log. On resume there is no
recorded result to substitute, so the call is re-issued. Measured, on the current single-node code —
a log hand-built to end at `CompletionRequested` with no `CompletionReceived` resumed and invoked
the model again:

```
PROBE model calls before resume: 0
PROBE model calls AFTER resume:  1   ← the accepted-but-unrecorded call was re-issued
```

This is not new to distribution — it is the same crash window that exists single-node — but node
death makes it far more frequent, so the design must not overstate the guarantee.

Catalyst already has the machinery for this on the *tool* side: `seed()` detects a `ToolRequested`
with no matching `ToolCompleted` and routes recovery through `InDoubtPolicy` (`RETRY` / `FAIL` /
`ASK`). There is **no equivalent for model completions** — a trailing `CompletionRequested` sets
`pendingRequestHash` and is otherwise ignored, with no `danglingModel` counterpart to `danglingTool`.
Closing that asymmetry (an in-doubt policy for model calls, keyed on the recorded `requestHash`) is a
prerequisite for honestly claiming exactly-once provider spend under node failure, and is worth doing
independently of distribution.

## What "just start an instance" looks like

The operational model this buys is the one worth having:

```
$ java -jar my-agent.jar    # node A
$ java -jar my-agent.jar    # node B — no config, no seed list, no join
```

Both nodes subscribe to the shared task tag and take what they can claim. Start more
instances and throughput rises; kill one and its executions are reclaimed and resumed. There is no
membership protocol, no seed nodes, no split-brain to reason about, and no cluster state that can
disagree with itself — because there is no cluster, only a shared log.

This is deliberately *less* than BEAM-style clustering, and it is less because Catalyst needs less.
Distributed Erlang has to form a real mesh because a process is an addressable, in-memory, stateful
thing: losing the node loses the state, so you need membership, a global registry and handoff.
Catalyst's state is not in the process — it is in the log, and `resume(id)` can reconstitute it
anywhere. Solving the weaker problem is what lets the operational story be this small.

> The full set of Gumbo defects and missing primitives this design depends on — with the evidence for
> each, a suggested order, and the tests that would have caught them — is written up separately in
> [`gumbo-requirements.md`](gumbo-requirements.md).

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
| Lease storage | Gumbo's tag KV exists (`setTagValue`/`getTagValue`/`deleteTagValue`) — but get/set only | **compare-and-set + TTL** on the existing KV |
| Claimable work | ~~no way to ask the log what needs running~~ | **already possible** — dual-tagging delivers it, and since Gumbo 0.6.0 the queue tag can be cursored by version too |

Smaller than it first appeared, because two of the three are partly solved already.

**The KV already exists**, in Gumbo rather than Catalyst: `PersistenceAdapter.setTagValue /
getTagValue / deleteTagValue`, surfaced as `LogView.getValue/setValue`, persisted to `kv.dat` in the
file adapter and a `kvSubspace` in FoundationDB. Catalyst is already a client of it — `findByKey` /
`putKey` (the idempotency index) and `readSnapshot` / `writeSnapshot` both go through it. So leases
do not need a new store, only a **conditional** write on the store that is already there.

Conditional append must **not** silently degrade. A default method that ignores `expectedSeq` and
writes unconditionally would keep old implementations compiling at the cost of quietly removing the
only fence the design has — a log that cannot reject a stale writer would look like it was
participating in the protocol while providing none of it. That is the worst possible failure shape
for a correctness primitive.

The default therefore **throws**, following the convention Gumbo already uses for optional adapter
capabilities:

```java
default long append(ExecutionId id, CatalystEvent event, long expectedSeq) {
    throw new UnsupportedOperationException(
            "conditional append not implemented by " + getClass().getSimpleName());
}

/** True if this log can reject a stale writer. Distributed execution requires it. */
default boolean supportsConditionalAppend() { return false; }
```

Source compatibility is preserved — existing implementations still compile — but the capability is
now *declarable and checkable*, so the runtime can refuse to enable distributed execution against a
log that cannot fence, rather than discovering it by corruption.

### Claimable work: solved by dual-tagging, not by a new index

An earlier draft of this document called out "no way to ask the log what needs running" as the least
obvious and most consequential gap. It is neither — **Boudin already solves it on the same
substrate**, and the technique falls out of Gumbo's design rather than extending it.

A Gumbo entry can carry *multiple tags*, so one atomic append can write to both a per-instance stream
and a shared queue:

| Boudin tag | Contents |
|---|---|
| `workflow-history:{workflowId}` | the durable per-instance record |
| `workflow-tasks:{taskQueue}` | the delivery channel workers read |

> "a single atomic append writes the same entry to **both** … There is no separate two-phase write."

Catalyst can do exactly this: tag `ExecutionCreated` into `catalyst-tasks/<queue>` alongside
`catalyst-exec/<id>`. No new SPI, no secondary index to keep consistent, and — because the queue
entry and the history entry are the *same append* — no window in which an execution is recorded but
not yet claimable.

Better still, Gumbo has **push subscriptions** (`SharedLogService.notifySubscribers`), which Boudin's
`WorkflowDispatcher` uses to subscribe to its task tag rather than poll. Work delivery is therefore
push already, which removes polling latency from the design and further reduces what a placement
layer would add.

#### Cursoring the queue tag: resolved in Gumbo 0.6.0

This section once read "already possible" without qualification, then carried a caveat, and now
reads plainly again. The middle step is worth keeping, because it is the part that was measured.

"Already possible" was measured against atomic multi-tag append, which does hold — one append, both
tags, no window. It was **not** measured against the *cursor*, and the cursor is where it bit. A
Gumbo entry used to carry **one** `streamVersion`, assigned from its primary tag, and every tag it
touched was told that number. For a tag carried only as a secondary that number belonged to a
different stream: not dense, not starting at zero, and — the part that loses work — able to go
*backwards* relative to entries already delivered. A worker holding "I have processed
`catalyst-tasks/<queue>` through version *N*" could be handed an item numbered below *N*, and a
version-keyed tail read would skip it. Silently, with nothing in the log looking wrong.

**Gumbo 0.6.0 fixes it at the source.** Every tag an entry carries now gets its own position, so a
queue tag is dense from zero in its own right and a version-keyed cursor over it advances one place
per queue entry regardless of how far along each item's workflow happens to be. The fix cost a
record-layout change but no migration: each record's marker says which layout it is, so an existing
log keeps reading and simply grows new-format records on the end.

So the design needs no workaround. `catalyst-tasks/<queue>` is cursored with `readAfterVersion`,
the same read `catalyst-exec/<id>` uses, and the claim loop can hold one number per worker.

**The dependency is pinned, not assumed.** `GumboEventLogTest.aFanOutTagIsCursoredByItsOwnVersion`
exercises the property through the log Catalyst actually builds, so a future regression in the layer
below shows up here rather than as work that is never claimed. That is the same lesson this document
learned the expensive way: the first version of this section asserted a guarantee nobody had
checked.

On the Gumbo side, conditional append is natural for the FoundationDB adapter (real transactions)
and enforceable with a local lock in the file adapter, which is sufficient because that adapter is
single-process by construction.

## Where the CajunSystems actor systems fit

Both sibling projects were surveyed for this design. Neither provides a ready-made "start a node and
it joins" story for Catalyst today, which is part of why the shared-storage design above is not
merely the safer option but the only one that does not block on another project's roadmap.

| | Cajun `ClusterActorSystem` | Bayou | Boudin |
|---|---|---|---|
| Clustering | Yes — metadata store, leader election, actor→node assignment, `EXACTLY_ONCE` delivery | **None** — single process | **None** — no lease/claim/ownership in source |
| Durable substrate | Its own `MetadataStore` + `MessagingSystem` | **Gumbo** | **Gumbo** |
| Relevance | Placement, eventually | Erlang primitives *within* a node | **Closest sibling — same problem, same substrate** |

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

**Boudin** is the closest sibling of the three and deserves the most attention: a Temporal-like
durable workflow framework on Gumbo, whose crash recovery is recognisably the same design as
Catalyst's — load history, cache prior results, substitute on replay, switch to live at the edge. It
contributed the dual-tag technique above. It is also *single-worker-process*: there is no lease,
claim, heartbeat or ownership anywhere in its source, so it sits behind the same Gumbo limitation
Catalyst does.

That sharpens the argument for putting these primitives in the log rather than in an actor system.
Gumbo is the layer Catalyst, Bayou and Boudin all share, and multi-writer-safe id assignment plus a
conditional append would unblock **all three** — Boudin most immediately, since it wants exactly the
same worker-claiming story.

## What this does not give you

Being clear about the limits, since the comparison to BEAM invites them:

- **No hot code reload, no preemptive scheduling, no supervision trees across nodes, no per-process
  isolated heaps.** Those are runtime properties of the JVM, not design choices Catalyst can make.
- **In-flight non-boundary computation is lost when a node dies.** Work since the last recorded
  boundary is redone on resume. This is the trade Catalyst already made for determinism, and replay
  makes it cheap.
- **An in-flight boundary may be executed twice.** See the in-doubt window above: a provider call
  accepted but not yet recorded is re-issued on resume. Bounded by `InDoubtPolicy` for tools; not yet
  bounded at all for model completions.
- **No cross-node visibility of *running* work.** A node cannot ask "who is executing what right
  now" except through lease rows. That is a monitoring gap, not a correctness one.

## Open questions

- **Where CAS belongs on the tag KV.** A `compareAndSetTagValue(tag, key, expected, value)` on
  `PersistenceAdapter` is the obvious shape, and FDB gives it natively. The file adapter needs it only
  to be correct within one process, since a shared *file* directory is not the multi-writer target.
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
