# What Gumbo needs, from Catalyst's perspective

Catalyst is a durable AI execution runtime built on Gumbo. Building it — and then designing
[distributed execution](distribution.md) on top — surfaced a set of defects and missing primitives in
the log layer. None of them are Catalyst-specific. They are the primitives *any* durable execution
engine eventually needs, which is why Boudin (a Temporal-like workflow framework on the same log)
sits behind several of the same limitations.

This document records them, with the evidence for each.

> **Status: most of this has landed.**
>
> | Item | Where |
> |---|---|
> | D1, D3, A1, A2, A5 | Gumbo 0.3.0 |
> | D4 | closed on the Catalyst side by adopting `readAfterVersion` (0.3.0 only *added* it) |
> | A3 | Gumbo 0.4.0 |
> | A4 | Gumbo, merged and unreleased |
> | D2, A6 | open |
> | Multi-tag entries carry one version | open, and now the item Catalyst's v1 depends on |
>
> The D4 verification is in `GumboEventLogTest` (a tail read with a second execution in the log, on
> both adapters, plus the single-writer refusal) and in
> `SnapshotAcceptanceTest.warmInspectMatchesColdWhenAnotherExecutionSharesTheLog`, which reproduces
> the warm-fold corruption end to end and fails if the seqnum-keyed read is restored.
>
> One thing this cost us worth recording: **upgrading to 0.3.0 changed nothing by itself.** The whole
> suite stayed green because 0.3.0 *adds* `readAfterVersion` rather than changing what `readAfter`
> does — the old call kept compiling and kept being wrong. A dependency bump is not a fix until the
> caller moves.
>
> **The same lesson, one layer up: a merged release is not a released one.** Gumbo 0.4.0 is cut and
> merged but the tag was never pushed, and JitPack builds from tags — so A3, the compare-and-set that
> the whole lease and claim story rests on, exists and is unreachable from this build. Catalyst is
> still pinned to 0.3.0 for that reason alone.
>
> **What Catalyst needs next, in order:** the 0.4.0 tag; A4 released (the runtime should refuse to
> distribute against a log reporting `multiWriter() == false` rather than find out later); and the
> multi-tag version defect, which [`distribution.md`](distribution.md) depends on for claimable work
> and which was filed last in Gumbo's backlog on log-migration cost — a ranking made before anything
> depended on it.

## How these conclusions were reached

Every defect below was **measured**, not inferred from reading. The probes were:

| Probe | What it did | Result |
|---|---|---|
| Sequential restart | One process appends `0 1 2`, exits; a second process appends | `3 4 5` — correct |
| Concurrent writers | Two JVMs, one directory, one execution, three appends each | Both assigned `0 1 2`; log reported 3 of 6 |
| Raw file inspection | `strings log.dat` after the above | All six events physically present |
| Tail read | Two executions in one log; `readFrom(second, afterSeq=2)` | Returned `[0,1,2,3,4,5]`, expected `[3,4,5]` |
| Warm fold | Same, through Catalyst's snapshot path, sole vs. shared log | Sole: 43 steps (correct). Shared: **51 steps (corrupted)** |

## What already works well

Worth stating plainly, because the defects below are narrow and the foundation is not the problem.

- **Durability is correct.** `FileBasedPersistenceAdapter.append` calls `force(false)` on both the log
  and index channels; `appendBatch` group-commits with one `fdatasync` for N entries; the FDB adapter
  commits before the future resolves. `BatchingPersistenceAdapter` documents its durability window
  honestly rather than hiding it.
- **The tag model is right.** Tags come from Boki as *virtual log-stream identifiers* — one physical
  log serving many logical streams. That is exactly the partitioning a per-execution event stream
  wants, and none of the fixes below require changing it.
- **Atomic multi-tag append already exists** and is genuinely differentiating. Boudin uses it to write
  one entry to both a per-instance history tag and a shared work-queue tag in a single append, with no
  two-phase write and no window where an item is recorded but not yet visible to workers.
- **A tag KV already exists** — `setTagValue` / `getTagValue` / `deleteTagValue`, persisted to
  `kv.dat` and to an FDB subspace. Catalyst uses it for its idempotency index and snapshots.
- **Push subscriptions exist** (`SharedLogService.notifySubscribers`), which is what lets Boudin's
  dispatcher react to new work rather than poll.
- **Single-process restart recovery is sound** — the index is rebuilt from a log scan on open, which
  is why the sequential-restart probe passed.
- **The distributed sequencer pattern is already demonstrated** by `FoundationDBSequencer`. The fixes
  below mostly consist of applying that existing pattern at a different granularity.

---

# Part 1 — Defects

## D1. `localId` is assigned per-process, so concurrent writers collide

**Severity: critical.** This is the single blocker for any multi-writer use.

**Evidence.** Two JVMs, one directory, one execution, three appends each:

```
PROBE[A] assigned seqs: 0 1 2      PROBE[A] read back 3 events
PROBE[B] assigned seqs: 0 1 2      PROBE[B] read back 3 events
```

**Root cause.** `SharedLogService.localIdFor(tag)` keeps a `ConcurrentHashMap<LogTag, AtomicLong>`,
seeded once and lazily from `adapter.getLocalIdCountForTag(tag)` — which in **both** the file and
FoundationDB adapters reads another in-memory `AtomicLong`, never storage. Two processes seed
identically and diverge silently, with no reconciliation.

This is not fixed by using the FDB backend. `FoundationDBSequencer` sequences only the **global
`seqnum`** through a single `{root}/"seq"` key. `localId` never passes through the `Sequencer` at all.

**Why it exists.** Gumbo's own Boki mapping table shows the cause:

| Boki concept | Gumbo |
|---|---|
| **Per-engine** `localid` | `LogEntry.localId()` — **per-tag** counter assigned at append time |

`localId` was repurposed from per-*engine* to per-*tag* while keeping per-engine assignment. In Boki a
node-local `AtomicLong` is correct by construction — each engine owns its own localid space and the
sequencer reconciles them into the global `seqnum` afterwards. Once `localId` became a per-entity
cursor shared by every writer of a tag, process-local assignment stopped being safe. **The semantics
moved; the implementation did not.**

**Fix.** Make the tag's current version storage-owned, and assign it in the same transaction as the
append. This is the `FoundationDBSequencer` pattern applied at tag granularity.

### Resolved design question: is the fix to *align* `localId` with Boki, or to stop exposing it?

The natural reading of "the semantics moved; the implementation did not" is that the fix is to move
the semantics back — restore `localId` to Boki's per-engine meaning, where a node-local `AtomicLong`
is correct by construction. That is a coherent change, but it is **orthogonal to this defect**,
because Boki's `localid` and the thing Catalyst actually consumes are two different quantities that
Gumbo has merged into one field:

| | Boki `localid` | What Catalyst needs |
|---|---|---|
| Scope | Per **engine** | Per **stream (tag)** |
| Purpose | Write-path pre-sequencing — an id before global order is assigned | Per-entity cursor, and the fence for optimistic concurrency |
| Visibility | **Internal** to the write path | **External**, part of the client contract |
| Lifetime | Superseded once the sequencer assigns `seqnum` | Permanent — it is how a client addresses a position in its stream |

Restoring per-engine semantics would make `localId` correct *and* useless to clients: it would no
longer identify a position in a stream, so D4's version-keyed read would still need a new number, and
A1's fence would still need a third. The two changes do not compete.

**Decision: do not expose `localId` at all. Expose `streamVersion` — per-tag, storage-owned, dense.**
If Gumbo later pursues Boki's parallel data path, `localId` comes back as an internal write-path
detail that never appears in `AppendResult`. Clients never had a use for it.

**Dense, or the tag's latest global `seqnum`?** Reusing `seqnum` as the per-tag cursor is tempting —
it already exists, it is already storage-owned, and it is already unique. Choose **dense** anyway, for
two reasons:

- **No log migration.** A dense, storage-owned counter seeded from persisted state simply continues
  the sequence existing logs already carry, so every log written to date stays readable and every
  recorded cursor stays valid. Redefining the exposed version as the global `seqnum` would silently
  invalidate stored positions in downstream state — Catalyst's snapshots persist a `throughSeq`, and a
  snapshot written under the old meaning would fold from the wrong point under the new one, with no
  error at the seam. Sparse numbering buys nothing that would justify that.
- **Density is a real ergonomic asset**, not just aesthetics. A durable execution runtime reports
  positions to humans: "diverged at step 7" is actionable, "diverged at seqnum 918,442" is not. The
  same property makes logs diffable and makes `expectedVersion` arithmetic obvious.

**What the client side actually depends on.** Catalyst treats `seq` as an ordinal almost everywhere —
substitution, replay alignment and cursor comparison are all ordering, and none of them would notice
gaps. There is **one** exception, and it is worth naming precisely rather than rounding off:
`CatalystRuntime.maybeSnapshot` decides when to checkpoint with

```java
if (folded.lastSeq() - sinceSeqExclusive < snapshotInterval) return;
```

which reads a *difference between two versions* as an *event count*. That identity holds only under
dense numbering. Against a sparse version the subtraction measures the span of the shared global
sequence rather than this execution's own progress, so the interval is overshot and checkpoints fire
early — a degraded heuristic, not corruption, but a real dependency all the same.

Density is also a **tested contract**, not only a documented invariant:
`GumboEventLogTest.appendAssignsDenseSeqAndReadsInOrder` asserts `0, 1, 2`, and a second case asserts
each execution is dense from 0 *independent of how the two interleave in the shared log* — which is
exactly the property a sparse version would abolish. `ReplayTest` pins a specific seq as well.

So the choice is not free on the client side, and the earlier framing understated it: dense is what
the current implementation already relies on in one place and verifies in another. That makes it the
recommendation for a third reason beyond migration cost and readability — sparse numbering would
require changing shipped behaviour and rewriting passing tests to accommodate a storage-side change
that buys nothing in return.

Finally, the fence in A1 uses **this same `streamVersion`** — a conditional append is
"append iff the tag is still at version N". No separate quantity, no second counter to keep
consistent, and the comparison and the increment collapse into one storage operation.

## D2. The index is written per-process and clobbers

**Severity: high.**

**Evidence.** After the concurrent-writer probe, the log reported **three** of six appends — but
`strings log.dat` shows all six are physically present (`AAA#0..2`, `BBB#0..2`, 966 bytes). Nothing
was lost; the *view* was.

**Root cause.** `index.dat` is written per-process, so the last process to close overwrites the
other's view of the log.

**Fix.** The index needs to be either append-only, storage-owned, or always rebuilt from a log scan on
open. The rebuild path already exists and works — it is what makes single-process restart correct.

## D3. Nothing prevents a second process opening a live log

**Severity: medium** — it converts D1 and D2 from "unsupported" into "silently wrong".

**Evidence.** No `FileLock`, `tryLock` or equivalent anywhere in `FileBasedPersistenceAdapter`. A
second process opens a directory another process is actively writing, with no error and no warning.

**Fix.** Until multi-writer is genuinely supported, the file adapter should take an exclusive lock on
its directory and fail fast. A clear "already open by another process" error is vastly better than
duplicate ids discovered later as corrupted state. This is worth doing **even before** D1, because it
is small and it converts silent corruption into a loud failure.

## D4. There is no version-keyed read — and its absence is already corrupting state

**Severity: high.** This one is causing a live bug in shipped Catalyst code today.

**Evidence.** Catalyst asks for the tail of one execution after seq 2, with two executions in the log:

```
PROBE readFrom(second, afterSeq=2) returned seqs: [0, 1, 2, 3, 4, 5]   expected [3, 4, 5]
```

And end-to-end, through Catalyst's snapshot warm-read path:

```
PROBE SOLE execution             cold steps=43  warm steps=43  OK
PROBE SHARED log (2 executions)  cold steps=43  warm steps=51  *** CORRUPTED ***
```

**Root cause.** Every read entry point is keyed on the **global `seqnum`**:
`PersistenceAdapter.readByTag(tag, fromSeqnum)`, `LogView.readAfter(afterSeqnum)`,
`LogView.readFrom(LogPosition, max)`. There is no way to say *"give me this tag's entries after
version N"*. Catalyst needs a per-execution cursor, so it passes a `localId` into a `seqnum`-keyed
API. The two number spaces coincide **only when the log holds a single stream** — which is true in
every test and false in every production deployment.

The consequence for Catalyst: the snapshot warm read returns events already folded into the snapshot,
and the reducer re-applies them (its contract requires exactly the events after the snapshot point,
and it does not defend against violations). Timeline steps, token counts, cost and attempt counters
all double-count.

**Fix.** Add a version-keyed read — the single most valuable addition on this list. See A2.

---

# Part 2 — Missing primitives

## A1. Conditional append (storage-owned)

```java
AppendResult append(LogTag tag, byte[] data, long expectedVersion);  // rejects on mismatch
```

The cornerstone. It lets a stale writer be rejected **by storage**, which means correctness no longer
depends on any coordination layer being right. A lock service can tell a node it holds a lock, but
never that it *still* holds it at the instant it writes — a GC pause between those two moments is
enough for two nodes to both believe they own a stream. Pushing the check to where the write lands
removes that entire class of failure.

Two constraints on the design:

- **It must be a Gumbo primitive, not a client-side wrapper.** Gumbo assigns the version, so a
  client-side compare would race the assignment underneath it. The comparison and the increment have
  to be atomic in the same store — ideally literally the same operation.
- **It must not silently degrade.** A default that ignores `expectedVersion` and writes
  unconditionally would make a log *look* like it was participating in the fencing protocol while
  providing none of it, surfacing as corruption rather than as an error. Follow the convention
  `PersistenceAdapter` already uses for optional capabilities: throw `UnsupportedOperationException`,
  and let callers check support explicitly (A4).

## A2. Version-keyed reads

```java
List<LogEntry> readFromVersion(LogTag tag, long fromVersion);
List<LogEntry> tail(LogTag tag, long afterVersion);
```

**Highest practical value on this list**, because its absence is already causing D4. Every consumer
that maintains a per-stream cursor — Catalyst resuming from a snapshot, Boudin replaying a workflow
history, Bayou's `EventSourcedActor` rebuilding state — currently has to either rescan the whole
stream or misuse the seqnum-keyed API.

Note the read path is already indexed per tag (`tagSeqnums` is a `ConcurrentSkipListMap<seqnum,
localId>` per tag), so a version-keyed lookup is a change of key, not a new index.

## A3. Compare-and-set on the tag KV

```java
boolean compareAndSetTagValue(LogTag tag, String key, byte[] expected, byte[] value);
boolean putTagValueIfAbsent(LogTag tag, String key, byte[] value);
boolean deleteTagValueIf(LogTag tag, String key, byte[] expected);
long incrementTagValue(LogTag tag, String key, long delta);
```

The KV already exists and is already load-bearing (Catalyst's idempotency index and snapshots both
use it). Adding CAS turns it into a coordination substrate — leases, ownership records, work claims —
without introducing ZooKeeper or any new subsystem.

A note on leases specifically: **expiry needs a clock, and clocks are where lease coordination gets
subtle.** The good news is that with A1 in place, clock skew on lease expiry becomes an *efficiency*
problem (two nodes briefly duplicate work) rather than a *correctness* problem (the log rejects the
loser). That is the whole point of putting the fence in storage. A stored `expiresAt` in the value,
compared by claimants, is sufficient — native TTL is a convenience, not a requirement.

## A4. Declared capabilities

```java
interface LogCapabilities {
    boolean conditionalAppend();
    boolean compareAndSet();
    boolean versionedReads();
    boolean pushSubscriptions();
    boolean atomicMultiTagAppend();
    boolean multiWriter();
}
```

Today a client has to consult documentation to know what an adapter supports. That is exactly how
Catalyst ended up misusing a seqnum-keyed read as if it were version-keyed (D4).

**Capabilities must be per-adapter, not per-Gumbo.** The file adapter cannot offer genuine
cross-process atomicity; FoundationDB can. A client should be able to refuse to start in distributed
mode against a log that reports `multiWriter() == false`, rather than discovering it through
corruption.

## A5. `AppendResult` everywhere, and rename `localId` → `streamVersion`

The record already exists — `AppendResult(seqnum, localId, primaryTag, timestamp)` — so most of this
is done. Two refinements:

- **Return it consistently** from every append entry point, so fields can be added later
  (`commitVersion`, `partition`, `transactionId`) without breaking callers.
- **Replace `localId` with `streamVersion`** — a rename in effect, but not only a rename: per the
  design decision resolved under D1, `localId` should leave the public API rather than be renamed in
  place. The word *version* tells a reader that optimistic concurrency lives here; *localId* tells them
  nothing, and actively misleads — it is a fossil of Boki's per-engine semantics, which is precisely
  why it does not describe what the field now means. `streamVersion` is per-tag, storage-owned and
  dense, and it is the same quantity A1 conditions on and A2 reads from; if Boki-style per-engine
  `localId` is ever reintroduced, it belongs on the write path, not in `AppendResult`.

  For Catalyst this change is cheap: `localId` appears in exactly one module
  (`catalyst-gumbo/GumboEventLog`), six lines, all at a single mapping seam.

## A6. Atomic multi-tag append as a first-class operation

This **already works** — Boudin depends on it — so the work is ergonomics and an explicit guarantee,
not new capability:

```java
AppendResult appendAtomically(byte[] data, LogTag primary, LogTag... additional);
```

One open design question the current API does not answer: **with per-tag versions, what does
`expectedVersion` mean for a multi-tag append?** N tags implies either N expected versions or a
designated primary. For Catalyst's use — one execution tag that needs fencing, one queue tag that does
not — conditioning on the primary tag only is the right semantics. Whatever the answer, it needs to be
stated, or implementers will each assume something different.

---

# Part 3 — Cross-cutting concerns

These are not blockers, but they shape whether the above scales.

**In-memory index growth.** `globalIndex` (`ConcurrentSkipListMap<seqnum, offset>`), `tagSeqnums`
(per tag), and `kvStore` are all fully in-memory and hold an entry per log record. For a durable
execution runtime whose logs are meant to live forever, this is unbounded heap proportional to total
history. Worth a plan — spillable index, or bounded cache over an on-disk index — before the first
large deployment.

**Trim versus permanent history.** `trim(upToSeqnum)` exists and is honoured by `readByTag` via
`effectiveFrom = max(fromSeqnum, trimSeqnum)`. For Catalyst this is dangerous: replay requires the
*whole* stream from seq 0, so trimming a tag silently destroys replayability for those executions.
Either trim needs to be tag-aware with an opt-out, or clients need a way to mark a tag as
non-trimmable.

**Snapshot interaction.** If a stream can be trimmed but a snapshot covers the trimmed prefix, the
combination is safe. Without snapshots it is not. This relationship should be explicit in the API
rather than left to each client.

---

# Part 4 — Suggested order

Ordered by (blocking-ness × cost), not by conceptual elegance.

| # | Item | Why here |
|---|---|---|
| 1 | **D3** — exclusive directory lock, fail fast | Smallest possible change; converts silent corruption into a loud error. Buys safety immediately while the rest is designed. |
| 2 | **A2** — version-keyed reads | Fixes a live corruption bug (D4) in a downstream framework. Independent of everything else. |
| 3 | **A5** — `localId` out, `streamVersion` in; return `AppendResult` | Do it *before* new APIs are written against the old name — A1 and A2 both name this quantity, so settling it first stops two more surfaces inheriting the fossil. Cheap now, expensive later. |
| 4 | **D1 + A1** — storage-owned version + conditional append | The cornerstone. One change: if the version is storage-owned and assigned in the append transaction, conditional append is nearly free, because the comparison and the increment become one operation. |
| 5 | **D2** — non-clobbering index | Required for multi-writer to be usable, but pointless before D1. |
| 6 | **A3** — KV compare-and-set | Unlocks leases and work claiming. Depends on nothing above, but is only *useful* once D1/A1 land. |
| 7 | **A4** — declared capabilities | Best added once there is real variation between adapters to declare. |
| 8 | **A6** — multi-tag ergonomics + the `expectedVersion` rule | Pure polish over an existing capability. |

Items 1–3 are worth doing regardless of whether anything ever runs multi-writer: a fail-fast lock, a
correct tail read, and an honest name are improvements to a single-node log too.

# Part 5 — Tests worth having

Each of these corresponds to a probe that found a real defect. They are cheap and they would have
caught every issue in this document.

1. **Two processes, one directory, one tag.** Assert no duplicate versions and that every append is
   readable afterwards. *(Currently fails: duplicate versions, 3 of 6 readable.)*
2. **Two tags in one log, tail read on the second.** Assert `readFromVersion(tag, n)` returns exactly
   the entries after version `n` **of that tag**. *(Currently fails: returns the whole stream.)*
3. **Restart continuation.** One process writes, exits, another continues. Assert versions continue
   rather than restart, **and remain dense across the restart** — density is a client-visible contract
   (see D1), so any change to how versions are assigned has to preserve it, including across a reopen.
   *(Currently passes — keep it that way.)*
4. **Conditional append rejection.** Two writers at the same expected version; assert exactly one
   succeeds and the other is rejected rather than silently accepted.
5. **Capability honesty.** For each adapter, assert every capability it reports `true` for actually
   works, and every one it reports `false` for throws rather than silently no-ops.

The general lesson from all of these: **every existing test uses one tag in a fresh log**, which is
the one configuration where a per-stream version and a global sequence number are indistinguishable.
That single blind spot hides D1, D2 and D4 simultaneously.
