# Catalyst Roadmap

Catalyst **v0.1 is complete**: M0 (execute + record + resume), M1 (replay + inspect), and
M2 (branch + diff) are implemented, CI-gated, and merged. This document lays out what comes next.

The organizing principle is unchanged from the spec: **execution semantics first.** Each phase adds
capability without breaking the durable event schema — the sealed `CatalystEvent` hierarchy already
reserves the slots later phases need (`WAITING`/`ExecutionPaused`, `ExecutionBranched`,
`RetryRequested`), so growth stays additive.

Status legend: ✅ done · 🔜 next · 🔭 later

---

## v0.1 — shipped ✅

| Milestone | What it delivers |
|---|---|
| **M0 — execute + record + resume** | `Task`/`Context` SPIs, event-sourced runtime, in-memory + durable Gumbo log, idempotency keys, `kill -9` crash recovery with zero duplicate model calls |
| **M1 — replay + inspect** | Strict canonical-hash replay, `NonDeterministicReplayException`, `replay(id, task)`, typed token/cost timeline (`CostModel`, `ExecutionState.timelineView()`), LangChain4j model adapter |
| **M2 — branch + diff** | `ReplayMode.BRANCH` (fork on divergence), `runtime.branch(id, atSeq)` with model / counterfactual-tool swaps, `Trajectory` + `TrajectoryDiff` |

CI runs all three exit demos as gates (`.github/workflows/ci.yml`).

---

## v0.2 — Harden, scale, observe ✅

Production-readiness for the single-node runtime. No new top-level concepts; deepen what exists.

**Delivery: small, independently-shippable increments.** Each item below lands as its own PR with its
own tests (and an exit demo where it touches the engine), keeping the diff reviewable and CI green at
every step. Snapshots was the first. Planned order of the remaining increments — most self-contained
first: **① cancellation event → ② task registry / `resume(id)` → ③ built-in HTTP + Filesystem tools →
④ generic-collection payloads → ⑤ blob store**, with schema evolution, retry semantics, the
auto-capture agent, per-execution locking, streaming, and observability sequenced after. Order is a
guide, not a contract — it flexes as we learn. Snapshots, the cancellation event (①), the task
registry / standalone `resume(id)` (②), the built-in HTTP + Filesystem tools (③), generic-collection
payloads (④), the blob store (⑤), schema evolution, per-execution locking, retry semantics, the OTel
exporter, the auto-capture agent, streaming completions, and the timeline UI have all shipped —
**v0.2 is complete**.

### Durability & storage (spec §8)
- ✅ **Snapshots** — periodic fold checkpoints so long executions don't re-fold the whole log on
  `inspect`/resume. `EventLog` gained a snapshot read/write seam (`readSnapshot`/`writeSnapshot`) plus
  a tail read (`readFrom`); the reducer is now resumable (`Reducer.foldFrom` over a serializable
  `ReducerState`) and folds *from* the latest snapshot forward. The runtime checkpoints
  opportunistically every `snapshotInterval` events (builder-configurable, default 100; `0` disables).
  Gated by the v0.2 Snapshot exit demo in CI.
- ✅ **Blob store** (⑤) — a content-addressed `BlobStore` (in-memory + durable `FileBlobStore`, SHA-256
  refs, dedup) offloads any event payload field over a threshold (default 64 KiB) at the `EventCodec`
  seam and rehydrates it on decode, so oversized completions/tool results/documents are stored
  out-of-line and the rest of the system only ever sees fully-inlined events. Small events stay
  byte-identical to the no-blob encoding, and a blob-backed codec still reads legacy inlined logs.
  `GumboEventLog.at(path)` wires a `FileBlobStore` under `path/blobs` by default. Gated by the v0.2
  Blob-store exit demo in CI. *Swappability follow-up (noted): the `BlobStore` SPI is already
  injectable with an opaque ref contract (a custom S3/GCS/Redis store just returns its own refs), but
  two additive ergonomics remain for remote backends — make `BlobStore extends AutoCloseable` (default
  no-op) so `GumboEventLog.close()` can close it, and add a `.blobStore(...)` convenience to
  `Catalyst.builder()` instead of only the log factory.*
- ✅ **Schema evolution** (open question §13.4) — strategy chosen and documented in
  [`docs/schema-evolution.md`](docs/schema-evolution.md): **tolerant reader** for additive changes
  (the shared mapper ignores unknown fields and defaults missing ones) + an **`EventUpcaster`** chain
  applied on decode for structural changes (rename field/type, change type, split). Upcasters compose,
  see fully-inlined events, and are registered via `EventCodec.builder()` / `GumboEventLog.at(path,
  upcasters)`. An explicit schema version is deferred until the first breaking change (absent ⇒ v1), so
  today's logs stay byte-identical. Gated by the v0.2 Schema-evolution exit demo in CI.

### Determinism & correctness
- ✅ **Auto-capture agent** (spec §6, v0.2 stretch) — a Byte Buddy agent rewrites the JDK's
  nondeterministic call sites inside configured task packages into calls on a static `AutoCapture`
  bridge, which records each value through the executing `Context` as an ordinary effect boundary
  (`auto:<source>#<n>`, positional per attempt) — so a task written with plain `Instant.now()` /
  `UUID.randomUUID()` / `Math.random()` / `Random` draws resumes, replays and branches exactly, with
  no `ctx.effect` in it. Captured sources are selectable (`time`, `identity`, `random`); packages are
  opt-in with no default, backed by a deny list (above all the bridge's own package — rewriting it
  would make every capture recurse). Only invocations are rewritten, never a method's shape, so
  already-loaded classes can be retransformed and `install()` works from a running process, not just
  `-javaagent` at launch. The bridge lives in `catalyst-core` so instrumented classes link and run
  unchanged when no agent is attached (bridge methods fall through to the plain JDK call), and
  capture is suppressed inside boundaries that already record their own result (manual `effect`
  suppliers, tool bodies) so a nested capture cannot desynchronise the boundary queue. Two things
  real bytecode forced: Byte Buddy's default configuration skips synthetic methods, which would have
  exempted every lambda body; and draws are matched by declaring type against `RandomGenerator`, which
  covers `ThreadLocalRandom`/`SecureRandom`/`SplittableRandom` plus `nextInt(int,int)` (a method
  `Random` does not declare). Draws always advance their generator even when the value is substituted
  — the generator is task-local state, so replay must leave it where the recorded run did, or a resume
  that substitutes a prefix then draws live continues from a stale position. Shipped as two artifacts:
  the library jar (ordinary dependency, for the programmatic `install()`) and a self-contained
  `-javaagent`-classified jar with Byte Buddy bundled and relocated — a `-javaagent` jar does not get
  its Maven dependencies, and bundling `catalyst-core` into it would give instrumented code a
  *different* `AutoCapture` than the runtime binds. Gated by the v0.2 Auto-capture exit demo and a
  packaging gate in CI. *Not captured (noted):
  `Random`'s stream methods (`ints()`/`doubles()`), the `now(Clock)`/`now(ZoneId)` overloads (an
  explicit time source is the caller's own determinism choice), and nondeterminism on threads the task
  spawns — the binding is per-thread by design.*
- ✅ **Generic-collection payloads** (④) — `PayloadCodec` now encodes `List`/`Set`/`Map`/arrays
  structurally, carrying each element in its own typed envelope (recursively), so element types survive
  the round-trip (`List<Point>` comes back as records, not maps; `Map` keys may be non-`String`). Leaf
  encoding is byte-identical to before, so existing logs interoperate, and the class allowlist is
  enforced at every nested leaf and array component (gadget-safe). Collections rebuild as
  `ArrayList`/`LinkedHashSet`/`LinkedHashMap` (equal by content). Gated by the v0.2 Collection-payloads
  exit demo in CI.
- ✅ **Streaming completions** (open question §13.1) — resolved: the `Model` SPI gets a streaming
  variant now, as a **default method** (`stream(request, TokenSink)`) rather than a second interface,
  so every existing `Model` keeps working and `Model` stays functional. The default delegates to
  `complete()` and hands the sink one chunk; an adapter overrides it to deliver incrementally and must
  block until the completion is assembled. Streaming is treated as a **delivery** concern, not a
  durability one: the boundary recorded is the assembled `Completion` — byte-for-byte the same
  `CompletionReceived` a non-streaming call writes — so the schema did not change, a streamed and a
  non-streamed execution are indistinguishable in the log, and the two are interchangeable on replay.
  A replay re-emits the recorded text to the sink (the task rebuilds its result from what it receives)
  and contacts no provider. Token-level replay is deliberately deferred: chunk boundaries are not
  recorded, so replay delivers the text in one piece — a task may accumulate chunks but must not branch
  on how many arrived, which is the one determinism caveat streaming adds. `MockModel` streams in
  word-sized chunks; `LangChain4jModel` adapts a `StreamingChatModel`, bridging its async callbacks to
  the synchronous contract over a queue so the sink runs on the **task's** thread (where the `Context`
  is bound) rather than the provider's. Auto-capture is suppressed while the sink runs, for the same
  reason it is inside an `effect` supplier: the sink executes between `CompletionRequested` and
  `CompletionReceived`, and a capture there would append into that gap. Gated by the v0.2 Streaming
  exit demo in CI.
- ✅ **Retry semantics** (open question §13.3) — resolved in favour of **retry-as-attempt** (same
  `ExecutionId`, same stream) over child execution: a retryable task failure appends `RetryRequested`
  instead of `ExecutionFailed`, then re-enters the task as a resume with the recorded prefix
  substituted and only the failed boundary re-run live (a tool failure is recorded, so `RetryRequested`
  carries the `failedSeq` the seeder drops; model/effect failures record nothing and already re-run).
  A pluggable `RetryPolicy` (`none()` default — opt-in; `maxRetries`, `exponential`) bounds it,
  configured runtime-wide (`Catalyst.builder().retryPolicy`) or per execution
  (`ExecutionOptions.retryPolicy`). Retries fold to a **crash-safe** `retries` counter distinct from
  `attempt` (a crash resume never burns budget). Retryability is an engine gate (excludes determinism
  divergence, in-doubt, interrupts, `Error`) consulted before the policy. This is **whole-task** retry,
  not per-tool. Gated by the v0.2 Retry exit demo in CI.

### Runtime ergonomics & scale
- ✅ **Dedicated cancellation event** (①) — `ExecutionCancelled` folds `cancel()` to `CANCELLED`
  instead of `FAILED`. Cancellation of a running task is now cooperative: `cancel(id)` trips a
  `CancellationToken` and interrupts the worker, which unwinds at its next live boundary and records
  the event itself (so no other thread ever writes to a running execution's stream). A task not
  running in this process records the event directly; attaching to a cancelled execution surfaces a
  `CancellationException`. Cancellation never masks a real failure — only the cooperative unwind
  itself (the `CancellationSignal` the task hits at its next live boundary) folds to `CANCELLED`; any
  other throwable after a cancel, including a bare `InterruptedException` from cleanup, still records
  `ExecutionFailed`. The interrupt is only a best-effort nudge to reach that boundary. Gated by the
  v0.2 Cancellation exit demo in CI.
- ✅ **Standalone `resume(id)` / task registry** (②) — a `TaskRegistry` maps a recorded task type to
  a `TaskFactory`, so `runtime.resume(id)` recovers an execution from its id alone — no idempotency
  key, no re-submitted `Task` instance (the M0 recover-by-key path still works). Register types up
  front via `Catalyst.builder().task(...)`. A non-terminal execution runs forward with every recorded
  boundary substituted (zero duplicate side effects); a terminal one replays its recorded outcome
  without re-running. Use named `Task` classes: a lambda's synthetic class name is not stable across
  processes. Gated by the v0.2 Resume-by-id exit demo in CI.
- ✅ **Per-execution locking** — the coarse instance-wide `synchronized` on
  `execute`/`resume`/`pause`/`cancel` (each of which does log I/O + a full fold before it schedules) is
  replaced by a reentrant, reference-counted `KeyedLock` seam with two domains: `byExecution` (keyed by
  `ExecutionId`) guards the schedule-attempt decision, and `byIdempotencyKey` (keyed by the key string)
  keeps the `findByKey → putKey → createExecution` window atomic so concurrent same-key submits still
  create exactly one execution. Lock order is key → id (no cycle). Unrelated executions no longer
  serialize on each other's log I/O or folds. Idle keys hold no lock (evicted at refcount zero). Covered
  by `KeyedLockTest` + new `CatalystRuntimeTest` concurrency cases (the isolation test is verified to
  deadlock if the old global lock is restored). *Distribution follow-up (noted): the in-process
  single-writer invariant (`inFlight` + `KeyedLock`) holds within one JVM only. Cluster-wide
  single-writer-per-execution wants a cluster-singleton actor per `ExecutionId` across **Cajun** nodes
  (see v1 distributed execution) — an actor mailbox **is** per-execution locking expressed as a queue.
  `KeyedLock` is deliberately the single seam that swap replaces; it was chosen over an actor here
  because the critical section is tiny and in-process, where a mailbox would add latency and a
  request/response round-trip to the synchronous `execute`/`pause` API for no in-process gain.*
- ✅ **Remaining built-in tools** (③) — `HttpTool` (pluggable `Sender` seam; default wraps
  `java.net.http.HttpClient`, tests run offline; safe-by-default `TargetPolicy` blocks
  loopback/link-local/private/metadata targets and re-validates every redirect hop, with
  `allowAll()`/custom opt-outs) and `FilesystemTool` (sandboxed to a root dir; rejects `..`, absolute,
  and symlink escapes, walking each path component `NOFOLLOW` via `SecureDirectoryStream` —
  `openat(O_NOFOLLOW)` semantics — so intermediate-directory symlink swaps can't escape). Both are
  non-deterministic recorded boundaries: a
  strict replay substitutes them, re-issuing no request and re-applying no write. Outputs are flat
  records (full header maps / structured listings await ④); large bodies inline until the blob store
  (⑤). Gated by the v0.2 Built-in-tools exit demo in CI. (`ShellTool` stays excluded until there's a
  policy story.)

### Observability (spec §12)
- ✅ **OTel exporter** — `catalyst-otel`'s `CatalystTracer.export(id, events)` folds an execution's
  event log into one OpenTelemetry trace: a root span for the run (name = task type, bounds =
  `startedAt`/`endedAt`, status OK/ERROR, attempt/retries/token/cost attributes), a child span per
  side-effecting boundary (model / tool / effect / memory — model/tool carry real latency, model spans
  carry that call's own tokens/cost/finish reason), and lifecycle moments (started/resumed/paused/
  retry/branched/terminal) as span events on the root. Read-only and post-hoc — it consumes
  `runtime.log().read(id)`, needing no runtime hook — so the log genuinely *is* the trace. The module
  depends on the OpenTelemetry **API** only; the app supplies the SDK + a real OTLP exporter (the same
  shape as the LangChain4j adapter). Gated by the v0.2 OTel exit demo in CI.
- ✅ **Timeline UI** — `catalyst-timeline`'s `TimelineReport.html(state)` renders a folded
  `ExecutionState` as a **self-contained HTML page**: a status header, the roll-ups
  (`timelineView()` — model/tool counts, tokens, cost, latency, wall clock) and the step-by-step
  trajectory with each boundary's label, offset, latency and recorded payload. Read-only and post-hoc,
  the same shape as the OTel exporter — it consumes a fold of the log and installs no runtime hook, so
  any execution renders without having cooperated in advance, and the log stays the single source of
  truth. Because the input is a fold, the output is a **pure function** of the log: two renders of one
  execution are byte-identical, which is what makes a report safe to diff or commit as a build
  artifact. The page references nothing external (inline CSS, no scripts/fonts/images) so it opens from
  `file://` years later; everything interpolated is HTML-escaped, since tool names, effect labels and
  recorded payloads are log content and a report gets opened in a browser by someone who did not
  produce the execution. The module depends on `catalyst-core` alone — no templating engine, no web
  server. Gated by the v0.2 Timeline exit demo in CI. *Deferred (noted): latency bars and a
  `TrajectoryDiff` view (the M2 branch comparison) — the table is the read-only view the roadmap asked
  for; visual timing and diff rendering are polish on top.*

---

## Next — deployable, then distributed 🔜

v0.2 hardened the single-node runtime; this is the sequenced plan for running it in production, then
across nodes. Two tracks share one foundation. **Deploy** makes a single active node deployable and
self-recovering on today's file-backed log (a `StatefulSet` of one with a persistent volume; scale by
sharding across independent nodes). **Dist** removes the single-node limit using the design in
[`docs/distribution.md`](docs/distribution.md). Items are labelled `Deploy-n` / `Dist-n` so they do
not collide with the Gumbo items (A1–A6, D1–D4) in [`docs/gumbo-requirements.md`](docs/gumbo-requirements.md).
Each item is its own PR with its own tests, and items that touch the engine get an exit demo gated in
CI, as in v0.2.

**The shared foundation is an execution index.** `EventLog` cannot list its executions today — you
can read a stream only if you already hold its id — so nothing can find the in-flight work after a
restart. The startup recovery scan (Deploy-2) needs exactly what the distributed claim loop (Dist-4)
needs: a log-resident set of executions that may need running. Both are served by **dual-tagging**
lifecycle events into a shared tag in the same atomic append as the execution's own stream (the
Boudin technique; cursorable by version since gumbo 0.6.0). Building it once, in Deploy-1, means
Dist inherits its queue instead of inventing a second one.

### Deploy — one deployable node
Labels follow the build order (see *Order* below).

- ✅ **Deploy-1. Execution index** — dual-tag `ExecutionCreated` **and** the terminal events
  (`ExecutionCompleted` / `ExecutionFailed` / `ExecutionCancelled`) into `catalyst-tasks/<queue>`
  (default queue for now), so a fold of the queue yields the *open* set rather than every execution
  ever created. The fold is checkpointed in the tag KV like an execution snapshot (open set + queue
  version), so a scan reads only the queue tail since the last checkpoint — cost tracks open work and
  recent churn, not history. Exposed through an additive `EventLog` read, default-throwing like
  conditional append so a log that cannot enumerate says so. `InMemoryEventLog` + `GumboEventLog`;
  tests put a second execution in the log.
- ✅ **Deploy-2. Startup recovery** — `runtime.recoverAll()` (and `Catalyst.builder().recoverOnStart(true)`)
  folds each open execution (snapshot-warm) and resumes **only crash-interrupted states**
  (`NEW` / `STARTING` / `RUNNING` / `RESUMING`). `PAUSED` and `WAITING` are left alone: `pause(id)`
  and `InDoubtPolicy.ASK` record `ExecutionPaused` on purpose, and a restart must not un-pause what an
  operator or a pending in-doubt verdict is holding. Unregistered task types are reported, not fatal.
  Exit demo `Demo recover`: crash with several executions in flight plus one paused, restart, the
  in-flight ones finish with zero duplicate model calls and the paused one stays paused.
- ✅ **Deploy-3. Blob store ergonomics** — `BlobStore extends AutoCloseable` (default no-op) and a
  `.blobStore(...)` builder option, so a remote store (S3/GCS) is a builder line and closes with the
  log. The store is bound into the `EventCodec` when the log is constructed, so the option only
  applies when the builder constructs the log itself (the `embedded(path)` path); combined with
  `.log(...)` — a log the caller already built — it is **rejected** at `build()`, never silently
  ignored.
- ✅ **Deploy-4. Graceful shutdown** — needs a new engine path, not just a better `close()`. Today
  `close()` calls `executor.close()`, which blocks until every in-flight attempt finishes (a task
  waiting on a slow provider holds the pod, and the Gumbo directory lock, indefinitely), and
  interrupting a worker without the cancel token folds to `ExecutionFailed` — terminal, so Deploy-2
  would never resume it. Add an **abandon** unwind: shutdown trips an abandon token, the worker raises
  at its next live boundary (`ReplayingContext.requireAppendable`, next to the cancellation check),
  and `runAttempt` handles it in its own branch beside `CancellationSignal` — remove from `inFlight`,
  complete the future exceptionally, **append nothing** — so the execution stays `RUNNING` in the log
  and Deploy-2 resumes it on the next start. `close()` stops accepting submissions, trips abandon,
  waits up to a drain timeout, then closes the log and releases the directory lock. Exit demo: shut
  down mid-run, restart, the execution finishes with zero duplicate model calls.
- ✅ **Deploy-5. Deployment guide + sharding** — `docs/deployment.md`: single active node on a
  `ReadWriteOnce` block volume (not NFS — the directory lock is the single-writer guarantee),
  `InDoubtPolicy` and `RetryPolicy` choices for node loss, volume snapshots for DR. For scale, shard
  across independent nodes **by idempotency key**: the id is minted inside `execute()`, so a client
  holds nothing else stable to route on before submit, and the idempotency index is per-log — any
  other routing lets a same-key re-submission land on a second shard and duplicate model calls.
  Sharded deployments therefore require a key on submit, and the owning node stamps its shard into
  the `ExecutionId` so later id-addressed calls (`inspect`/`resume`/`cancel`) route without a lookup.
- ✅ **Deploy-6. Signals / `WAITING`** (from v1, pulled forward) — `await`/signal on `Context`, activating
  the reserved slot. Human-in-the-loop is what single-node users want most, and it does not need
  leases: re-enqueue on signal is a dual-tagged signal event into the Deploy-1 queue. The signal
  event **folds `WAITING` → `RESUMING`** — it is a state change, not just a wake-up — so a crash
  between the signal landing and a worker picking it up leaves an execution Deploy-2 resumes rather
  than a `WAITING` one with an unconsumed signal that nothing would ever re-enqueue. `Demo recover`
  gains that case: a waiting execution is signalled, the process crashes, and it finishes after
  restart. The lease interaction (a waiting execution releases its lease) is specified with Dist-3.
  *As shipped:* `ctx.awaitSignal(name, type)` / `runtime.signal(id, name, payload)`, events
  `SignalAwaited` / `SignalReceived`. Deliberate limits: a signal is accepted only while the execution
  is `WAITING` for it (no buffering, so nothing appends to a running stream from outside); waits have
  no timeout yet; the signal is **not** yet dual-tagged into the queue — nothing consumes the queue as
  a work feed until the Dist-4 claim loop, which adds it then.

### Dist — distributed execution
The original phasing rationale is in `docs/distribution.md`; **this list is the sequencing of record.**

- ✅ **Dist-0. Multi-writer log (mostly Gumbo-side, blocking)** — done when **all** of: the FoundationDB
  adapter wired through a `GumboEventLog` factory; a distributed sequencer; Gumbo **D2**
  (non-clobbering index) closed or shown not to apply to the FDB `kvSubspace` index, with the
  measurement; and a **CI-runnable** multi-writer configuration (e.g. a shared in-process adapter with
  a storage-side sequencer) so Dist-4's exit demo can be gated without FDB in CI. `supportsMultiWriter()`
  is only trusted once Gumbo reports it from a measured configuration — asserting it early is the D4
  mistake again.
  *As shipped* (Gumbo **0.7.0**, which answered [gumbo#38](https://github.com/CajunSystems/gumbo/issues/38)):
  `GumboEventLog.foundationDB(clusterFile, root, blobs)` pairs FDB storage with the FDB sequencer, so
  `supportsMultiWriter()` comes from Gumbo, which measures that pairing with two writer JVMs in its CI.
  D2 is shown not to apply on FDB (the index is written in the entry's transaction). **One deviation:**
  the CI-runnable configuration is a real FoundationDB in a service container (CI job `fdb`), not an
  in-process adapter — Gumbo declined that option as less honest than the real thing, and the
  container is cheap. So Dist-4's exit demo gates in that job. Catalyst's own two-node tests
  (`FoundationDBEventLogTest`, `FoundationDBAcceptanceTest`) found two Catalyst-side bugs a
  single-writer fixture hid: stream reads were seqnum-ordered (out of seq order under two writers),
  and `latestSeq` served a per-process cache. Left for Dist-4: `putKey` is a plain overwrite, so two
  nodes taking the same new idempotency key both create an execution; keyed `execute` across nodes
  needs a CAS claim on the key plus attach-or-claim on the execution.
- ✅ **Dist-1. Fenced runtime writes** — the runtime appends through `append(id, event, expectedSeq)`
  when the log supports it. **Precondition: Gumbo A6 resolved as primary-tag fencing.** Deploy-1 and
  Deploy-6 make Catalyst's lifecycle appends multi-tag (execution tag + queue tag), so the fenced
  appends *are* multi-tag appends, and A6 still leaves open what `expectedVersion` means across N
  tags. Catalyst needs it to condition on the execution tag only (the queue tag is not fenced), stated
  and tested in Gumbo before this lands. A `StaleWriterException` is handled in **its own branch in
  `runAttempt`, before the retry decision** — like the `CancellationSignal` branch: complete the
  future exceptionally with it, remove from `inFlight`, append nothing. It must **not** route through
  `isRetryable` → `recordFailed`, which appends `ExecutionFailed` — a write the fence would reject,
  on a stream this writer no longer owns. Meaningful single-node.
  *As shipped:* the A6 precondition was met by Gumbo 0.6.0 (a multi-tag conditional append names its
  fenced tag; stated and tested in Gumbo's `ConditionalAppendTest`). Every runtime append goes through a
  `StreamCursor` whose expectation is one past what that writer **read** — never the log's tip at write
  time, which would let a writer fence on history it never saw. A rejection fences the cursor out for
  good, and the attempt exits ahead of the stop rule and the retry decision, appending nothing.
- ✅ **Dist-2. Distribution guard** — distributed mode refuses to start unless the log reports **both**
  `supportsConditionalAppend()` and `supportsMultiWriter()`.
  *As shipped:* `Builder.distributed()`; `build()` reads both answers from the log and refuses with
  both in the message (closing a log the builder opened). It also refuses, before opening anything:
  a missing `nodeId(...)`, since nodes left at the default would be indistinguishable; `shard(...)`,
  since sharding is the alternative to a shared log; and `recoverOnStart`. `recoverAll()` refuses on
  a distributed runtime too. On a shared index the scan would resume work running on *other* nodes
  mid-call: their dangling `CompletionRequested` becomes in-doubt, which is terminal under
  `InDoubtPolicy.FAIL`. Dist-3/4 lift that refusal once leases say whose work is whose. The same
  hazard remains on the by-id paths, which are not refused: a keyed `execute` or `resume(id)` of an
  execution running on another node (`liveAttempt` only sees this process). Until leases exist, the
  operator must know the execution is not running elsewhere.
  `isDistributed()` reports the mode. Tests: `DistributionGuardTest` and
  `DistributionGuardAcceptanceTest` (the file-backed log is refused and its directory released).
  `FoundationDBAcceptanceTest`'s nodes run distributed.
- ✅ **Dist-3. Leases** — lease row per execution via the tag KV's CAS (gumbo 0.4.0+): claim, heartbeat,
  expiry. Resolve the open question first: whether `expectedSeq` makes a separate fencing token
  redundant. Lease duration and heartbeat interval get defaults plus a knob. A `WAITING` execution
  releases its lease; its signal re-enqueues it (Deploy-6).
  *As shipped:* **no separate fencing token.** Every stream append is already conditional on what its
  writer read. A takeover always appends `ExecutionResumed` before doing any work, so the old
  holder's next append is refused. The lease row itself changes only by CAS on its exact bytes. The
  lease decides *who should run*, and the fence keeps the stream correct.
  - **Storage:** `Lease(holder, expiresAtMillis)` on `EventLog` (`supportsLeases` / `readLease` /
    `compareAndSetLease`; the default throws). `GumboEventLog` keeps it in a KV-only tag through
    Gumbo's `compareAndSetValue`, and Dist-2's guard now requires `supportsLeases()` as well.
  - **Claiming:** a distributed runtime claims the lease before any write: in `scheduleAttempt`
    (every attempt except terminal replay), and via `underLease` for create, signal, pause and cancel
    of an execution not running here. A live lease held elsewhere → `LeaseHeldException`, before
    writing. That closes Dist-2's by-id hazard: keyed `execute` and `resume(id)` of another node's
    running execution are refused.
  - **Renewal:** a heartbeat (`lease(duration, heartbeat)`, defaults 30s/10s) renews held leases. A
    renewal that finds the row changed sets the attempt's `leaseLost`, which the token reads as an
    abandon. The attempt unwinds at its next boundary, appends nothing, and its handle fails with
    `LeaseLostException`.
  - **Release:** when the attempt ends, whatever the outcome, `WAITING` included. `close()` releases
    the rest after the drain.
  - **Recovery:** `recoverAll()` / `recoverOnStart` are allowed again on a distributed runtime: a live
    lease held elsewhere → `leftAlone` RUNNING, while a lapsed one is taken over. The scan decides
    each execution while holding its lease, and a resume re-folds after claiming (claim, then
    decide), so a node that finished, paused or parked the execution just before the claim can't be
    overridden. A lease whose
    holder is this node's own id counts as this node's, so a node restarted under its id needn't wait
    out its predecessor (node ids must be unique among live nodes).
  - **Clocks:** expiry is wall-clock from the writer's runtime `Clock`. Skew moves takeovers early or
    late, never corrupts — but an early takeover leaves the old holder's in-flight call in doubt,
    terminal under the default `InDoubtPolicy.FAIL`, so lease duration is set well above pauses
    (`FoundationDBAcceptanceTest` uses a skewed node to show the fence catching
    exactly that).
  - **Left to Dist-4:** the signal side ("re-enqueues it"). A signal on any node already resumes the
    execution, but nothing watches the queue yet.
- ✅ **Dist-4. Claim loop + cross-node control** — each node subscribes to the Deploy-1 queue, claims via
  Dist-3, runs `resume(id)`, and reclaims expired leases. **Control ships in the same item**: today
  `cancel(id)`/`pause(id)` append `ExecutionCancelled`/`ExecutionPaused` directly when the execution
  is not in flight *in this process*, which across nodes is a second writer on a stream another node
  owns. So control ops become lease-gated — a node that does not hold the lease records a control
  request in the tag KV, and the owner observes it at its next boundary and appends the event itself
  (an unleased execution is claimed first, then controlled). **Keyed submission too:** `putKey` becomes
  a CAS claim (`setValueIfAbsent`), and a node that loses the claim attaches to the winner's execution
  rather than creating its own (see Dist-0's note). Exit demo `Demo distributed`: two
  runtimes on the Dist-0 CI multi-writer log, one killed mid-run, the other reclaims and finishes with
  zero duplicate model calls, the zombie's late write is rejected by the fence, and a cancel issued
  from the non-owner lands via the owner.
  *As shipped:*
  - **Claim loop:** it *polls* the index (`claimInterval`, default 10s) rather than subscribing, since
    Gumbo's push subscriptions only see the subscribing process's own appends. Each tick is
    `recoverAll`'s scan. An execution is folded without the lease, and only one a crash could have
    left is claimed and re-decided under the lease. So parked executions cost no lease writes.
  - **Signal re-enqueue:** a signalled execution whose node can't run it folds to `RESUMING`, so the
    loop gives it to a node that can.
  - **Cancel:** rides **on the lease row**, not a separate KV key. The non-owner adds `cancelRequest`
    by CAS. The owner's next renewal finds its own row plus a request, keeps the lease, and trips the
    attempt's cancel locally, so it records `ExecutionCancelled` itself. A takeover carries the
    request and honours it before running. A release never deletes an unhonoured request on a
    non-terminal execution; it leaves the row lapsed. The loop records a request orphaned on a parked
    execution.
  - **Pause:** of an execution running elsewhere stays refused (`LeaseHeldException`), as pausing a
    running execution is refused locally.
  - **Keys:** `EventLog.claimKey` (CAS). A loser resolves to the winner's id. A key whose winner
    never created the execution is created under that id by whoever holds the lease, fenced at seq 0.
  - **Exit demo:** `Demo <root> distributed-a` / `distributed-b`, gated in CI's `fdb` job, and run
    locally with `scripts/fdb-tests.sh --demo`.
- ⏸ **Dist-5. Push placement (optional) — deferred.** Cajun `ClusterActorSystem` behind `KeyedLock`, replacing
  subscription-plus-claim with assignment. An optimisation only; correctness stays in Dist-1/Dist-3.
  *Deferred (2026-10-12):* Cajun's clustered actor system is still experimental, and nothing
  depends on push placement. The Dist-4 claim loop already gives correct failover. Revisit when
  Cajun's clustering is stable **and** there is measured lease contention or claim-latency pain to
  justify it.

### Order
**Deploy-1 → Deploy-6** in label order — the track is *deployable* after Deploy-5, and signals
(Deploy-6) follow rather than gate it. Then **Dist-1** (once Gumbo A6 is settled) → **Dist-0** (lands
mostly in Gumbo) → **Dist-2 → Dist-3 → Dist-4**, then the remaining v1 platform items below. Eval
harness and the timeline polish (latency bars, `TrajectoryDiff` view) are independent and can slot in
anywhere.

---

## v1 — Agents, signals, distribution 🔭

The substrate becomes a platform. This is where the other CajunSystems components integrate.

### Agent abstraction (spec §12)
- An **`Agent`** built *on* `Task` (reasoning loops, tool selection) — a consumer of the runtime, not
  part of its core. Possibly delegate planning to Embabel rather than competing.
- **MCP** integration and richer tool ecosystems.

### Signals & human-in-the-loop — Boudin integration (spec §5 lifecycle, §12)
*Pulled forward as Deploy-6 above; Boudin workflow invocation stays here.*
- Activate the reserved **`WAITING`** state: a task pauses, awaits an external signal, schedules
  future work, or invokes a long-running Boudin workflow. Human-in-the-loop lands here.
- `await`/signal APIs on `Context` — no schema change needed (the slot is reserved).

### Distributed execution (spec §12)
*Sequenced as `Dist-0`…`Dist-5` above; the bullets below are design and status, not order.*
- Execution across many nodes over a shared **Gumbo cluster** — same `EventLog` SPI, history and
  replay preserved across nodes. Design recorded in [`docs/distribution.md`](docs/distribution.md):
  **the log is the arbiter, not a coordination service.** Single-writer-per-execution is enforced by
  a *conditional append* (`append(id, event, expectedSeq)`) rather than by a distributed lock, so a
  stale writer is rejected by storage even if the coordination layer is wrong. Nodes then compete for
  work through shared storage — lease CAS to claim, the existing `resume(id)` to run, lease expiry to
  reclaim — with no membership protocol, no leader election and no seed list: start another instance
  and it participates. That makes placement an *optimisation*, not a correctness dependency, so
  **Cajun**'s `ClusterActorSystem` can be swapped in behind `KeyedLock` (push-based assignment
  instead of polling) whenever it is ready, and backed out cheaply if it is not. **Bayou** does not
  distribute — it is single-process — but it shares Gumbo with Catalyst, so it is useful *within* a
  node (supervising the claim loop, lease-renewal timers, death watch) and could later consume the
  same primitives to become clustered itself.
- ✅ **The `EventLog` seam for conditional append has landed** — `append(id, event, expectedSeq)` plus
  `supportsConditionalAppend()`, rejecting a stale writer with `StaleWriterException`. The default
  **throws** rather than falling back to an unconditional append: a log that quietly ignored
  `expectedSeq` would look like it was participating in the protocol while providing none of it,
  which surfaces as a corrupted history rather than as an error. `InMemoryEventLog` fences under the
  monitor that assigns the seq; `GumboEventLog` delegates to Gumbo's own fence, where the compare and
  the increment are one storage operation — Catalyst's `seq` *is* the execution tag's
  `streamVersion`, so there is no translation in between. Covered by `ConditionalAppendTest` and
  three `GumboEventLogTest` cases (with a second execution in the log), each verified to fail when
  the fence is removed. What remains for distribution is the claim loop above it, not the primitive.
- ✅ **Both capability answers are now asked for, not asserted** — `supportsConditionalAppend()` and
  the new `supportsMultiWriter()` delegate to Gumbo 0.5.0's `capabilities()`. The first used to be a
  hardcoded `true`, correct for every adapter Gumbo ships and wrong in shape: a client asserting a
  guarantee on storage's behalf is the mistake that produced D4. Read them as a pair — a file-backed
  log is fenced and *not* multi-writer, which is precisely the configuration a runtime must refuse
  to distribute over. **No log Catalyst builds today reports `multiWriter`**, because Gumbo composes
  it from storage *and* the sequencer, and the default sequencer is a per-process counter that no
  storage fence can compensate for.
- **Status of the storage-side prerequisites**, all measured rather than assumed. Gumbo 0.3.0 closed
  the blocker: versions are assigned *in storage* now, not by a per-process counter, so the two-JVM
  probe that handed both writers `seq` 0,1,2 no longer applies — and the file adapter takes an
  exclusive directory lock, so a second process is refused loudly instead of corrupting silently.
  Lease CAS arrived in 0.4.0. Two gaps are left, and both are Gumbo-side:
  - ~~**0.4.0 is merged but untagged**~~ — tagged and picked up. Catalyst is now on gumbo **0.6.0**:
    the lease CAS (A3), declared capabilities (A4) and per-tag stream versions are all reachable.
    Nothing consumes the CAS yet; the claim loop is what would.
  - ~~**Claimable work** … the queue tag cannot be cursored by version~~ — **fixed in gumbo 0.6.0**
    and adopted. Every tag an entry carries now has its own dense position, so a shared
    `catalyst-tasks/<queue>` fed by dual-tagged appends is cursored with `readAfterVersion` like any
    other stream, and a worker cannot be handed an item numbered below its cursor. It cost a
    record-layout change but no migration — a per-record marker lets both layouts coexist. Pinned
    from this side by `GumboEventLogTest.aFanOutTagIsCursoredByItsOwnVersion`; the Deploy-1
    execution index is its first consumer.

### In-doubt model completions (found while designing distribution)
- ✅ **Closed.** A crash between `CompletionRequested` and `CompletionReceived` left the provider call
  **in doubt** — it may have been accepted and billed with only the result lost on the way back — and
  a resume re-issued it. Measured before the fix: a log ending at `CompletionRequested` resumed and
  invoked the model a second time. Catalyst already handled this for **tools** (`seed()` detects a
  `ToolRequested` with no `ToolCompleted` and routes recovery through `InDoubtPolicy`); the model
  path had no equivalent, because a trailing `CompletionRequested` set `pendingRequestHash` and was
  otherwise ignored. It now routes through the same `InDoubtPolicy`, keyed on the recorded
  `requestHash` so a recovery cannot attach itself to a different question (a divergent request at
  that boundary is a `NonDeterministicReplayException`). A `RETRY` **completes the dangling request**
  rather than opening a second one, so a recovered log is one request paired with one result and
  replays like an execution that never crashed. The discriminator that keeps retry semantics intact:
  a `RetryRequested` recorded after the request is a *verdict*, not a doubt — the provider call threw
  and the process lived to say so — and re-runs live as before. That distinction is pinned by its own
  test, because getting it wrong turns every retried model failure into an `InDoubtException` under
  the default policy. This is what "zero duplicate model calls" needs in order to hold under node
  failure rather than only under graceful resume; distribution merely makes the window far more
  frequent.

### Eval harness (spec §12)
- Recorded production executions replayed against **candidate models/prompts** as a regression suite
  — the log *is* the dataset. Branch + diff (M2) is the primitive; the harness batches and scores it.

### Memory & retrieval
- **Memory types** (episodic/semantic) behind a common interface; **embeddings / vector stores /
  RAG** as opt-in modules. Deliberately excluded from v0 (spec §10); they return here as consumers,
  not core.

---

## Guiding constraints (unchanged)

- **Immutable, append-only history** — everything derives from events; nothing mutates.
- **Model-agnostic core** — providers arrive through adapters; Catalyst never maintains provider
  HTTP clients.
- **Additive schema** — reserved event slots mean new phases don't break existing logs.
- **CI as source of truth** — every milestone keeps an executable exit demo gated in CI.

See the v0.1 specification for the full design rationale and the open questions (§13) that these
phases resolve.
