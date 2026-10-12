# Catalyst — notes for Claude

Catalyst is a **durable AI execution runtime for the JVM** (Java 21, Maven multi-module). This repo
implements all three v0.1 milestones plus most of v0.2: **M0** (*execute + record + resume*), **M1** (*replay +
inspect* — strict canonical-hash replay with `NonDeterministicReplayException`, `replay(id, task)`,
typed token/cost timeline via `ExecutionState.timelineView()` + pluggable `CostModel`), and **M2**
(*branch + diff* — `ReplayMode.BRANCH` forks on divergence, `runtime.branch(id, atSeq)` with model /
counterfactual-tool swaps, `Trajectory.diff`).

## Build & test

```bash
mvn install            # full reactor + tests
mvn -pl catalyst-core -am install   # a single module and its deps
```

**Gumbo dependency:** `catalyst-gumbo` depends on **`com.github.CajunSystems:gumbo:0.7.0`**,
resolved from JitPack (declared in the root pom) — an ordinary build needs no setup. The groupId is
*not* the `com.cajunsystems` that Gumbo's own pom declares; JitPack rewrites it to
`com.github.{owner}` on publish. So a hand-built `mvn -f /path/to/gumbo/pom.xml install -DskipTests`
installs `com.cajunsystems:gumbo`, which this build does not reference and which therefore will not
satisfy the dependency. To work from a local Gumbo build, rewrite the project groupId in that
checkout's pom to `com.github.CajunSystems` before installing.

## Architecture (where things live)

- `catalyst-events` — sealed `CatalystEvent` hierarchy + `EventCodec` (Jackson) + `BlobStore`
  (content-addressed offload of large payload fields, in-memory + `FileBlobStore`). Schema-stable; keep
  changes additive.
- `catalyst-core` — SPIs (`Task`, `Context`, `Model`, `Tool`, `Memory`, `EventLog`), the pure
  `Reducer` fold, and `ReplayingContext` — the record/substitute engine that makes resume/replay
  work. This is the heart of the system.
- `catalyst-runtime` — `CatalystRuntime` (virtual-thread scheduler, lifecycle, idempotency) +
  `InMemoryEventLog`.
- `catalyst-gumbo` — `GumboEventLog`: one Gumbo `LogTag` per execution; Gumbo `streamVersion` ==
  Catalyst `seq`; durable KV for the idempotency index. Tail reads must be **version-keyed**
  (`readAfterVersion`), not `readAfter` — the latter is keyed on the log's global `seqnum`, which
  equals a stream's own numbering only while the log holds one execution (see "Gumbo 0.3.0" below).
- `catalyst-tools`, `catalyst-api` — built-in tools (`ClockTool`, `CalculatorTool`, `HttpTool`,
  `FilesystemTool`) and the `Catalyst` facade.
- `catalyst-langchain4j` — `LangChain4jModel`: wraps any LangChain4j `ChatModel` (real providers).
  Depends only on `langchain4j-core`; the app supplies the provider. Tested offline with a fake
  `ChatModel` (override `doChat`).
- `catalyst-agent` — `AutoCaptureAgent`: a Byte Buddy agent that rewrites the JDK's nondeterministic
  call sites (`Instant.now()`, `UUID.randomUUID()`, `Math.random()`, `Random` draws) inside configured
  task packages into calls on `catalyst-core`'s `AutoCapture` bridge. The bridge lives in **core**, not
  here, so instrumented classes link without the agent. Depends on Byte Buddy; tests fork a JVM per
  class (agent installs are JVM-wide).
- `catalyst-timeline` — `TimelineReport`: renders a folded `ExecutionState` as a self-contained HTML
  page (header + roll-ups + step table). Read-only, post-hoc, pure function of the log; depends on
  `catalyst-core` only — no templating engine, no web server. All interpolated log content is escaped.
- `catalyst-otel` — `CatalystTracer`: folds an execution's event log into an OpenTelemetry trace
  (root span + per-boundary child spans + lifecycle annotations). Read-only, post-hoc, no runtime hook.
  Depends on the OpenTelemetry **API** only; the app supplies the SDK + exporter. Tested offline with
  the SDK's `InMemorySpanExporter`.

## Key invariants

- Every side-effecting boundary goes through `Context` and is recorded as an event. On replay/resume
  it is **substituted** from the log (no re-execution) until the task runs past the log tail.
- `seq` is dense and per-execution; it lives on `SequencedEvent`, not on the event itself.
- Determinism contract: task code between boundaries must be deterministic (Temporal-style).

## Acceptance tests / exit demos (keep green if you touch the engine)

- **M0** — `catalyst-api` → `M0ResumeAcceptanceTest` + `Demo record|resume`: crash after step 1,
  resume, finish with zero duplicate model calls.
- **M1** — `M1ReplayAcceptanceTest` + `Demo replay`: replay a recorded execution with zero external
  calls, canonical hashes verified, and a divergent replay raising `NonDeterministicReplayException`.
- **M2** — `M2BranchAcceptanceTest` + `Demo branch`: rerun a recorded execution with a different
  model from step N and diff the trajectories (only the post-branch step changes).
- **v0.2 Snapshots** — `SnapshotAcceptanceTest` + `Demo snapshot`: a long execution is checkpointed so
  `inspect` folds from the latest snapshot forward (warm inspect reads only the log tail, not the whole
  log) and the snapshot fold matches a full re-fold exactly. The reducer is resumable via
  `Reducer.foldFrom(ReducerState, events)`; the `EventLog` seam is `readFrom` + `readSnapshot`/`writeSnapshot`.
- **v0.2 Resume-by-id** — `ResumeByIdAcceptanceTest` + `Demo resumeid`: a crashed execution is recovered
  from its id alone via a `TaskRegistry` (`runtime.resume(id)`) — no idempotency key, no re-submitted
  `Task` — finishing with zero duplicate model calls. Register task types up front with
  `Catalyst.builder().task(...)`; use named `Task` classes (lambda class names aren't stable across
  processes). A terminal execution's `resume(id)` replays its recorded outcome without re-running.
- **v0.2 Built-in tools** — `ToolsAcceptanceTest` + `Demo tools`: a task fetches over HTTP (`HttpTool`,
  with a pluggable `Sender` seam so tests run offline) and writes to a sandboxed `FilesystemTool`; a
  strict replay substitutes both recorded boundaries — the request is not re-issued and the write is
  not re-applied. `FilesystemTool` is sandboxed to a root dir and rejects `..`/absolute/symlink
  escapes; both tools are non-`@Deterministic` (their outputs are recorded, not re-executed).
- **v0.2 Schema evolution** — `SchemaEvolutionTest` + `SchemaEvolutionAcceptanceTest` + `Demo schema`: a
  log recorded under an older schema (renamed `@type` + field, plus an unknown field) reads and folds
  under the current schema. Policy (`docs/schema-evolution.md`): tolerant reader for additive changes +
  an `EventUpcaster` chain (applied on decode, after blob rehydration) for structural changes, wired via
  `EventCodec.builder()` / `GumboEventLog.at(path, upcasters)`. Version stamping deferred until the first
  breaking change (absent ⇒ v1).
- **v0.2 Blob store** — `BlobStoreAcceptanceTest` + `Demo blob`: a payload over the offload threshold
  (default 64 KiB) is stored out-of-line in a content-addressed `BlobStore` (durable `FileBlobStore` under
  `path/blobs`, SHA-256 refs, dedup) and rehydrated transparently on inspect/replay — offloading lives at
  the `EventCodec` seam (`encode` externalizes large top-level payload fields, `decode` inlines them), so
  the core only ever sees fully-inlined events. Small events stay byte-identical (old logs interoperate).
  **Deploy-3:** `BlobStore extends AutoCloseable` (default no-op) and the log **owns** the store bound
  into its codec — `GumboEventLog.close()` closes it (in a `finally`). A remote store is one line on
  the embedded path: `Catalyst.builder(path).blobStore(store[, threshold])`. That works through the
  runtime builder's `logFactory(EventLogFactory)` seam (runtime can't depend on gumbo, so the facade
  supplies the factory). `blobStore(...)` with `log(...)` is **rejected** at `build()` (the codec is
  fixed once a log exists — never silently ignore an option), as is `log` + `logFactory`. Tests:
  `BuilderLogOptionsTest`, `BlobStoreAcceptanceTest.aSuppliedBlobStore...`, `GumboEventLogTest.closingTheLogClosesItsBlobStore`.
- **v0.2 Collection payloads** — `CollectionPayloadAcceptanceTest` + `Demo collections`: a task captures a
  `List`/`Map` of records (via `ctx.effect` or as its result); `PayloadCodec` encodes collections/arrays
  structurally (each element in its own typed envelope, recursively) so element types survive the
  round-trip, and a strict replay substitutes the recorded collection with fidelity intact. Leaf
  encoding is unchanged (old logs interoperate); the allowlist holds at every nested leaf.
- **v0.2 Cancellation** — `Demo cancel`: a running task is cancelled cooperatively and folds to
  `CANCELLED` (not `FAILED`). `cancel(id)` records `ExecutionCancelled`; while the execution is in
  flight it trips a `CancellationToken` and interrupts the worker, which unwinds at its next live
  boundary (checked in `ReplayingContext.requireAppendable`) and records the event itself — so no other
  thread ever appends to a running execution's stream. Attaching to a cancelled execution surfaces a
  `CancellationException`.
- **v0.2 Retry semantics** — `RetryAcceptanceTest` + `Demo retry`: a transient tool failure is retried
  as a new attempt on the same stream (retry-as-attempt). A retryable failure appends `RetryRequested`
  (carrying the failed boundary's `failedSeq`) instead of `ExecutionFailed`, then re-enters the task as
  a resume — the successful prefix is substituted and only the failing boundary re-runs live (`seed()`
  drops the retried `ToolCompleted(error)` so it is not substituted; model/effect failures record
  nothing and already re-run). A pluggable `RetryPolicy` (`none()` default, `maxRetries`, `exponential`)
  bounds it, set via `Catalyst.builder().retryPolicy` or per-execution `ExecutionOptions.retryPolicy`.
  Retries fold to a crash-safe `retries` counter distinct from `attempt`. Retryability is gated in the
  runtime (`isRetryable`: excludes `NonDeterministicReplayException`, `InDoubtException`,
  `InterruptedException`, `Error`) before the policy is consulted. Whole-task, not per-tool. A retried
  log still replays exactly.
- **v0.2 Auto-capture** — `AutoCaptureAcceptanceTest` + `Demo autocapture`: a task containing plain
  `Instant.now()` / `UUID.randomUUID()` / `Random` calls and **no** `ctx.effect` is recorded and
  replayed exactly, because `catalyst-agent` rewrote those call sites into `AutoCapture` calls that
  record through the bound `Context` as effect boundaries (`auto:<source>#<n>`, counted per attempt —
  a resume/replay re-enters the task from the top, which realigns the counter). The uninstrumented
  control run is half the criterion: without capture, replay raises **nothing** and silently
  recomputes different values. `CatalystRuntime` binds the scope (`AutoCapture.bind`) around all three
  `task.execute` sites, per-attempt so retries realign. Capture is suppressed inside boundaries that
  already record a result (`ctx.effect` suppliers, tool bodies) — otherwise a nested capture appends
  its event *before* the enclosing one and desynchronises the boundary queue. `PayloadCodec` allowlists
  `UUID` + the `java.time` value types for the same reason. Gotchas that bit: Byte Buddy skips
  synthetic methods by default (exempting every lambda body — the agent overrides `ByteBuddy.ignore`),
  and `Random` does not declare `nextInt(int,int)` (draws match by declaring type against
  `RandomGenerator`).
- **v0.2 Streaming** — `StreamingAcceptanceTest` + `Demo streaming`: a task consumes a completion
  incrementally via `ctx.model().stream(request, TokenSink)` and still replays exactly. **Streaming is
  a delivery concern, not a durability one** — the recorded boundary is the assembled `Completion`,
  i.e. the same single `CompletionReceived` a non-streaming call writes, so the schema is unchanged and
  a streamed log is interchangeable with a plain one on replay. `Model.stream` is a **default method**
  (delegates to `complete()`, one chunk) so every existing `Model` still compiles and `Model` stays
  functional. `ReplayingContext.model()` now returns a `RecordingModel` (was a method ref) folding both
  entry points onto one `modelBoundary`. Chunk boundaries are NOT recorded — replay re-emits the
  recorded text as one chunk (token-level replay deferred), so tasks may accumulate chunks but must not
  branch on chunk counts. The sink is wrapped so auto-capture is **suppressed** while it runs: it
  executes between `CompletionRequested` and `CompletionReceived`, and a capture there would append
  into that gap and break replay. `LangChain4jModel.streaming(...)` bridges LangChain4j's async
  callbacks over a `BlockingQueue` so the sink runs on the **task's** thread, not the provider's.
- **v0.2 Timeline UI** — `TimelineAcceptanceTest` + `Demo timeline`: a folded execution renders to a
  self-contained HTML report via `TimelineReport.html(state)` / `writeTo(state, path)`. Same shape as
  the OTel exporter: consumes `inspect(id)` (a fold), no runtime hook, so the log stays the only source
  of truth. Two properties are gated because they are what make a report useful: **self-contained**
  (no scripts/external refs — portable, openable from `file://`) and **deterministic** (a pure fold, so
  reports diff cleanly). Everything interpolated is HTML-escaped — tool names, effect labels and
  payloads are log content. Oversized payloads are elided at 2000 chars rather than inlined.
- **v0.2 Observability / OTel exporter** — `OtelAcceptanceTest` + `Demo otel`: an execution's event log
  folds into one OpenTelemetry trace via `catalyst-otel`'s `CatalystTracer.export(id, events)` — a root
  span for the run, a child span per boundary (model/tool/effect/memory; model/tool carry real latency,
  model spans carry that call's tokens/cost/finish reason), and lifecycle moments (retry, pause, etc.)
  as span events on the root. Root status is OK/ERROR from the folded terminal state. Read-only and
  post-hoc (consumes `runtime.log().read(id)`), so the log *is* the trace. The module depends on the
  OTel **API** only; tests drive a real SDK into an in-memory exporter offline.
- **Gumbo 0.3.0** — `GumboEventLogTest` (two shared-log cases + the single-writer case) +
  `SnapshotAcceptanceTest.warmInspectMatchesColdWhenAnotherExecutionSharesTheLog`: the log-layer fixes
  from `docs/gumbo-requirements.md`. The one that mattered is **D4** — Catalyst's `seq` is a per-tag
  position, but `readFrom` passed it to `readAfter`, which is keyed on the log's *global* `seqnum`. The
  two numbers are equal only while a log holds one execution, so with a second execution present the
  snapshot warm read returned the whole stream and the reducer re-folded the snapshot's own prefix —
  every timeline step, token count and cost double-counted. Fixed by `readAfterVersion`;
  `latestSeq`'s cold path is `getLatestVersion()`. **0.3.0 does not fix this on its own** — it adds the
  version-keyed API, and Catalyst has to call it, which is why the upgrade alone left the whole suite
  green. Also: `localId` → `streamVersion` (0.2.0 accessors still work, deprecated), and the file
  adapter now takes an exclusive directory lock — `GumboEventLog` surfaces its message rather than
  burying it, and the lock is released on close/process death so crash→resume still reopens.
  **When adding a log-layer test, put a second execution in the log**: a single-execution fixture is
  the one configuration where these bugs are invisible, which is how D4 shipped.

- **Conditional append (v1 distribution seam)** — `ConditionalAppendTest` (runtime) +
  three `GumboEventLogTest` cases: `EventLog.append(id, event, expectedSeq)` rejects a writer the
  stream has moved past with `StaleWriterException`, and `supportsConditionalAppend()` says whether a
  log can do it at all. The default **throws** — never falls back to an unconditional append, since a
  log that ignored `expectedSeq` would look like it was fencing while fencing nothing. `GumboEventLog`
  delegates to Gumbo's storage-side fence (Catalyst's `seq` *is* the tag's `streamVersion`, so no
  translation); the conflict arrives wrapped in `LogWriteException` inside `CompletionException`, so
  it is matched on the whole cause chain, not the top type. **The runtime writes through it (Dist-1)**
  — see below; `KeyedLock` + `inFlight` remain the in-process invariant, the fence is what holds across
  writers.
  Both capability answers **delegate** to Gumbo's `capabilities()` (0.5.0+) rather than asserting:
  `supportsConditionalAppend()` and `supportsMultiWriter()`. Read them as a **pair** — a
  file-backed log is fenced (compare and append under its own monitor) and *not* multi-writer, and
  distribution needs both. Note Gumbo composes `multiWriter` from storage **and** its sequencer, so
  only `GumboEventLog.foundationDB(...)` reports `true` (Dist-0, below): the default sequencer is a
  per-process `AtomicLong`, which no storage fence can compensate for since the seqnum never passes
  through it.
- **Fan-out tag cursors (v1 claimable work)** — `GumboEventLogTest.aFanOutTagIsCursoredByItsOwnVersion`:
  since gumbo **0.6.0** every tag an entry carries has its own dense position, so a shared
  `catalyst-tasks/<queue>` fed by dual-tagged appends can be cursored with `readAfterVersion` like
  any other stream. Before 0.6.0 a queue entry inherited its execution's version and a worker could
  silently skip work numbered below its cursor — `docs/distribution.md` carried that caveat and no
  longer does. The execution index (below) is the first consumer; the test pins the property
  directly so a regression underneath surfaces here rather than as work that is never found.
  `GumboEventLog.service()` is package-private for it.
- **Execution index (Deploy-1)** — `ExecutionIndexTest` (runtime) + five `GumboEventLogTest` cases:
  `EventLog.openExecutions()` lists executions created and not yet terminal, in creation order — what
  a restarted runtime needs to find in-flight work without holding ids. Paired with
  `supportsExecutionIndex()`; the default **throws**, since an empty list would tell a recovery scan
  "nothing to recover". Membership lives in one place, `ExecutionIndex.changeFor`: opens at
  `ExecutionCreated`, closes at Completed/Failed/Cancelled; paused stays open. Branch children are
  **not** indexed (they start with `ExecutionBranched`, and their overrides aren't recorded, so a
  restart can't resume them). `GumboEventLog` dual-tags those events into `catalyst-tasks/default`
  in the same append, always **naming the execution tag** as the fenced/reported one (Gumbo
  otherwise picks `Set.iterator().next()`, salted per JVM — the returned seq could be the queue's),
  and `decode` reads `entry.streamVersion(execTag)` for the same reason. The scan folds from a
  checkpoint in the queue tag's KV (`open-set`: queue version + ids) and reads only the tail; the
  checkpoint is a plain overwrite because resuming the fold from *any* earlier checkpoint gives the
  same set — a lost/stale one costs a read, never a wrong answer. The checkpoint starts with a
  **format byte** (`OPEN_SET_FORMAT`); an unknown format is refolded, never misread. A closing event
  is dual-tagged only if its execution was **opened** (`GumboEventLog.wasOpened`: an in-process map
  of open/branched ids, else the stream's first event) — so a branch child's terminal event stays
  out of the queue. Only `ExecutionBranched` at **seq 0** marks a branch child; the BRANCH-replay
  divergence marker appears mid-stream of an indexed execution. Checkpoint format 2 adds a trailing
  **CRC32**; any checkpoint that doesn't verify or parse — including a `RuntimeException` like a blank
  id — is refolded, never thrown (a throwing checkpoint would fail every scan and every
  `recoverOnStart`). Executions recorded before the index existed are not in it.

- **Startup recovery (Deploy-2)** — `RecoveryTest` (runtime) + `RecoveryAcceptanceTest` +
  `Demo <dir> crashmany` then `Demo <dir> recover` (two processes, like M0): `runtime.recoverAll()`
  walks `openExecutions()`, folds each (snapshot-warm) and calls `resume(id)` **only** for
  crash-interrupted states (`NEW`/`STARTING`/`RUNNING`/`RESUMING`). **`execute()` creates the
  execution under the id lock** — `ExecutionCreated` makes it visible in the index, so creating it
  before the lock let a concurrent `recoverAll()` resume it and `execute` start a second writer
  (`RecoveryTest.aScanRacingExecuteNeverStartsASecondWriter`, verified to fail without the fix). `PAUSED`/`WAITING` are recorded
  on purpose (`pause`, `InDoubtPolicy.ASK`) and a restart must not undo them. An execution already
  in flight here is left alone (reported `RUNNING`), so it is safe on a live runtime. Each decision
  runs under `byExecution` (reentrant, so it can call `resume` inside). Per-execution failures —
  unregistered task type, unreadable stream — go to `RecoveryReport.notRecovered`, never thrown: one
  retired task type must not block every other recovery. `Builder.recoverOnStart(true)` runs it in
  `build()` (report via `startupRecovery()`) and **refuses** a log without an index rather than start
  a node that silently recovers nothing; if the scan itself throws, `build()` closes the log only if
  the builder opened it. Resumes use no vars (only `argsHash` is persisted).

- **Graceful shutdown (Deploy-4)** — `ShutdownTest` (runtime) + `ShutdownAcceptanceTest` + `Demo shutdown`:
  `close()` **abandons** running attempts instead of failing them. One runtime-wide `volatile closing`
  is every attempt's abandon flag (`CancellationToken(BooleanSupplier)`), so a single write covers
  attempts scheduled concurrently with shutdown. `ReplayingContext.requireAppendable` raises
  `ExecutionAbandonedSignal` (after the cancel check — cancel wins) **before** a boundary records
  anything; `runAttempt` handles it in its own branch before the retry decision and appends
  **nothing**, so the execution stays `RUNNING` and Deploy-2 recovery resumes it. **One stop rule** at both
  exits of an attempt: stopped by a signal (raised, or unwound by one) with a cancel pending →
  `CANCELLED`, whichever signal stopped it and in whatever order (a cancel arriving while a task runs
  the fallback for a swallowed abandon must not be undone by recovery); else stopped by abandon →
  abandon. A *genuine* failure during the drain goes through the normal retry/fail
  decision — essential for model calls: `CompletionRequested` is appended before the provider call,
  so an abandoned provider failure would leave it dangling → in-doubt on restart → terminal under
  `InDoubtPolicy.FAIL`. A retryable one writes `RetryRequested`, then abandons instead of sleeping. A
  late worker whose appends fail on a closed log still writes nothing (`recordFailed` is best-effort).
  **Abandon never interrupts**: interrupting mid-provider-call leaves a dangling `CompletionRequested`
  → in-doubt on resume. The in-flight call finishes and records; the unwind happens at the next
  boundary. So the retry backoff sleep polls the token in 50ms slices rather than relying on an
  interrupt. `close()` refuses new work (`requireOpen`, plus `RejectedExecutionException` handling for
  the race), waits `drainTimeout` (default 10s), then closes the log (releasing the Gumbo dir lock);
  idempotent **and a barrier** (a concurrent second `close()` waits on a latch until the log is closed).
  From a task thread of the same runtime it can't wait (the drain includes the caller), so it hands
  the drain + `log.close()` to another thread and returns — every attempt still gets the full drain.
  Test such paths with a log that refuses writes after close; `InMemoryEventLog.close()` is a no-op
  and hides log-closed-too-early bugs. If
  `close()` lands after `execute` recorded `ExecutionCreated`, `scheduleAttempt` returns a *failed
  handle* (never throws) — the same result as the `RejectedExecutionException` race.
  `isShuttingDown()` for readiness probes. **Swallowed stop signals:** both signals are
  `RuntimeException`s, so a task with `catch (RuntimeException)` around a boundary can swallow them
  and return normally. `ReplayingContext` records that it raised one (`raisedCancellation()` /
  `raisedAbandon()`), and `runAttempt` honours that on the success path (and the failure path, for
  cancel) instead of recording `ExecutionCompleted` with a result the stop produced.

- **Deployment guide + sharding (Deploy-5)** — `docs/deployment.md` + `ShardRingTest` (core) +
  `ShardingTest` (runtime) + `ShardingAcceptanceTest` (three Gumbo shards behind a client router;
  re-submitting every key makes zero new model calls). Scale-out is **independent nodes, each its own
  log, routed by idempotency key** — the only thing a client holds before submit, and the idempotency
  index is per-log, so any other routing duplicates model calls on re-submission. `ShardRing.shardFor`
  is rendezvous hashing over SHA-256; its outputs are **pinned in the test** — changing them re-routes
  every key, which is a data break. `Builder.shard(self, ring)` makes the node enforce the contract
  **before writing**: no key → refused, key routed elsewhere → refused, ids minted
  `exec-<uuid>@<shard>` (`ExecutionId.shard()` parses only that exact minted shape), and id-addressed
  calls (`inspect`/`resume`/`cancel`/`pause`/`replay`/`branch`) refuse another shard's id; unstamped
  (pre-sharding) ids pass. The shard set is fixed for the life of the data. Not failover: a shard's
  work waits for that node — node takeover is the Dist track.

- **Signals / WAITING (Deploy-6)** — `SignalTest` (runtime) + `SignalAcceptanceTest` + `Demo signal`,
  and `Demo crashmany`/`recover` gained a waiting (left alone) and a signalled-then-crashed (finished)
  execution. `ctx.awaitSignal(name, type)`: substituted if `SignalReceived` is recorded; else records
  `SignalAwaited` (→ `WAITING`) and throws `ExecutionWaitingSignal` — the attempt ends, **no thread
  held**, runtime appends nothing more. `SignalAwaited` is not a boundary; `SignalReceived` is (the
  wait's result, identity = name). `runtime.signal(id, name, payload)` accepts only `WAITING` *for that
  name* and not in flight — **no buffering**, so nothing ever appends to a running stream from
  outside — appends `SignalReceived` (→ `RESUMING`, a state change so a crash before the resume is
  recovered) then `resume(id)` (reentrant). `resume`/keyed `execute` of a still-`WAITING` execution run
  **no attempt** (its `ExecutionResumed` would fold to `RUNNING` and recovery would treat a waiter as
  crashed) — `waitingHandle`. The stop rule extends to waiting (swallowed `ExecutionWaitingSignal` →
  still parked; a pending cancel wins, since `cancel()` only tripped the token while the attempt was
  registered). `replay` of a waiting execution ends cleanly at the wait. A **live** attempt that reaches a still-recorded
  wait (only possible after something folded it out of `WAITING`, e.g. `pause` then `resume`)
  **re-records** `SignalAwaited` so the fold returns to `WAITING` — otherwise it stays `RUNNING`, refused by
  `signal()` and re-resumed by every recovery. **Handles complete only after deregistration:**
  `scheduleAttempt` returns (and registers in `inFlight`) a stage chained *behind* the deregistering
  `whenComplete`, so no caller can observe a finished attempt still in flight — before this, an
  immediate `signal()` after `ExecutionWaitingSignal` could be refused, or its `resume()` attach to the
  dead attempt and schedule nothing. `liveAttempt(id)` (ignores done futures) is the second layer.
  Parking (waiting **or** paused) ends via `parkUnlessCancelled`, which checks the token
  and completes/deregisters **under the id lock** `cancel()` decides under — otherwise a `cancel()` between
  the check and the deregistration only trips the token and is lost. A **swallowed wait** re-raises at every later boundary (`requireAppendable`, after cancel/abandon), so
  nothing is recorded after `SignalAwaited` — a fallback's events there would make the signalled resume
  diverge. `ExecutionState.awaitingSignal()`
  is **derived from the timeline**, so the state records and snapshot format are unchanged.
  `Context.awaitSignal` is a default method (throws) so other `Context` implementations still compile.
  **`signal()` refuses before writing** everything a resume could never get past: wrong name, running,
  branch child (no task type), a payload the allowlist would refuse on read (round-trip `fromTree`
  first — `toTree` does not check it), and a payload not of the awaited type (`SignalAwaited` records
  `payloadType`; `ExecutionState.awaitingSignalType()`). A recorded signal is substituted into every
  resume, so a bad one is terminal on an append-only log. `signal(id, name, payload, opts)` for tasks
  that read vars.

- **Fenced runtime writes (Dist-1)** — `FencedWritesTest` (runtime: two runtimes over one log = two
  writers) + `FencedWritesAcceptanceTest` (the same over a real Gumbo log: storage refuses the zombie).
  Every runtime append goes through a `StreamCursor`: conditional when the log can fence, plain
  otherwise. **The expectation is one past what that writer *read*** (`StreamCursor.after(log, id,
  recorded)`), advanced only by its own appends — never the log's tip at write time, which would let a
  writer that read seq 10 fence on 11 and write on top of history it never saw. One cursor per attempt,
  shared by `runAttempt`'s lifecycle appends and its `ReplayingContext` (new 17-arg constructor; the old
  ones derive a cursor from `recorded`), and kept across retry iterations so a foreign write between
  retries fails the next `ExecutionResumed`. A `StaleWriterException` **fences the cursor out for good**
  (every later append refused without asking the log), and `runAttempt` exits on `cursor.fencedOut()`
  **first** — on both exits, ahead of the stop rule and the retry decision — appending nothing;
  `recordFailed`/`recordCancelled` write through the cursor so a zombie's "best-effort" record is
  refused too. One-shot appends fence on the fold just read under the id lock: create at 0, `pause`,
  `cancel`, `signal`; branch children get their own cursor from -1. Mutation-checked: with the cursor
  forced unfenced, the zombie tests fail.

- **Multi-writer log (Dist-0)** — `FoundationDBEventLogTest` (catalyst-gumbo) +
  `FoundationDBAcceptanceTest` (catalyst-api), both against a **real FoundationDB cluster**, run by
  CI's `fdb` job (copied from Gumbo 0.7.0's) and locally by `scripts/fdb-tests.sh` (Docker). They
  **skip** without a cluster and **fail** instead under `CATALYST_REQUIRE_FDB=true`, which CI sets —
  plus a step that fails if either class reports a skip. `GumboEventLog.foundationDB(clusterFile,
  root, blobs|codec)` pairs Gumbo's FDB storage adapter with its FDB sequencer over one `Database`
  the log owns and closes (Gumbo's service never opens or closes a sequencer, and neither component
  closes a `Database` it was handed). Every FDB type lives in package-private `FoundationDBLogs`:
  `fdb-java` is **optional** (it carries `libfdb_c`), so `GumboEventLog` must not reference it. A blob
  store is **required** — FDB refuses values over 100 KB, and it must be one every node can read.
  **Two logs on one root are the test fixture for two nodes**: a log opened once only sees its own
  writes, which hides per-process state. That fixture found two Catalyst-side bugs: `read()` used
  the tag's `readAll()`, which is **seqnum-ordered** — under two writers seqnum order ≠ version
  order (a writer claims its seqnum before its transaction, gets its version inside it), so the fold
  got events out of seq order; every stream read is now version-keyed (`readAfterVersion(-1)`,
  version-ordered per Gumbo 0.7.0), `wasOpened` included. And `latestSeq`'s per-process cache is
  bypassed when `multiWriter` (mutation-checked). Per-process state that stays: the `indexed` map
  (only ever true→removed or false, never wrong for a dual-tag decision) and the open-set checkpoint
  (any checkpoint is a valid fold prefix). **Fenced, not coordinated:** two runtimes on one root
  cannot corrupt a stream, but nothing stops both *starting* the same work — leases and the claim
  loop are Dist-3/4, as is `putKey` (a plain overwrite: two nodes taking one new key both create).

- **Distribution guard (Dist-2)** — `DistributionGuardTest` (runtime) + `DistributionGuardAcceptanceTest`:
  `Builder.distributed()` makes `build()` refuse a log that does not **report** both
  `supportsConditionalAppend()` and `supportsMultiWriter()`. The message gives both answers, and a log
  the builder opened is closed, so a file-backed log's directory lock is released. Refused before
  any log is opened: no explicit `nodeId(...)` and `shard(...)` (alternatives: own log vs shared log).
  Dist-3 added `supportsLeases()` to the required answers, and lifted Dist-2's `recoverAll` /
  `recoverOnStart` refusal now that the scan respects leases (below). Mutation-checked: dropping
  either capability check fails a test. `isDistributed()` reports the mode.

- **Leases (Dist-3)** — `LeaseTest` (runtime: runtimes sharing one `SharedLog` test helper are nodes;
  a dead node is a lease row written directly and lapsed with a `TestClock`) + `LeaseCodecTest`
  (core) + `GumboEventLogTest.leaseRowsAreChangedOnlyByCompareAndSet` +
  `FoundationDBEventLogTest.exactlyOneOfTwoNodesClaimsALease` + `FoundationDBAcceptanceTest`.
  - **No fencing token:** the lease decides *who should run*, and the append fence keeps streams
    correct. A takeover appends `ExecutionResumed` before any work, so a holder that paused past its
    lease is refused at its next append. `Lease.encode()` is canonical, because CAS compares bytes.
  - **`LeaseKeeper`:**
    - `acquire` is reentrant per process: it returns whether this call claimed. A lease held by this
      node's *own id* counts as ours (restart under the same id).
    - `renewAll` runs on a daemon heartbeat thread. A failed CAS removes the lease and runs the
      attempt's `onLost`.
    - `release` is a CAS from the row this process last wrote.
    - Renewal and release synchronize per lease, so a release can't lose to a renewal it raced.
  - **Runtime integration:**
    - `scheduleAttempt` claims (except `REPLAY_TERMINAL`) before registering anything, and releases
      in the deregistering `whenComplete`, *before* `visible` completes. **Claim, then decide:** a
      `RESUME` re-folds after the claim, because the caller's fold predates it. If another node
      finished the execution or parked it waiting and released in between, the attempt becomes a
      terminal replay or a waiting handle, never `ExecutionResumed` after the end
      (`LeaseTest.an…DuringTheClaimIsNotResumed`, via `SharedLog.beforeReadLease`).
    - `underLease` covers the writes outside an attempt: create, signal, pause and cancel when not in
      flight here. It releases only if no attempt took ownership.
    - `RunningAttempt.leaseLost` is read by the token as an abandon, so the existing abandon path
      (append nothing) handles a lost lease. `abandon()` completes with `LeaseLostException` instead
      of the shutdown signal.
    - `recoverAll` makes each whole decision **under the lease** (`underLease` around fold → decide
      → resume), so another node pausing or parking an execution and releasing just before the
      claim can't get it resumed: recovery never undoes a deliberate `PAUSED`/`WAITING`. A
      `LeaseHeldException` maps to `leftAlone` RUNNING.
    - `close()` releases the leftovers after the drain.
  - **Mutation-checked:** removing the claim, the release, the lost callback, recovery's skip,
    close's release or the liveness check each fails a `LeaseTest` case.

- **Claim loop + cross-node control (Dist-4)** — `CrossNodeTest` (runtime) + `LeaseKeeperTest` +
  claimKey/lease-codec cases in Gumbo/core/FDB tests + **`Demo <root> distributed-a|distributed-b`**
  (two JVMs on a real FDB root, gated in CI's `fdb` job; `scripts/fdb-tests.sh --demo` locally).
  - **Claim loop:** polls `scan()`, which is `recoverAll`'s body, every `claimInterval` (it can't
    subscribe: Gumbo push subscriptions are per process). It folds without the lease and claims only
    crash-interrupted executions, so parked ones cost no lease writes. It re-decides under the lease.
  - **Cancel on the lease row:** `Lease.cancelRequest` uses format `lease1c`, with the reason in
    base64.
    - `LeaseKeeper.requestCancel` adds it by CAS.
    - `renewAll` absorbs "my row + a request" and keeps the lease. The request counts as honoured
      (`cancelFired`) **only if a live attempt took it**. `onCancel` is a predicate that returns false
      once the attempt's future is done. Registering it fires any request already absorbed. A request
      absorbed during a pause, before registration, or after the attempt parked therefore stays for
      the next holder rather than being deleted by the release.
    - `acquire` carries a request over on takeover. `scheduleAttempt` honours a carried request
      before running, and `scan`'s `honourOrphanedCancel` honours one on a lapsed parked execution.
    - `release` keeps an unhonoured request on a lapsed row unless the execution is terminal.
  - **Keys:** `EventLog.claimKey` (CAS; the default is lookup+put). Keyed `execute` decides under the
    lease, and an empty stream behind a claimed key is created by whoever holds it.
  - **Test timeouts:** `@Timeout(30)` on the lease and cross-node test classes. A lease bug *hangs*
    (a parked attempt nobody stops), and an unbounded `result()` turned one mutant into a stuck build.
  - **Mutation-checked:** removing the loop, forwarding, absorbing, carrying, the key CAS, the pre-check,
    orphan handling, release keeping or dropping the request, or the create-for-claimed-key each fails a
    test.

- **In-doubt model completions** — `ReplayingContextInDoubtCompletionTest`: a `CompletionRequested`
  with no `CompletionReceived` is a provider call that may have been accepted and *billed*, so it
  routes through `InDoubtPolicy` exactly as a dangling `ToolRequested` does (it previously just
  re-issued the call). Keyed on the recorded `requestHash` — a different request at that boundary is
  a divergence, not a recovery. `RETRY` completes the dangling request instead of opening a second
  one, so the recovered log stays one request / one result and replays normally. **The trap:** a
  model failure also leaves an unmatched `CompletionRequested` (model failures record nothing), so
  `seed()` clears the pending request on `RetryRequested` — a verdict, not a doubt. Without that,
  every retried model failure becomes an `InDoubtException` under the default `FAIL`.

CI (`.github/workflows/ci.yml`) runs all exit demos as gates — it is the source of truth per phase.

**Docs for developers:** `README.md` is the getting-started page (quickstart, concepts, recipe table);
`docs/guide.md` holds the depth; `docs/deployment.md` the ops. **`QuickstartTest` (catalyst-api) runs
the README's Quickstart code verbatim** — change one, change the other. `scripts/demo.sh <name>` runs
any exit demo with the same classpath CI builds. Keep both docs to what the code does: check
signatures (e.g. the payload allowlist refuses plain classes) before documenting them.
