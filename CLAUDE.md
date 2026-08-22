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

**Gumbo dependency:** `catalyst-gumbo` depends on **`com.github.CajunSystems:gumbo:0.3.0`**,
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
  it is matched on the whole cause chain, not the top type. Not yet used by the runtime — the
  in-process invariant is still `KeyedLock` + `inFlight`; this is the seam a claim loop writes to.

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
