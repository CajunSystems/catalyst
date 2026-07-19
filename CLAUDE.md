# Catalyst — notes for Claude

Catalyst is a **durable AI execution runtime for the JVM** (Java 21, Maven multi-module). This repo
implements all three v0.1 milestones: **M0** (*execute + record + resume*), **M1** (*replay +
inspect* — strict canonical-hash replay with `NonDeterministicReplayException`, `replay(id, task)`,
typed token/cost timeline via `ExecutionState.timelineView()` + pluggable `CostModel`), and **M2**
(*branch + diff* — `ReplayMode.BRANCH` forks on divergence, `runtime.branch(id, atSeq)` with model /
counterfactual-tool swaps, `Trajectory.diff`).

## Build & test

```bash
mvn install            # full reactor + tests
mvn -pl catalyst-core -am install   # a single module and its deps
```

**Gumbo dependency:** `catalyst-gumbo` depends on `com.cajunsystems:gumbo:0.2.0`. JitPack is the
intended source, but if `jitpack.io` is blocked, install Gumbo locally first:
`mvn -f /path/to/gumbo/pom.xml install -DskipTests`.

## Architecture (where things live)

- `catalyst-events` — sealed `CatalystEvent` hierarchy + `EventCodec` (Jackson) + `BlobStore`
  (content-addressed offload of large payload fields, in-memory + `FileBlobStore`). Schema-stable; keep
  changes additive.
- `catalyst-core` — SPIs (`Task`, `Context`, `Model`, `Tool`, `Memory`, `EventLog`), the pure
  `Reducer` fold, and `ReplayingContext` — the record/substitute engine that makes resume/replay
  work. This is the heart of the system.
- `catalyst-runtime` — `CatalystRuntime` (virtual-thread scheduler, lifecycle, idempotency) +
  `InMemoryEventLog`.
- `catalyst-gumbo` — `GumboEventLog`: one Gumbo `LogTag` per execution; Gumbo `localId` == Catalyst
  `seq`; durable KV for the idempotency index.
- `catalyst-tools`, `catalyst-api` — built-in tools (`ClockTool`, `CalculatorTool`, `HttpTool`,
  `FilesystemTool`) and the `Catalyst` facade.
- `catalyst-langchain4j` — `LangChain4jModel`: wraps any LangChain4j `ChatModel` (real providers).
  Depends only on `langchain4j-core`; the app supplies the provider. Tested offline with a fake
  `ChatModel` (override `doChat`).
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
- **v0.2 Observability / OTel exporter** — `OtelAcceptanceTest` + `Demo otel`: an execution's event log
  folds into one OpenTelemetry trace via `catalyst-otel`'s `CatalystTracer.export(id, events)` — a root
  span for the run, a child span per boundary (model/tool/effect/memory; model/tool carry real latency,
  model spans carry that call's tokens/cost/finish reason), and lifecycle moments (retry, pause, etc.)
  as span events on the root. Root status is OK/ERROR from the folded terminal state. Read-only and
  post-hoc (consumes `runtime.log().read(id)`), so the log *is* the trace. The module depends on the
  OTel **API** only; tests drive a real SDK into an in-memory exporter offline.

CI (`.github/workflows/ci.yml`) runs all exit demos as gates — it is the source of truth per phase.
