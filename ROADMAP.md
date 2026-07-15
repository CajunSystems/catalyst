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

## v0.2 — Harden, scale, observe 🔜

Production-readiness for the single-node runtime. No new top-level concepts; deepen what exists.

**Delivery: small, independently-shippable increments.** Each item below lands as its own PR with its
own tests (and an exit demo where it touches the engine), keeping the diff reviewable and CI green at
every step. Snapshots was the first. Planned order of the remaining increments — most self-contained
first: **① cancellation event → ② task registry / `resume(id)` → ③ built-in HTTP + Filesystem tools →
④ generic-collection payloads → ⑤ blob store**, with schema evolution, retry semantics, the
auto-capture agent, per-execution locking, streaming, and observability sequenced after. Order is a
guide, not a contract — it flexes as we learn. Snapshots, the cancellation event (①), the task
registry / standalone `resume(id)` (②), the built-in HTTP + Filesystem tools (③), generic-collection
payloads (④), and the blob store (⑤) have shipped; the remaining v0.2 work (schema evolution, retry
semantics, the auto-capture agent, per-execution locking, streaming, observability) is unsequenced.

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
  Blob-store exit demo in CI.
- **Schema evolution** (open question §13.4) — pick a strategy before a public release: tolerant
  reader vs. upcasters. Events live forever; this must be decided while the schema is still small.

### Determinism & correctness
- **Auto-capture agent** (spec §6, v0.2 stretch) — a ByteBuddy agent that records `Instant.now()`,
  `Random`, and `UUID.randomUUID()` inside task code automatically, so users don't have to route
  every nondeterministic call through `ctx.effect(...)`.
- ✅ **Generic-collection payloads** (④) — `PayloadCodec` now encodes `List`/`Set`/`Map`/arrays
  structurally, carrying each element in its own typed envelope (recursively), so element types survive
  the round-trip (`List<Point>` comes back as records, not maps; `Map` keys may be non-`String`). Leaf
  encoding is byte-identical to before, so existing logs interoperate, and the class allowlist is
  enforced at every nested leaf and array component (gadget-safe). Collections rebuild as
  `ArrayList`/`LinkedHashSet`/`LinkedHashMap` (equal by content). Gated by the v0.2 Collection-payloads
  exit demo in CI.
- **Streaming completions** (open question §13.1) — decide whether the `Model` SPI needs a streaming
  variant now; record the assembled completion, add token-level replay later.
- **Retry semantics** (open question §13.3) — finalize retry-as-attempt (`RetryRequested` + attempt
  counter, already in the schema) vs. child execution, and wire a retry policy.

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
- **Per-execution locking** — replace the single coarse `synchronized execute` lock with per-id
  coordination to lift the throughput ceiling under concurrent load.
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
- **OTel exporter** — one span per event, execution = trace; the log already *is* the trace, so this
  is a fold into OpenTelemetry.
- **Timeline UI** — a read-only view over `inspect(id).timelineView()` / `Trajectory`.

---

## v1 — Agents, signals, distribution 🔭

The substrate becomes a platform. This is where the other CajunSystems components integrate.

### Agent abstraction (spec §12)
- An **`Agent`** built *on* `Task` (reasoning loops, tool selection) — a consumer of the runtime, not
  part of its core. Possibly delegate planning to Embabel rather than competing.
- **MCP** integration and richer tool ecosystems.

### Signals & human-in-the-loop — Boudin integration (spec §5 lifecycle, §12)
- Activate the reserved **`WAITING`** state: a task pauses, awaits an external signal, schedules
  future work, or invokes a long-running Boudin workflow. Human-in-the-loop lands here.
- `await`/signal APIs on `Context` — no schema change needed (the slot is reserved).

### Distributed execution (spec §12)
- Execution across **Cajun** actor nodes over a shared **Gumbo cluster** — same `EventLog` SPI, a
  one-line bootstrap change from single-node. History and replay preserved across nodes.

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
