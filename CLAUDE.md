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

- `catalyst-events` — sealed `CatalystEvent` hierarchy + `EventCodec` (Jackson). Schema-stable; keep
  changes additive.
- `catalyst-core` — SPIs (`Task`, `Context`, `Model`, `Tool`, `Memory`, `EventLog`), the pure
  `Reducer` fold, and `ReplayingContext` — the record/substitute engine that makes resume/replay
  work. This is the heart of the system.
- `catalyst-runtime` — `CatalystRuntime` (virtual-thread scheduler, lifecycle, idempotency) +
  `InMemoryEventLog`.
- `catalyst-gumbo` — `GumboEventLog`: one Gumbo `LogTag` per execution; Gumbo `localId` == Catalyst
  `seq`; durable KV for the idempotency index.
- `catalyst-tools`, `catalyst-api` — built-in tools and the `Catalyst` facade.
- `catalyst-langchain4j` — `LangChain4jModel`: wraps any LangChain4j `ChatModel` (real providers).
  Depends only on `langchain4j-core`; the app supplies the provider. Tested offline with a fake
  `ChatModel` (override `doChat`).

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
- **v0.2 Cancellation** — `Demo cancel`: a running task is cancelled cooperatively and folds to
  `CANCELLED` (not `FAILED`). `cancel(id)` records `ExecutionCancelled`; while the execution is in
  flight it trips a `CancellationToken` and interrupts the worker, which unwinds at its next live
  boundary (checked in `ReplayingContext.requireAppendable`) and records the event itself — so no other
  thread ever appends to a running execution's stream. Attaching to a cancelled execution surfaces a
  `CancellationException`.

CI (`.github/workflows/ci.yml`) runs all five exit demos as gates — it is the source of truth per phase.
