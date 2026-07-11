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

CI (`.github/workflows/ci.yml`) runs all three exit demos as gates — it is the source of truth per phase.
