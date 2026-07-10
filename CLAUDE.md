# Catalyst — notes for Claude

Catalyst is a **durable AI execution runtime for the JVM** (Java 21, Maven multi-module). This repo
currently implements the **M0** milestone: *execute + record + resume*.

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

## Key invariants

- Every side-effecting boundary goes through `Context` and is recorded as an event. On replay/resume
  it is **substituted** from the log (no re-execution) until the task runs past the log tail.
- `seq` is dense and per-execution; it lives on `SequencedEvent`, not on the event itself.
- Determinism contract: task code between boundaries must be deterministic (Temporal-style).

## The M0 acceptance test

`catalyst-api` → `M0ResumeAcceptanceTest` and the runnable `Demo` prove: crash after step 1, resume,
finish with zero duplicate model calls. If you touch the engine, keep these green.
