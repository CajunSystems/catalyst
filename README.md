# Catalyst

[![CI](https://github.com/CajunSystems/catalyst/actions/workflows/ci.yml/badge.svg)](https://github.com/CajunSystems/catalyst/actions/workflows/ci.yml)

**The Durable AI Execution Runtime for the JVM.**

Catalyst lets you build AI-powered systems that are reliable, resumable, replayable, and
observable. It focuses on **execution semantics**, not prompt engineering — *Temporal meets Git, for
AI execution*, embedded in your JVM process.

Applications don't call models; they **execute AI tasks**. Every important state transition is an
event appended to a durable log, so an execution can crash and resume at the exact step it left off,
be replayed deterministically, and (later) be branched. The log *is* the trace: timelines, token
usage, and cost are folds over events, not a separate instrumentation layer.

Built on the [CajunSystems](https://github.com/CajunSystems) stack: **Gumbo** (a shared, append-only
log) provides durability.

> **Status: M0 — Execute + record + resume.** This is the first milestone of the
> [v0.1 spec](#specification). It delivers the durable foundation; M1 (replay + inspect) and
> M2 (branch + diff) build on it. See [Milestones](#milestones).

---

## What works today (M0)

- **`Task` / `Context` API** — a task is a unit of AI work; everything it needs (model, tools,
  memory, effects) comes from the `Context` passed to `execute`.
- **Durable, event-sourced execution** — every boundary (model completion, tool call, effect,
  memory read/write) becomes an event in an append-only log.
- **Crash recovery** — `kill -9` mid-task, re-submit with the same idempotency key, and the
  execution resumes at the exact event and completes with **zero duplicate model calls**.
- **Two storage backends behind one SPI** — an in-memory log for tests, and a durable
  **Gumbo**-backed file log (the flagship embedded mode).
- **`MockModel`** (deterministic, for tests/demos) and built-in **`ClockTool`** / **`CalculatorTool`**.
- **In-doubt tool policy** — a tool call caught mid-flight by a crash is surfaced via a pluggable
  `InDoubtPolicy` (`RETRY` / `FAIL` / `ASK`) instead of being silently re-executed.

Deferred to later milestones (schema slots already reserved so no breaking change is needed):
strict replay with canonical-hash mismatch detection (M1), the LangChain4j model adapter and real
providers (M1), branch/diff (M2), `WAITING`/signal APIs, snapshots and blob store (v0.2).

## Module layout

| Module | Responsibility |
|---|---|
| `catalyst-events` | Sealed `CatalystEvent` hierarchy + Jackson serialization (isolated for schema stability) |
| `catalyst-core` | `Task`, `Context`, `Model`/`Tool`/`Memory`/`EventLog` SPIs, the reducer fold, and `ReplayingContext` (record/substitute engine) |
| `catalyst-runtime` | `CatalystRuntime`, virtual-thread scheduler, lifecycle, idempotency, in-memory `EventLog` |
| `catalyst-gumbo` | `GumboEventLog`: durable `EventLog` over the Gumbo shared log |
| `catalyst-tools` | `ClockTool`, `CalculatorTool` |
| `catalyst-api` | Thin facade: `Catalyst.embedded(path)`, builders, `Serializers` |

Coordinates: `com.cajunsystems:catalyst-*`. Root package `com.cajunsystems.catalyst`. Java 21.

## How Catalyst maps onto Gumbo

Each execution is one Gumbo `LogTag` (`catalyst-exec/<id>`). Gumbo's per-tag `localId` is Catalyst's
dense per-execution `seq`; a `TypedLogView<CatalystEvent>` handles serialization; and the
idempotency-key → execution index is a durable, tag-scoped key-value store. Swapping the Gumbo
persistence adapter switches between file-backed durability and in-memory — the same SPI a future
Gumbo *cluster* backend will use.

## Building

Requires **JDK 21** and Maven.

Catalyst depends on Gumbo (`com.cajunsystems:gumbo:0.2.0`). The intended delivery is JitPack, but if
`jitpack.io` is blocked (e.g. by an egress policy) build Gumbo into your local Maven repo first:

```bash
git clone https://github.com/CajunSystems/gumbo
mvn -f gumbo/pom.xml install -DskipTests     # installs com.cajunsystems:gumbo:0.2.0 into ~/.m2

# then, in this repo:
mvn install
```

## The M0 exit demo (crash & resume)

`Demo` tells the `kill -9` story across two real processes:

```bash
CP=$(find . -name 'classes' -type d | tr '\n' ':')$(find ~/.m2 -name 'gumbo-0.2.0.jar' -o -name 'jackson-*-2.17.2.jar' -o -name 'slf4j-*-2.0.12.jar' -o -name 'kryo-*.jar' | tr '\n' ':')

# 1) record the first step, then die abruptly (exit 137 = kill -9), leaving a durable partial log
java -cp "$CP" com.cajunsystems.catalyst.api.Demo /tmp/catalyst-demo record

# 2) resume from the durable log — step 1 is substituted, only step 2 runs live
java -cp "$CP" com.cajunsystems.catalyst.api.Demo /tmp/catalyst-demo resume
```

```
[record] step 1 result: SUMMARY (model calls this run: 1)
[record] simulating kill -9 (exit 137)...
[resume] execution ... -> COMPLETED
[resume] result: SUMMARY|FINAL
[resume] model calls THIS run: 1 (step 1 was substituted from the log; only step 2 ran live)
```

The same scenario is asserted automatically in `M0ResumeAcceptanceTest`.

## A taste of the API

```java
CatalystRuntime runtime = Catalyst.builder()
        .log(GumboEventLog.at(Path.of("./catalyst-log")))   // durable, embedded
        .model(myModel)
        .build();

Task<String> task = ctx -> {
    String summary = ctx.model().complete(
            CompletionRequest.of(Prompt.builder().user("summarize the document").build())).message();
    return summary.toUpperCase();
};

String result = runtime.execute(task, ExecutionOptions.withKey("doc:42")).result();
```

Re-submitting the same key resumes/attaches to the existing execution instead of starting a new one.

## Specification

The full v0.1 design lives in the project spec (Catalyst v0.1 Specification). This repository
currently implements the **M0** milestone of that spec.

## License

MIT — see [LICENSE](LICENSE).
