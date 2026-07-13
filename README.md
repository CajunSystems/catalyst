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

> **Status: v0.1 feature-complete (M0 + M1 + M2).** Durable execute/record/resume; strict,
> self-verifying replay with canonical hashing and a typed token/cost timeline; and branch + diff
> (fork a recorded run, swap the model or a tool result, and diff the trajectories).

---

## What works today (M0 + M1 + M2)

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
- **Strict, self-verifying replay (M1)** — `runtime.replay(id, task)` re-runs a recorded execution
  with every boundary substituted and **zero external calls**. Each boundary is checked against the
  log's canonical request hash / tool-input hash / effect label / memory key; a mismatch under
  `ReplayMode.STRICT` throws `NonDeterministicReplayException(seq, expected, actual)`, catching
  nondeterministic task code.
- **Typed timeline + token/cost accounting (M1)** — `runtime.inspect(id).timelineView()` folds the
  log into model/tool call counts, token usage, latency, and cost. Cost is priced by a pluggable
  `CostModel` (e.g. `CostModel.perMillionTokens(in, out)`).
- **Real providers via LangChain4j (M1)** — `LangChain4jModel.of(chatModel)` wraps any LangChain4j
  `ChatModel`, which buys OpenAI, Anthropic, Gemini, Ollama and local models. Catalyst keeps no
  provider HTTP client of its own; the completion flows through `ctx.model()` and is recorded and
  replayed like any other.
- **Branch + diff (M2)** — `runtime.branch(id, atSeq).withModel(other).withRecordedToolResult(name,
  value).run(task)` forks a recorded execution: it substitutes the prefix up to `atSeq` (swapping in
  counterfactual tool results), then runs forward with the new model. Under `ReplayMode.BRANCH` a
  divergence forks (appends `ExecutionBranched`, continues live) instead of throwing.
  `Trajectory.diff(base, fork)` gives the step-by-step difference.

Deferred to later milestones (schema slots already reserved so no breaking change is needed):
`WAITING`/signal APIs and human-in-the-loop, snapshots and blob store, distributed execution (v0.2+/v1).

## Module layout

| Module | Responsibility |
|---|---|
| `catalyst-events` | Sealed `CatalystEvent` hierarchy + Jackson serialization (isolated for schema stability) |
| `catalyst-core` | `Task`, `Context`, `Model`/`Tool`/`Memory`/`EventLog` SPIs, the reducer fold, and `ReplayingContext` (record/substitute engine) |
| `catalyst-runtime` | `CatalystRuntime`, virtual-thread scheduler, lifecycle, idempotency, in-memory `EventLog` |
| `catalyst-gumbo` | `GumboEventLog`: durable `EventLog` over the Gumbo shared log |
| `catalyst-tools` | `ClockTool`, `CalculatorTool` |
| `catalyst-langchain4j` | `LangChain4jModel`: adapts any LangChain4j `ChatModel` to Catalyst's `Model` |
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

## The M1 exit demo (strict replay)

`Demo replay` records a complete execution, then replays it with substitution and shows divergence
detection:

```bash
java -cp "$CP" com.cajunsystems.catalyst.api.Demo replay
```

```
[replay] timeline: 2 model call(s), 15 tokens, $0.000069
[replay] replayed -> COMPLETED; external calls during replay: 0
[replay] divergence correctly detected at seq 4
```

Asserted automatically in `M1ReplayAcceptanceTest`.

## The M2 exit demo (branch + diff)

`Demo branch` records a run, forks it at step 1 with a different model, and prints the diff:

```bash
java -cp "$CP" com.cajunsystems.catalyst.api.Demo branch
```

```
[branch] parent ... -> SUMMARY|FINAL
[branch] fork   ... (model swapped from step at seq 4)
[branch] diff (1 step(s) changed):
  [0] PROMPT  SAME
  [1] MODEL  SAME
  [2] PROMPT  SAME
~ [3] MODEL  CHANGED  base={"message":"FINAL",...}  fork={"message":"FINAL-B",...}
```

Asserted automatically in `M2BranchAcceptanceTest`.

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

Plug in a real provider by wrapping any LangChain4j `ChatModel` (add `catalyst-langchain4j` and a
LangChain4j provider such as `langchain4j-open-ai`):

```java
ChatModel chat = OpenAiChatModel.builder().apiKey(key).modelName("gpt-4o").build();
CatalystRuntime runtime = Catalyst.builder()
        .log(GumboEventLog.at(Path.of("./catalyst-log")))
        .model(LangChain4jModel.of(chat))
        .costModel(CostModel.perMillionTokens(2.50, 10.00))
        .build();
```

## Specification

The full v0.1 design lives in the project spec (Catalyst v0.1 Specification). This repository
implements all three v0.1 milestones — **M0** (execute + record + resume), **M1** (replay + inspect),
and **M2** (branch + diff).

## License

MIT — see [LICENSE](LICENSE).
