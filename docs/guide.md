# Catalyst developer guide

The [README](../README.md) gets a first task running. This guide covers what you need after that:
writing tasks that resume and replay correctly, models and tools, failures, controlling and
inspecting executions, and how the log underneath works. For running Catalyst as a service, see
[`deployment.md`](deployment.md).

- [Writing tasks](#writing-tasks)
- [Models](#models)
- [Tools](#tools)
- [Effects, memory and vars](#effects-memory-and-vars)
- [Idempotency keys and resuming](#idempotency-keys-and-resuming)
- [Failures: retries and in-doubt calls](#failures-retries-and-in-doubt-calls)
- [Signals: waiting for a human](#signals-waiting-for-a-human)
- [Cancel, pause, resume](#cancel-pause-resume)
- [Inspect, replay, branch](#inspect-replay-branch)
- [Streaming completions](#streaming-completions)
- [Timeline reports and OpenTelemetry](#timeline-reports-and-opentelemetry)
- [Auto-capture: nondeterminism without the ceremony](#auto-capture-nondeterminism-without-the-ceremony)
- [How the log works](#how-the-log-works)

---

## Writing tasks

A task is a unit of AI work: a `Task<R>` whose `execute(Context ctx)` returns a result.

```java
public final class TriageTask implements Task<String> {
    @Override
    public String execute(Context ctx) {
        String summary = ctx.model().complete(CompletionRequest.of(Prompt.builder()
                .system("You triage support tickets.")
                .user("Summarize: the printer on floor 3 is offline")
                .build())).message();
        String receivedAt = ctx.effect("received-at", () -> Instant.now().toString());
        return summary + " (received " + receivedAt + ")";
    }
}
```

Each call through `ctx` is a **boundary**: Catalyst records its result in the log before handing it to
your code. When the execution is resumed after a crash, replayed or branched, the task runs again
**from the top**, and every boundary already in the log is **substituted** with its recorded result
instead of being executed again. That is how a resumed execution makes zero duplicate model calls.

It works only if your task obeys **the one rule:**

> Between boundaries, task code must be deterministic. Given the same boundary results, it must make
> the same boundary calls in the same order.

In practice:

- **Go through `ctx` for anything that talks to the outside world** or isn't repeatable: models
  (`ctx.model()`), tools (`ctx.call(...)`), clock, random and UUID values, and reads of external state
  (`ctx.effect(...)`). Calling an HTTP client or `Instant.now()` directly gives a different answer on
  replay. [Auto-capture](#auto-capture-nondeterminism-without-the-ceremony) can handle the clock,
  random and UUID cases for you.
- **Don't branch on things that change between runs** (wall-clock time, environment, mutable statics)
  unless you read them through `ctx.effect`.
- **Use named classes, not lambdas,** for any task that may be resumed by id in another process
  (`resume(id)`, `recoverOnStart`). Catalyst records the task's class name, and a lambda's synthetic
  name is not stable across builds. Lambdas are fine for tests and one-off executions.

A strict replay (`runtime.replay(id, task)`) checks the rule: if the task's calls diverge from the
recording, it throws `NonDeterministicReplayException` and names the boundary.

## Models

Every model call goes through `ctx.model()`, and the runtime supplies the model:

```java
Catalyst.builder(dir).model(model)
```

- **`MockModel`**: deterministic, for tests and demos. `MockModel.alwaysReturn("text")`,
  `MockModel.scripted("first", "second")`, or `MockModel.builder().respond(request -> completion)`.
  `callCount()` tells you how many calls actually reached it, which is how the tests prove a resume
  made no duplicate calls.
- **Real providers via LangChain4j:** add `catalyst-langchain4j` plus a LangChain4j provider module
  (for example `langchain4j-open-ai`), then wrap its `ChatModel`:

  ```java
  ChatModel chat = OpenAiChatModel.builder().apiKey(key).modelName("gpt-4o").build();
  CatalystRuntime runtime = Catalyst.builder(dir)
          .model(LangChain4jModel.of(chat))
          .costModel(CostModel.perMillionTokens(2.50, 10.00))   // USD per million input/output tokens
          .build();
  ```

  `LangChain4jModel.streaming(streamingChatModel)` adapts a streaming provider (see
  [Streaming](#streaming-completions)). Catalyst has no provider HTTP client of its own; whatever
  LangChain4j supports works.
- **Your own:** implement `Model.complete(CompletionRequest)`.

`CostModel` prices recorded token usage, so cost shows up in `inspect(id).timelineView()`, the timeline
report and OTel spans. The default is `CostModel.free()`.

## Tools

A tool is a named, typed side effect:

```java
public final class CreateTicket implements Tool<CreateTicket.Input, String> {
    public record Input(String title, String body) {}

    @Override public String name() { return "create-ticket"; }
    @Override public Class<Input> inputType() { return Input.class; }
    @Override public String apply(Input in) throws Exception {
        return ticketing.create(in.title(), in.body());   // the real side effect
    }
}

// in a task:
String ticketId = ctx.call(new CreateTicket(), new CreateTicket.Input("Printer down", summary));
```

The request (`ToolRequested`) is recorded before the tool runs and the result (`ToolCompleted`) after,
so on resume a completed call is substituted, not re-run. A call that was in flight when the process
died is in doubt; see [Failures](#failures-retries-and-in-doubt-calls).

Built-in tools are in `catalyst-tools`:

| Tool | Input → output |
|---|---|
| `ClockTool` | `ClockTool.Query` → `ClockTool.Now` |
| `CalculatorTool` | `CalculatorTool.Expr(left, op, right)` → `CalculatorTool.Value` |
| `HttpTool` | `HttpTool.Request.get(url)` / `.post(url, body, contentType)` → `HttpTool.Response` |
| `FilesystemTool(root)` | `FilesystemTool.Command.read/write/list/exists/delete(path…)` → `FilesystemTool.Result` |

`FilesystemTool` is sandboxed to its root and rejects `..`, absolute paths and symlink escapes. On
macOS it refuses to run by default, because the JDK lacks `SecureDirectoryStream` there and the
fallback cannot guarantee containment. `HttpTool` takes a `Sender` for tests that must stay offline.

## Effects, memory and vars

- **`ctx.effect(label, supplier)`** records the value `supplier` returns. Use it for anything
  nondeterministic or external that isn't a tool: timestamps, random ids, a config lookup.

  ```java
  String requestId = ctx.effect("request-id", () -> UUID.randomUUID().toString());
  ```

  Recorded values (effect results, tool outputs, memory values, task results) must be a
  **record**, an enum, a string, number or boolean, a `java.time` value, a `UUID`, or lists, maps and
  arrays of those. A plain class is refused when the log is read back. The log is treated as
  untrusted input, so only these types are ever instantiated from it. Model your payloads as records.
- **`ctx.memory()`** is per-execution working memory (`put`, `get(key, type)`, `contains`). Reads and
  writes are recorded, so a resumed task sees the same values.
- **`ctx.var(name)`** reads an input passed at submit time:

  ```java
  runtime.execute(task, ExecutionOptions.withKey("doc-42").var("documentId", "42"));
  ```

  Vars are **not persisted**, only their hash. A task that reads vars must be resumed or replayed with
  the same `ExecutionOptions`, and a task cannot read its own idempotency key. So `recoverOnStart`,
  which resumes without vars, cannot correctly resume a task that depends on them. For such tasks,
  keep each execution's vars alongside its id and resume it yourself with `resume(id, opts)`. Tasks
  that don't read vars recover automatically.

## Idempotency keys and resuming

`ExecutionOptions.withKey(key)` names an execution. Submitting the same key again never starts a
second one:

- **still running in this process:** you get a handle to the running attempt
- **unfinished in the log (a crash):** it resumes, with recorded boundaries substituted
- **finished:** you get the recorded outcome; the task is not run

That makes retrying a submission safe: from a client, a message queue or a cron job. Without a key,
every `execute` starts a new execution.

After a crash you can also resume **by id**, with no key and no task instance, if the task type is
registered:

```java
CatalystRuntime runtime = Catalyst.builder(dir)
        .model(model)
        .task(new TriageTask())          // register every task type you may need to resume
        .recoverOnStart(true)            // resume everything a crash left unfinished, in build()
        .build();

runtime.resume(executionId);              // or resume one execution explicitly
```

`recoverOnStart` resumes executions left `STARTING`/`RUNNING`/`RESUMING` and leaves `PAUSED` ones
alone. `runtime.startupRecovery()` reports what it did, including any it couldn't recover (usually an
unregistered task type).

## Failures: retries and in-doubt calls

Two different things can go wrong, and two policies govern them.

**A call fails:** the provider returns a 503, a tool throws. `RetryPolicy` decides whether to try
again. A retry re-enters the task as a resume, so everything that succeeded is substituted and only the
failed boundary runs again.

```java
Catalyst.builder(dir).retryPolicy(RetryPolicy.exponential(5, Duration.ofSeconds(1), 2.0, Duration.ofMinutes(1)))
// or per execution:
ExecutionOptions.withKey(key).retryPolicy(RetryPolicy.maxRetries(3, Duration.ofSeconds(2)))
```

The default is `RetryPolicy.none()`. The retry count is recorded, so the budget survives restarts.
Some failures are never retried, whatever the policy: a nondeterministic replay, an in-doubt call, an
interrupt, a JVM `Error`.

**A call is in doubt:** the process died after recording a request but before recording its result.
Did the model call get billed? Did the ticket get created? `InDoubtPolicy` decides, on resume:

| Policy | What happens to the in-flight call |
|---|---|
| `FAIL` (default) | the execution fails with `InDoubtException`, for you to investigate |
| `RETRY` | the call is made again, completing the original request; right for idempotent calls |
| `ASK` | the execution is paused, for an operator to decide |

Set it with `Catalyst.builder(dir).inDoubtPolicy(InDoubtPolicy.RETRY)`. A graceful shutdown leaves
nothing in doubt as long as every in-flight call finishes within `drainTimeout`. A call still running
when the drain times out is left exactly as a crash would leave it, and this policy decides what
happens to it. Size `drainTimeout` above your slowest call (see [deployment.md](deployment.md#shutdown-drain-dont-kill)).

## Signals: waiting for a human

A task can stop and wait for input from outside, such as a reviewer's approval, a user's choice or a
webhook, without holding a thread while it waits:

```java
record Approval(String reviewer, boolean approved) {}

public final class ReviewedReport implements Task<String> {
    @Override public String execute(Context ctx) {
        String draft = ctx.model().complete(draftRequest).message();
        Approval approval = ctx.awaitSignal("approval", Approval.class);   // parks here
        if (!approval.approved()) return "rejected by " + approval.reviewer();
        return ctx.model().complete(finalRequest(draft)).message();
    }
}
```

The first time the task reaches `awaitSignal`, Catalyst records the wait and the execution parks in
`WAITING`. The attempt ends and no thread is held, and the handle's `result()` throws
`ExecutionWaitingSignal`. It can wait for minutes or months, across restarts. Startup recovery leaves
it alone.

Deliver the signal, from a request handler, a UI, anywhere with the runtime:

```java
runtime.inspect(id).awaitingSignal();                                // Optional[approval]
runtime.signal(id, "approval", new Approval("dana", true)).result(); // resumes; returns the result
```

`signal` records the payload as the result of the wait and resumes the task. Like any resume, the task
re-runs from the top: the draft is substituted from the log, `awaitSignal` returns the recorded
payload, and only the work after it runs live. Replays substitute the payload too.

Worth knowing:

- **Only a waiting execution accepts its signal.** `signal` refuses an execution that is running,
  waiting for a different signal, or finished. Signals sent before the task reaches its wait are not
  buffered; check `inspect(id).awaitingSignal()` and retry.
- **A crash right after `signal` is safe.** Recording the signal moves the execution out of `WAITING`
  (to `RESUMING`), so if the process dies before it resumes, startup recovery finishes it.
- **The task type must be registered** (`Catalyst.builder().task(...)`) for `signal` to resume it. If
  it isn't, the signal is still recorded and you resume by re-submitting the task with its key.
- **A task that reads `ctx.var(...)`** (or ran with its own retry policy) must be signalled with the
  options it was submitted with: `runtime.signal(id, name, payload, opts)`. Vars aren't persisted, and
  the resume re-enters the task from the top.
- **Re-submitting the key, or calling `resume(id)`, while it is still waiting runs nothing.** The
  handle fails with `ExecutionWaitingSignal`.
- **Waits have no timeout yet**, and a waiting execution can be cancelled like any other.
- The payload must be a recordable type (a record, an enum, a string, number, `java.time` value or
  `UUID`, or collections of those) **and** the type the task awaits. `signal` refuses anything else
  with nothing recorded, because a recorded signal is replayed into every resume.

## Cancel, pause, resume

```java
runtime.cancel(id);    // cooperative: stops at the next boundary, folds to CANCELLED
runtime.pause(id);     // only for an execution not running in this process; folds to PAUSED
runtime.resume(id);    // continues a paused (or crashed) execution
```

`cancel` on a running execution trips a flag and interrupts its worker. The task stops at its **next
`ctx` call**, which records `ExecutionCancelled`. Two cases where you get something else:

- **The task returns without making another `ctx` call.** It completes normally and the cancel has no
  effect, exactly like cancelling an execution that has already finished. A long stretch of local
  work after the last call is not interrupted.
- **The interrupt lands inside a call** and surfaces as an ordinary exception, for example from an
  interrupted HTTP request. The execution is recorded `FAILED`, not `CANCELLED`. It is not retried, because a cancel is pending.

`close()` shuts the runtime down gracefully. Running executions finish their current call, then stop
without recording a failure, and the next start resumes them. That holds for calls that finish within
`drainTimeout` (default 10s). After that, the log is closed under any call still running, which leaves
what a crash would. See [deployment.md](deployment.md#shutdown-drain-dont-kill).

## Inspect, replay, branch

```java
ExecutionState state = runtime.inspect(id);           // status, result/error, attempts, timeline
Timeline t = state.timelineView();                     // model/tool calls, tokens, cost, latency
```

`inspect` is a fold over the log, made fast for long executions by periodic snapshots.

**Replay** re-runs a recorded execution with every boundary substituted and **zero external calls**,
verifying each boundary against the recording:

```java
runtime.replay(id, new TriageTask());   // throws NonDeterministicReplayException on divergence
```

Use it in tests to catch nondeterministic task code, or to re-run an old execution under a new build.

**Branch** forks a recorded execution at a step and runs forward with something changed, without
touching the original:

```java
Trajectory fork = runtime.branch(id, atSeq)
        .withModel(otherModel)                                // a different model from atSeq on
        .withRecordedToolResult("create-ticket", "T-999")     // or a counterfactual tool result
        .run(new TriageTask());

TrajectoryDiff diff = Trajectory.diff(Trajectory.of(runtime.inspect(id)), fork);
```

The fork is a new execution; the diff shows which steps changed.

Past the fork a branch runs live, tools included. To explore without repeating side effects, answer
tool calls from the recording instead:
`.toolReplay(BranchSpec.ToolReplay.RECORDED_ONLY)` (or `RECORDED_OR_LIVE` to run only calls the
recording can't answer).

## Agents

`catalyst-agents` (included in `catalyst-api`) is a tool-calling agent loop: the model decides
which tools to call, the agent runs them and sends back the results, until the model answers or a
step budget runs out.

```java
Agent triage = Agent.builder()
        .instructions("You triage support tickets. Look the ticket up before deciding.")
        .tool(ticketLookup, "Look up a ticket by its id")
        .tool(pager, "Page a team's on-call engineer")
        .maxSteps(8)
        .build();

final class TriageTask implements Task<String> {
    private final String ticket;
    TriageTask(String ticket) { this.ticket = ticket; }
    TriageTask() { this(null); }                     // for the registry: resume substitutes the input
    @Override public String execute(Context ctx) {
        String input = ctx.effect("ticket", () -> ticket);
        return triage.run(ctx, input).answer();      // AgentResult: answer, stop reason, turns, tool calls
    }
}
```

**An agent is ordinary task code.** Every model turn is a recorded model call and every tool call a
recorded tool boundary, so an agent run gets everything an execution gets:
- **Resume:** a crash mid-conversation resumes at the turn it reached, repeating no model or tool
  call.
- **Replay:** a strict replay re-runs the whole conversation with zero calls.
- **Branch, eval and the timeline report** work on it unchanged. An eval re-runs the conversation
  with the candidate model and answers its tool calls from the recording.

Some specifics:
- **Tool inputs:** a record of strings, numbers, booleans and enums (a flat object, which
  tool-calling models handle reliably), or a single such value, offered as `{"value": ...}`. The
  JSON schema is derived from the type, and anything richer is refused when the agent is built.
- **Tool failures go back to the model as data,** so it can recover: an unknown tool, arguments that
  don't fit, or a tool that throws. Failure text is identical live and on replay.
- **Engine signals are never swallowed:** cancel, shutdown, waits, pauses, a stale writer, a
  divergence, an in-doubt call.
- **Inputs:** pass the task's input through a recorded boundary, as above, not vars (they aren't
  persisted).
- **Real models:** `LangChain4jModel` carries tool calls and tool results (linked by call id) both
  ways.

### Tools from an MCP server

`catalyst-mcp` (opt-in, like `catalyst-langchain4j`) turns the tools an MCP server offers into
Catalyst tools, using the official MCP Java SDK:

```java
McpSyncClient client = McpClient.sync(new StdioClientTransport(
        ServerParameters.builder("npx").args("-y", "@acme/ticket-mcp").build(),
        new JacksonMcpJsonMapper(new ObjectMapper()))).build();
client.initialize();

Agent agent = McpToolset.from(client)          // every tool the server lists
        .only("lookup_ticket", "escalate")     // optional: a fixed subset
        .addTo(Agent.builder().instructions("You triage support tickets."))
        .build();
```

Each MCP call goes through `ctx.call`, so it is a recorded boundary like any other tool call:
- **Resume, replay and eval substitute the recorded result,** and a replay never contacts the server.
- **A result with `isError: true`** is recorded as the call's failure, and the agent tells the model.
- **Text content** goes back to the model. Images and other non-text content are described as
  placeholders.

**The tool list is part of the recording.** Every model request carries the tools' names and schemas
(schemas canonicalised, so key order doesn't matter), and both feed the request hash. A replay or
resume must offer the same tools with the same schemas. Pin the server's version, or use `only(...)`.

Any tool that takes its arguments as JSON with its own schema can join an agent the same way:
`Agent.builder().tool(jsonTool, description, inputSchemaJson)`.

## Evaluating models and prompts

The log is a dataset. `catalyst-eval` (included in `catalyst-api`) replays recorded executions
against candidate models and prompt rewrites and scores them, so a model upgrade or a prompt change
can be checked against real traffic before it ships:

```java
EvalReport report = Eval.on(runtime)
        .cases(recordedIds)                                    // completed executions
        .task(new TriageTask())                                // the production task
        .candidate(Candidate.model("mini", miniModel))         // a cheaper model
        .candidate(Candidate.task("prompt-v2", new TriageTaskV2()))   // rewritten prompts
        .scorer(Scorers.sameResult())
        .scorer(Scorers.costAtMost(0.5))
        .scorer(Scorer.check("names-the-floor", s -> String.valueOf(s.candidateResult()).contains("floor")))
        .run();
System.out.print(report.pretty());   // pass rates, token and cost deltas, and why each failure failed
```

How each case runs:
- **It branches just before the recorded run's first model call.** Everything the run did before
  that (clock reads, ids, lookups) is substituted exactly, so only the candidate's change differs.
- **Tools are never re-run.** A tool call the recorded run made with the same input gets its
  recorded output. A call it never made ends the case as `UNRECORDED_TOOL`, a finding that the
  candidate took a different path; nothing is sent to the outside world. Opt into live tools with
  `.toolReplay(RECORDED_OR_LIVE)`.
- **Only the candidate's model is called.** A candidate without a model uses the runtime's default.
- **Every evaluated run is a branch execution in the log** (`CaseResult.branchId()`), which you can
  inspect, render or replay like any other.

Scorers are pure functions of a `Sample`: both results, both trajectories, and their diff. The
built-ins are `sameResult`, `sameSteps`, `costAtMost` and `tokensAtMost`, and `Scorer.check(name,
predicate)` covers a domain check. Task vars are not persisted (only their hash is), so a case's
input must reach the task through a recorded boundary, such as `ctx.effect("ticket", ...)` or a tool
call, to be replayed. A task that reads `ctx.var(...)` sees nothing in an evaluation.

## Streaming completions

A task that wants tokens as they arrive asks for a sink instead of a return value:

```java
Task<String> task = ctx -> {
    StringBuilder assembled = new StringBuilder();
    ctx.model().stream(
            CompletionRequest.of(Prompt.builder().user("describe streaming").build()),
            chunk -> {
                ui.append(chunk);          // incremental delivery
                assembled.append(chunk);
            });
    return assembled.toString();
};
```

Streaming is a **delivery** concern, not a durability one. Whatever the provider does on the wire, the
boundary Catalyst records is the assembled `Completion`: the same single `CompletionReceived` event a
non-streaming call writes. So nothing about resume, replay or branch had to change. A streamed
execution and a non-streamed one look the same in the log and are interchangeable on replay. On
replay, the recorded text is re-emitted to the sink and no provider is contacted.

`Model.stream` is a **default method**, so every existing `Model` keeps working; one that cannot stream
hands the sink the whole message as one chunk. `LangChain4jModel.streaming(streamingChatModel)` adapts
a real streaming provider.

Two things to know:

- **Chunk boundaries are not recorded.** A replay delivers the recorded text in a single chunk.
  Accumulating chunks is safe; branching on *how many* arrived is not, and is the one way streaming can
  make a task nondeterministic.
- **The sink runs on the task's thread**, before the boundary is recorded. It must not call
  `ctx.effect`, `ctx.call`, or another completion: that would append an event between this boundary's
  request and its result, which no replay can match.

## Timeline reports and OpenTelemetry

The log already contains everything an execution did, so observability is a fold over it, not a
separate instrumentation layer. Both exporters are read-only and post-hoc: an execution recorded months
ago renders as well as a fresh one.

**HTML timeline** (`catalyst-timeline`):

```java
TimelineReport.writeTo(runtime.inspect(id), Path.of("build/reports/" + id.value() + ".html"));
```

You get a status header, the roll-ups (model/tool calls, prompt and completion tokens, cost, boundary
latency, wall clock), and the step-by-step trajectory: each boundary with its kind, label, offset,
latency, a **latency bar** on the execution's time axis, and its recorded payload in a collapsed
block. The bars form a waterfall, so the slow model and tool calls stand out.

**Branch comparison:** render an original run and a branch of it (or an eval candidate's run, since
`CaseResult.sample()` carries both trajectories) side by side:

```java
Trajectory base = Trajectory.of(runtime.inspect(id));
Trajectory fork = runtime.branch(id, atSeq).withModel(otherModel).run(task);
TimelineReport.writeDiffTo(base, fork, Path.of("build/reports/branch.html"));
```

Each work step is aligned and marked SAME, CHANGED, ADDED or REMOVED. A changed row previews what
each side said, and the header shows the token and cost delta. Both pages are:

- **self-contained:** inline CSS, no scripts, no fonts or images, so it opens from a `file://` URL and
  can be attached to a ticket
- **deterministic:** two renders of the same execution are byte-identical, so reports diff cleanly
- **escaped:** tool names, labels and payloads are log content, and are HTML-escaped

**OpenTelemetry** (`catalyst-otel`, API only; your app supplies the SDK and exporter):

```java
CatalystTracer.of(openTelemetry).export(id, runtime.log().read(id));
```

This produces one trace per execution: a root span for the run, a child span per boundary (model spans
carry tokens, cost and finish reason), and lifecycle moments such as retries and pauses as span events.

## Auto-capture: nondeterminism without the ceremony

The manual way to honour the determinism rule is to route each nondeterministic call through
`ctx.effect(...)`:

```java
Task<String> task = ctx -> {
    String stamp = ctx.effect("stamp", () -> Instant.now().toString());
    String id = ctx.effect("id", () -> UUID.randomUUID().toString());
    return stamp + "|" + id;
};
```

Add the `catalyst-agent` module and you can write the same task the way you'd write any Java:

```java
public final class StampedReport implements Task<String> {
    @Override public String execute(Context ctx) {
        return Instant.now() + "|" + UUID.randomUUID();   // no ctx.effect, still replayable
    }
}
```

Point the agent at the packages holding your task code. You can do it at launch, using the
self-contained `javaagent`-classified artifact (a `-javaagent` jar does not get its Maven
dependencies):

```bash
java -javaagent:catalyst-agent-<version>-javaagent.jar=packages=com.acme.tasks -cp ... com.acme.Main
```

or from inside the process, using the ordinary `catalyst-agent` dependency. This path also
retransforms classes that are already loaded:

```java
AutoCaptureAgent.install(AutoCaptureConfig.forPackages("com.acme.tasks"));
```

Each rewritten call site records its value as an ordinary effect boundary (`auto:Instant.now#0`,
`auto:UUID.randomUUID#1`, …), so resume, replay and branch substitute it like any other.

**What is captured:**
- `Instant.now()` and the other no-argument `java.time` factories
- `System.currentTimeMillis()` / `nanoTime()`
- `UUID.randomUUID()`
- `Math.random()`
- draws from `Random` and its relatives, including `ThreadLocalRandom` and `SplittableRandom`

Narrow that with `sources=time,identity` when a source isn't something your task treats as data.

Worth knowing:

- **Packages are opt-in, with no default.** Instrumenting a whole classpath would rewrite library
  internals whose `nanoTime` calls are none of Catalyst's business.
- **Without the agent, nothing changes.** An instrumented class calls the plain JDK method when no
  execution is bound to the thread, so it behaves normally in unit tests and ordinary code.
- **Nondeterminism already inside a boundary is left alone.** A `ctx.effect` supplier or a tool body
  is covered by that boundary's own recorded result.
- **The rule still holds.** Auto-capture removes the bookkeeping, not the discipline: a task whose
  *sequence* of calls varies between runs still diverges, and strict replay still says so.

## How the log works

Every important state transition is an event in an append-only log. `CatalystEvent` is a sealed
hierarchy covering:

- lifecycle: created, started, resumed, paused, completed, failed, cancelled, retry requested, branched
- boundaries: prompt built, completion requested/received, tool requested/completed, effect recorded,
  memory read/written

Each event gets a dense, per-execution `seq` (0, 1, 2, …). Execution state, the timeline, token counts
and cost are all **folds** over those events (`Reducer`). Nothing is stored that the log can't rebuild.

Two `EventLog` implementations sit behind one SPI:

- **`InMemoryEventLog`**: tests and ephemeral use; `CatalystRuntime.builder().log(EventLogs.inMemory())`.
- **`GumboEventLog`**: durable and file-backed, the embedded production mode; it's what
  `Catalyst.builder(dir)` uses.

**On Gumbo:**
- Each execution is one Gumbo `LogTag` (`catalyst-exec/<id>`), and Gumbo's per-tag `streamVersion`
  *is* Catalyst's `seq`.
- Tail reads are therefore version-keyed (`readAfterVersion`), never keyed on the log's global
  `seqnum`.
- Lifecycle events that open or close an execution are also tagged into a shared
  `catalyst-tasks/default` queue in the same atomic append. That queue is the execution index startup
  recovery reads.
- The idempotency-key index and fold snapshots live in Gumbo's durable key-value store.

**Storage details:**
- **Large payloads:** fields of 64 KiB or more are stored out of line in a content-addressed blob
  store (`<dir>/blobs` by default, or your own with `Catalyst.builder(dir).blobStore(store)`) and
  rehydrated transparently on read.
- **Schema evolution:** older logs stay readable. See [`schema-evolution.md`](schema-evolution.md).
- **Distributed execution:** where the log is heading for that is in
  [`distribution.md`](distribution.md).
