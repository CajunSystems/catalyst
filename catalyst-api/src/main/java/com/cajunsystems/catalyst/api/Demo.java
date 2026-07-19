package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionInfo;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.Tool;
import com.cajunsystems.catalyst.engine.CostModel;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.InDoubtPolicy;
import com.cajunsystems.catalyst.engine.PayloadCodec;
import com.cajunsystems.catalyst.engine.ReplayingContext;
import com.cajunsystems.catalyst.engine.RetryPolicy;
import com.cajunsystems.catalyst.engine.Timeline;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.EventJson;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Message;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.cajunsystems.catalyst.runtime.ExecutionHandle;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The M0 exit demo (spec §11), runnable by hand. Two modes tell the {@code kill -9} story across
 * <em>separate processes</em>:
 *
 * <pre>
 *   # 1) record the first step, then die abruptly (exit 137 = kill -9), leaving a durable partial log
 *   java -cp ... com.cajunsystems.catalyst.api.Demo /tmp/catalyst-demo record
 *   # 2) resume from the durable log with the same key — step 1 is substituted, only step 2 runs live
 *   java -cp ... com.cajunsystems.catalyst.api.Demo /tmp/catalyst-demo resume
 * </pre>
 *
 * With no arguments it runs both phases in-process against a fresh temp directory.
 */
public final class Demo {

    private static final String KEY = "doc:42";

    private static final CompletionRequest STEP1 =
            CompletionRequest.of(Prompt.builder().system("summarizer").user("summarize the document").build());
    private static final CompletionRequest STEP2 =
            CompletionRequest.of(Prompt.builder().user("produce the final answer").build());

    private static final Task<String> TWO_STEP = ctx -> {
        String summary = ctx.model().complete(STEP1).message();
        String finalAnswer = ctx.model().complete(STEP2).message();
        return summary + "|" + finalAnswer;
    };

    /**
     * The same two-step task as a <em>named</em> class. A lambda's synthetic class name is not stable
     * across processes, but a named class records a fixed {@code taskType} — so a {@link
     * com.cajunsystems.catalyst.TaskRegistry} can reconstruct it and drive {@code resume(id)} from the
     * log alone.
     */
    static final class TwoStepTask implements Task<String> {
        @Override public String execute(Context ctx) throws Exception {
            String summary = ctx.model().complete(STEP1).message();
            String finalAnswer = ctx.model().complete(STEP2).message();
            return summary + "|" + finalAnswer;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length >= 2 && args[1].equals("record")) {
            record(Path.of(args[0]));
            System.out.println("[record] step 1 durably recorded; simulating kill -9 (exit 137)...");
            Runtime.getRuntime().halt(137); // abrupt death: no completion event, no graceful close
        } else if (args.length >= 2 && args[1].equals("resume")) {
            resume(Path.of(args[0]));
        } else if (args.length >= 1 && args[0].equals("replay")) {
            replayDemo(Files.createTempDirectory("catalyst-replay-"));
        } else if (args.length >= 1 && args[0].equals("branch")) {
            branchDemo(Files.createTempDirectory("catalyst-branch-"));
        } else if (args.length >= 1 && args[0].equals("snapshot")) {
            snapshotDemo(Files.createTempDirectory("catalyst-snapshot-"));
        } else if (args.length >= 1 && args[0].equals("cancel")) {
            cancelDemo(Files.createTempDirectory("catalyst-cancel-"));
        } else if (args.length >= 1 && args[0].equals("resumeid")) {
            resumeByIdDemo(Files.createTempDirectory("catalyst-resumeid-"));
        } else if (args.length >= 1 && args[0].equals("tools")) {
            toolsDemo(Files.createTempDirectory("catalyst-tools-"));
        } else if (args.length >= 1 && args[0].equals("collections")) {
            collectionsDemo(Files.createTempDirectory("catalyst-collections-"));
        } else if (args.length >= 1 && args[0].equals("retry")) {
            retryDemo(Files.createTempDirectory("catalyst-retry-"));
        } else if (args.length >= 1 && args[0].equals("blob")) {
            blobDemo(Files.createTempDirectory("catalyst-blob-"));
        } else if (args.length >= 1 && args[0].equals("schema")) {
            schemaDemo();
        } else {
            Path dir = Files.createTempDirectory("catalyst-demo-");
            System.out.println("No args: running record + resume in-process under " + dir);
            record(dir);
            resume(dir);
        }
    }

    /**
     * The M1 exit demo: record a complete execution, then replay it with substitution — zero
     * external calls, hashes verified — and show that a divergent task is caught.
     */
    private static void replayDemo(Path dir) {
        MockModel model = summarizerModel();
        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir))
                .model(model)
                .costModel(CostModel.perMillionTokens(3.0, 15.0)) // priced so cost folds in
                .build()) {

            ExecutionId id = runtime.execute(TWO_STEP, ExecutionOptions.withKey(KEY)).id();
            runtime.execute(TWO_STEP, ExecutionOptions.withKey(KEY)).result(); // ensure completed
            int callsAfterRecord = model.callCount();
            Timeline t = runtime.inspect(id).timelineView();
            System.out.println("[replay] recorded execution " + id.value());
            System.out.println("[replay] timeline: " + t.modelCalls() + " model call(s), "
                    + t.totalTokens() + " tokens, $" + String.format("%.6f", t.totalCostUsd()));

            // Exact replay with substitution — must make no new model calls.
            ExecutionState replayed = runtime.replay(id, TWO_STEP);
            int externalCalls = model.callCount() - callsAfterRecord;
            System.out.println("[replay] replayed -> " + replayed.status()
                    + "; external calls during replay: " + externalCalls);
            if (externalCalls != 0) throw new AssertionError("replay made " + externalCalls + " external calls");

            // A divergent task (different prompt) must be rejected.
            Task<String> divergent = ctx ->
                    ctx.model().complete(CompletionRequest.of(Prompt.builder().user("a DIFFERENT prompt").build())).message();
            try {
                runtime.replay(id, divergent);
                throw new AssertionError("divergent replay was not detected");
            } catch (com.cajunsystems.catalyst.engine.NonDeterministicReplayException e) {
                System.out.println("[replay] divergence correctly detected at seq " + e.seq());
            }
        }
    }

    /** Durably records the first model call, then returns (the caller then dies to simulate a crash). */
    private static void record(Path dir) {
        MockModel model = summarizerModel();
        ExecutionId id = ExecutionId.random();
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            Instant t = Instant.now();
            log.putKey(KEY, id);
            log.append(id, new CatalystEvent.ExecutionCreated(t, TWO_STEP.getClass().getName(), "h", "cfg", KEY));
            log.append(id, new CatalystEvent.ExecutionStarted(t, 1, "node-0"));

            ExecutionInfo info = new ExecutionInfo(id, 1, "TwoStep", Map.of());
            ReplayingContext ctx = new ReplayingContext(id, log, model, info, Map.of(),
                    EventJson.shared(), new PayloadCodec(), InDoubtPolicy.FAIL, CostModel.free(),
                    com.cajunsystems.catalyst.ReplayMode.STRICT, null,
                    Clock.systemUTC(), LoggerFactory.getLogger("demo.record"), log.read(id), true);
            String summary = ctx.model().complete(STEP1).message();
            System.out.println("[record] execution " + id.value());
            System.out.println("[record] step 1 result: " + summary + " (model calls this run: " + model.callCount() + ")");
        }
    }

    /** Reopens the durable log and resumes to completion, proving step 1 is not re-called. */
    private static void resume(Path dir) {
        MockModel model = summarizerModel();
        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir)).model(model).build()) {
            String result = runtime.execute(TWO_STEP, ExecutionOptions.withKey(KEY)).result();
            ExecutionId id = runtime.log().findByKey(KEY).orElseThrow();
            ExecutionState state = runtime.inspect(id);
            System.out.println("[resume] execution " + id.value() + " -> " + state.status());
            System.out.println("[resume] result: " + result);
            System.out.println("[resume] model calls THIS run: " + model.callCount()
                    + " (step 1 was substituted from the log; only step 2 ran live)");
            System.out.println("[resume] tokens: " + state.cost().totalTokens()
                    + ", steps recorded: " + state.trajectory().size());
        }
    }

    /**
     * The M2 exit demo: record a 2-step execution, branch after step 1 with a different model, and
     * print the step-by-step diff between the original and the fork.
     */
    private static void branchDemo(java.nio.file.Path dir) {
        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir))
                .model(answerModel("FINAL"))
                .build()) {

            ExecutionHandle<String> handle = runtime.execute(TWO_STEP, ExecutionOptions.withKey(KEY));
            String parentResult = handle.result();
            ExecutionId id = handle.id();

            com.cajunsystems.catalyst.engine.ExecutionState parent = runtime.inspect(id);
            long step1Seq = parent.trajectory().stream()
                    .filter(s -> s.kind() == com.cajunsystems.catalyst.engine.TimelineStep.Kind.MODEL)
                    .findFirst().orElseThrow().seq();

            com.cajunsystems.catalyst.engine.Trajectory fork =
                    runtime.branch(id, step1Seq).withModel(answerModel("FINAL-B")).run(TWO_STEP);
            com.cajunsystems.catalyst.engine.TrajectoryDiff diff =
                    com.cajunsystems.catalyst.engine.Trajectory.diff(
                            com.cajunsystems.catalyst.engine.Trajectory.of(parent), fork);

            System.out.println("[branch] parent " + id.value() + " -> " + parentResult);
            System.out.println("[branch] fork   " + fork.id().value()
                    + " (model swapped from step at seq " + step1Seq + ")");
            System.out.println("[branch] diff (" + diff.changedCount() + " step(s) changed):");
            System.out.print(diff.pretty());
        }
    }

    /**
     * The v0.2 durability exit demo (roadmap — Snapshots): record a long execution, then show that
     * {@code inspect} folds from a checkpoint instead of re-folding the whole log, and that the
     * snapshot fold matches a full re-fold exactly.
     */
    private static void snapshotDemo(Path dir) {
        final int steps = 250;
        Task<Integer> counter = ctx -> {
            int sum = 0;
            for (int i = 0; i < steps; i++) {
                final int step = i;
                sum += ctx.effect("step-" + step, () -> step);
            }
            return sum;
        };

        // A tiny counting wrapper so the demo can report how many events each fold reads.
        long[] readFromEvents = {0};
        try (GumboEventLog inner = GumboEventLog.at(dir)) {
            com.cajunsystems.catalyst.log.EventLog counting = new com.cajunsystems.catalyst.log.EventLog() {
                @Override public long append(ExecutionId id, CatalystEvent e) { return inner.append(id, e); }
                @Override public java.util.List<com.cajunsystems.catalyst.events.SequencedEvent> read(ExecutionId id) { return inner.read(id); }
                @Override public java.util.List<com.cajunsystems.catalyst.events.SequencedEvent> readFrom(ExecutionId id, long after) {
                    var tail = inner.readFrom(id, after);
                    readFromEvents[0] += tail.size();
                    return tail;
                }
                @Override public long latestSeq(ExecutionId id) { return inner.latestSeq(id); }
                @Override public java.util.Optional<ExecutionId> findByKey(String k) { return inner.findByKey(k); }
                @Override public void putKey(String k, ExecutionId id) { inner.putKey(k, id); }
                @Override public java.util.Optional<com.cajunsystems.catalyst.log.Snapshot> readSnapshot(ExecutionId id) { return inner.readSnapshot(id); }
                @Override public void writeSnapshot(ExecutionId id, com.cajunsystems.catalyst.log.Snapshot s) { inner.writeSnapshot(id, s); }
                @Override public void close() { /* inner closed by try-with-resources */ }
            };

            try (CatalystRuntime runtime = Catalyst.builder().log(counting).snapshotInterval(100).build()) {
                // A single blocking run: a fresh execute folds via Reducer.fold and never calls
                // foldState, so it deterministically writes no snapshot and the first inspect below is
                // the genuine cold path. (Two idempotent execute() calls would race: if the first
                // completed between them, the second would fold and pre-write a snapshot.)
                ExecutionHandle<Integer> handle = runtime.execute(counter, ExecutionOptions.withKey("count:1"));
                int result = handle.result();
                ExecutionId id = handle.id();
                long total = counting.read(id).size();
                System.out.println("[snapshot] recorded execution " + id.value() + " -> " + result
                        + " (" + total + " events, snapshot interval 100)");

                readFromEvents[0] = 0;
                ExecutionState cold = runtime.inspect(id); // no snapshot yet: folds the whole log, writes one
                long coldFolded = readFromEvents[0];

                readFromEvents[0] = 0;
                ExecutionState warm = runtime.inspect(id); // restores the snapshot, folds only the tail
                long warmFolded = readFromEvents[0];

                ExecutionState fullRefold = com.cajunsystems.catalyst.engine.Reducer.fold(id, counting.read(id));
                boolean matches = warm.status() == fullRefold.status()
                        && warm.lastSeq() == fullRefold.lastSeq()
                        && warm.trajectory().equals(fullRefold.trajectory());

                System.out.println("[snapshot] cold inspect folded " + coldFolded + " events");
                System.out.println("[snapshot] warm inspect folded " + warmFolded + " events (from snapshot)");
                System.out.println("[snapshot] snapshot fold matches full fold: " + matches);
                if (warmFolded >= coldFolded) throw new AssertionError("warm fold did not use the snapshot");
                if (!matches) throw new AssertionError("snapshot fold diverged from full fold");
                System.out.println("[snapshot] cold=" + cold.status() + " warm=" + warm.status());
            }
        }
    }

    /**
     * The v0.2 cancellation exit demo (roadmap increment ①): a running task is cancelled cooperatively
     * and folds to {@code CANCELLED} — a clean, deliberate stop, distinct from {@code FAILED} — with the
     * post-cancel boundary never executed (zero wasted model calls after the cancel).
     */
    private static void cancelDemo(Path dir) throws Exception {
        MockModel model = answerModel("FINAL");
        java.util.concurrent.CountDownLatch pastFirstBoundary = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);

        // Two model boundaries with a park between them; cancellation is requested while parked.
        Task<String> twoStep = ctx -> {
            String first = ctx.model().complete(STEP1).message();
            pastFirstBoundary.countDown();
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return first + "|" + ctx.model().complete(STEP2).message();
        };

        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir)).model(model).build()) {

            ExecutionHandle<String> handle = runtime.execute(twoStep, ExecutionOptions.withKey(KEY));
            if (!pastFirstBoundary.await(5, java.util.concurrent.TimeUnit.SECONDS)) {
                throw new AssertionError("task never reached the first boundary");
            }

            runtime.cancel(handle.id()); // cooperative: trip the token + interrupt the parked worker
            release.countDown();

            String outcome;
            try {
                handle.result();
                outcome = "COMPLETED (unexpected)";
            } catch (java.util.concurrent.CancellationException e) {
                outcome = "CancellationException";
            }

            ExecutionState state = runtime.inspect(handle.id());
            System.out.println("[cancel] execution " + handle.id().value() + " -> " + state.status());
            System.out.println("[cancel] handle completed with: " + outcome);
            System.out.println("[cancel] model calls before cancel: " + model.callCount()
                    + " (the post-cancel boundary was not executed)");
            System.out.println("[cancel] terminal? " + state.isTerminal() + "; reason: " + state.error());

            if (state.status() != com.cajunsystems.catalyst.Status.CANCELLED) {
                throw new AssertionError("expected CANCELLED, got " + state.status());
            }
            if (model.callCount() != 1) {
                throw new AssertionError("expected exactly 1 model call, got " + model.callCount());
            }
        }
    }

    /** Input for the flaky demo tool. */
    record FlakyIn(String v) {}

    /** A tool that fails its first {@code failures} live invocations, then succeeds; counts invocations. */
    static final class FlakyTool implements Tool<FlakyIn, String> {
        final AtomicInteger invocations = new AtomicInteger();
        private final int failures;
        FlakyTool(int failures) { this.failures = failures; }
        public String name() { return "flaky"; }
        public Class<FlakyIn> inputType() { return FlakyIn.class; }
        public String apply(FlakyIn in) {
            int n = invocations.incrementAndGet();
            if (n <= failures) throw new RuntimeException("transient failure #" + n);
            return "ok:" + in.v();
        }
    }

    /**
     * The v0.2 retry exit demo (roadmap §13.3): a transient tool failure is retried as a new attempt on
     * the same stream. The failing boundary re-runs live while the successful prefix (the model call) is
     * substituted, bounded by the policy — and the retried log still replays exactly. A default
     * ({@link RetryPolicy#none()}) run shows the opt-in nature: the same failure is terminal.
     */
    private static void retryDemo(Path dir) throws Exception {
        // Case 1 — a flaky tool fails twice then succeeds, under a bounded retry policy.
        MockModel model = answerModel("FINAL");
        FlakyTool flaky = new FlakyTool(2);
        Task<String> task = ctx -> {
            String m = ctx.model().complete(STEP1).message();
            String r = ctx.call(flaky, new FlakyIn("x"));
            return m + "|" + r;
        };

        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir.resolve("ok")))
                .model(model)
                .retryPolicy(RetryPolicy.maxRetries(3, Duration.ofMillis(10)))
                .build()) {

            ExecutionHandle<String> handle = runtime.execute(task, ExecutionOptions.withKey(KEY));
            String result = handle.result();
            ExecutionState state = runtime.inspect(handle.id());
            int modelCallsAfterRun = model.callCount();

            System.out.println("[retry] transient tool failure retried -> " + state.status());
            System.out.println("[retry] result: " + result);
            System.out.println("[retry] tool invocations: " + flaky.invocations.get());
            System.out.println("[retry] model calls across retries: " + modelCallsAfterRun);
            System.out.println("[retry] retries recorded: " + state.retries());

            // The retried log still replays exactly: no boundary is re-executed live.
            int toolBeforeReplay = flaky.invocations.get();
            ExecutionState replayed = runtime.replay(handle.id(), task);
            int externalDuringReplay = (model.callCount() - modelCallsAfterRun)
                    + (flaky.invocations.get() - toolBeforeReplay);
            System.out.println("[retry] external calls during replay of the retried execution: "
                    + externalDuringReplay);
            System.out.println("[retry] replay -> " + replayed.status());

            if (state.status() != Status.COMPLETED) {
                throw new AssertionError("expected COMPLETED, got " + state.status());
            }
            if (flaky.invocations.get() != 3) {
                throw new AssertionError("expected 3 tool invocations (2 fail + 1 ok), got "
                        + flaky.invocations.get());
            }
            if (modelCallsAfterRun != 1) {
                throw new AssertionError("expected the model prefix substituted (1 call), got "
                        + modelCallsAfterRun);
            }
            if (state.retries() != 2) {
                throw new AssertionError("expected 2 retries recorded, got " + state.retries());
            }
            if (externalDuringReplay != 0) {
                throw new AssertionError("expected 0 external calls during replay, got "
                        + externalDuringReplay);
            }
        }

        // Case 2 — the default policy (none) is opt-in: the same transient failure is terminal.
        MockModel model2 = answerModel("FINAL");
        FlakyTool flaky2 = new FlakyTool(2);
        Task<String> task2 = ctx -> {
            String m = ctx.model().complete(STEP1).message();
            String r = ctx.call(flaky2, new FlakyIn("x"));
            return m + "|" + r;
        };
        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir.resolve("none")))
                .model(model2)
                .build()) { // no retry policy → RetryPolicy.none()

            ExecutionHandle<String> handle = runtime.execute(task2, ExecutionOptions.withKey(KEY));
            try {
                handle.result();
                throw new AssertionError("expected failure under the default policy");
            } catch (RuntimeException expected) {
                // the transient failure is terminal
            }
            ExecutionState state = runtime.inspect(handle.id());
            System.out.println("[retry] default policy (none) -> " + state.status()
                    + " on first failure, tool invocations: " + flaky2.invocations.get());

            if (state.status() != Status.FAILED) {
                throw new AssertionError("expected FAILED under default policy, got " + state.status());
            }
            if (flaky2.invocations.get() != 1) {
                throw new AssertionError("expected 1 tool invocation under default policy, got "
                        + flaky2.invocations.get());
            }
        }

        System.out.println("[retry] retry criterion holds: bounded retry-as-attempt, "
                + "prefix substituted, replay exact, opt-in.");
    }

    /**
     * The v0.2 resume-by-id exit demo (roadmap increment ②): durably record a task's first step, then
     * recover it <em>from its id alone</em> — no idempotency key, no re-submitting the task instance.
     * The runtime reconstructs the task from its recorded type via the {@code TaskRegistry}, substitutes
     * step 1 from the log, and runs only step 2 live (zero duplicate model calls).
     */
    private static void resumeByIdDemo(Path dir) {
        MockModel model = summarizerModel();
        String taskType = TwoStepTask.class.getName();
        ExecutionId id = ExecutionId.random();

        // ── Phase 1: durably record step 1 against the NAMED task, then "crash" (no completion) ──
        try (GumboEventLog log = GumboEventLog.at(dir)) {
            Instant t = Instant.now();
            log.append(id, new CatalystEvent.ExecutionCreated(t, taskType, "h", "cfg", ""));
            log.append(id, new CatalystEvent.ExecutionStarted(t, 1, "node-0"));

            ExecutionInfo info = new ExecutionInfo(id, 1, taskType, Map.of());
            ReplayingContext ctx = new ReplayingContext(id, log, model, info, Map.of(),
                    EventJson.shared(), new PayloadCodec(), InDoubtPolicy.FAIL, CostModel.free(),
                    com.cajunsystems.catalyst.ReplayMode.STRICT, null,
                    Clock.systemUTC(), LoggerFactory.getLogger("demo.resumeid"), log.read(id), true);
            String summary = ctx.model().complete(STEP1).message();
            System.out.println("[resume-id] recorded execution " + id.value() + " step 1 = " + summary
                    + " (model calls this run: " + model.callCount() + ")");
            // kill -9: control leaves here with no ExecutionCompleted written.
        }

        // ── Phase 2: reopen the durable log, register the task TYPE, recover from the id alone ──
        try (CatalystRuntime runtime = Catalyst.builder()
                .log(GumboEventLog.at(dir))
                .model(model)
                .task(new TwoStepTask())   // register the type so resume(id) can reconstruct it
                .build()) {

            String result = (String) runtime.resume(id).result();  // no key, no re-submitted task
            ExecutionState state = runtime.inspect(id);

            System.out.println("[resume-id] resumed " + id.value() + " -> " + state.status());
            System.out.println("[resume-id] result: " + result);
            System.out.println("[resume-id] model calls THIS run: " + model.callCount()
                    + " (step 1 was substituted from the log; only step 2 ran live)");

            if (state.status() != com.cajunsystems.catalyst.Status.COMPLETED) {
                throw new AssertionError("expected COMPLETED, got " + state.status());
            }
            if (model.callCount() != 2) {
                throw new AssertionError("expected 2 model calls total, got " + model.callCount());
            }
            System.out.println("[resume-id] resume-by-id criterion holds: recovered from the id alone,"
                    + " zero duplicate model calls.");
        }
    }

    /**
     * The v0.2 built-in-tools exit demo (roadmap increment ③): a task fetches over HTTP and writes the
     * response to a sandboxed filesystem, both through {@code ctx.call(...)}. A strict replay
     * substitutes both boundaries — the HTTP request is not re-issued and the file write is not
     * re-applied — which is exactly what makes external I/O safe inside a resumable/replayable
     * execution. Uses a fake HTTP sender so the demo needs no network.
     */
    private static void toolsDemo(Path dir) throws Exception {
        Path logDir = Files.createDirectory(dir.resolve("log"));
        Path sandbox = Files.createDirectory(dir.resolve("sandbox"));

        int[] httpCalls = {0};
        com.cajunsystems.catalyst.tools.HttpTool http =
                new com.cajunsystems.catalyst.tools.HttpTool(req -> {
                    httpCalls[0]++;
                    return new com.cajunsystems.catalyst.tools.HttpTool.Response(200, "{\"answer\":42}", "application/json");
                });
        com.cajunsystems.catalyst.tools.FilesystemTool fs =
                new com.cajunsystems.catalyst.tools.FilesystemTool(sandbox);

        Task<String> fetchAndSave = ctx -> {
            var resp = ctx.call(http, com.cajunsystems.catalyst.tools.HttpTool.Request.get("https://api.example/data"));
            ctx.call(fs, com.cajunsystems.catalyst.tools.FilesystemTool.Command.write("data.json", resp.body()));
            return "status=" + resp.status();
        };

        try (CatalystRuntime runtime = Catalyst.builder().log(GumboEventLog.at(logDir)).build()) {
            ExecutionHandle<String> handle = runtime.execute(fetchAndSave);
            String result = handle.result();
            ExecutionId id = handle.id();
            String saved = Files.readString(sandbox.resolve("data.json"));
            System.out.println("[tools] recorded execution " + id.value() + " -> " + result);
            System.out.println("[tools] HTTP calls during record: " + httpCalls[0]
                    + "; file data.json written: " + saved);

            // Externally delete the written file — if replay re-applied the write, it would reappear.
            Files.delete(sandbox.resolve("data.json"));

            ExecutionState replayed = runtime.replay(id, fetchAndSave);
            int externalHttp = httpCalls[0] - 1;
            boolean writeReapplied = Files.exists(sandbox.resolve("data.json"));
            System.out.println("[tools] replayed -> " + replayed.status()
                    + "; HTTP calls during replay: " + externalHttp
                    + "; file write re-applied on replay: " + writeReapplied);

            if (externalHttp != 0) throw new AssertionError("replay re-issued the HTTP request");
            if (writeReapplied) throw new AssertionError("replay re-applied the file write");
            System.out.println("[tools] built-in-tools criterion holds: HTTP + Filesystem calls recorded"
                    + " once and substituted on replay (zero re-execution).");
        }
    }

    /**
     * The v0.2 schema-evolution exit demo (roadmap — spec §13.4): a log recorded under an <em>older</em>
     * schema — an event type that has since been renamed, plus a field that has since been renamed — is
     * read through the current codec with {@code EventUpcaster}s and folds correctly, proving that events
     * written before a schema change still replay after it. Additive changes (an unknown field a newer
     * writer added) ride the tolerant reader with no upcaster at all.
     */
    private static void schemaDemo() {
        // Bytes as they would sit in an old log: "TaskCompleted" was later renamed to "ExecutionCompleted",
        // "ExecutionFailed.message" to "error", and a stray field a newer writer would add is present.
        byte[][] legacyLog = {
            bytes("{\"@type\":\"ExecutionCreated\",\"at\":\"2026-01-01T00:00:00Z\",\"taskType\":\"Demo\","
                    + "\"argsHash\":\"h\",\"configFingerprint\":\"cfg\",\"idempotencyKey\":\"k\"}"),
            bytes("{\"@type\":\"ExecutionStarted\",\"at\":\"2026-01-01T00:00:01Z\",\"attempt\":1,"
                    + "\"nodeId\":\"node-0\",\"futureFieldAddedLater\":true}"), // unknown field → tolerated
            bytes("{\"@type\":\"TaskCompleted\",\"at\":\"2026-01-01T00:00:02Z\",\"result\":42}"), // renamed type
        };

        com.cajunsystems.catalyst.events.EventCodec codec =
                com.cajunsystems.catalyst.events.EventCodec.builder()
                        .upcaster(com.cajunsystems.catalyst.events.EventUpcaster.renameType("TaskCompleted", "ExecutionCompleted"))
                        .upcaster(com.cajunsystems.catalyst.events.EventUpcaster.renameField("ExecutionFailed", "message", "error"))
                        .build();

        java.util.List<com.cajunsystems.catalyst.events.SequencedEvent> events = new java.util.ArrayList<>();
        for (int i = 0; i < legacyLog.length; i++) {
            events.add(new com.cajunsystems.catalyst.events.SequencedEvent(i, codec.decode(legacyLog[i])));
        }

        ExecutionId id = ExecutionId.random();
        ExecutionState state = com.cajunsystems.catalyst.engine.Reducer.fold(id, events);

        System.out.println("[schema] decoded " + events.size() + " events recorded under an older schema");
        System.out.println("[schema] folded status: " + state.status()
                + "; result: " + (state.result() == null ? "null" : state.result()));
        if (state.status() != com.cajunsystems.catalyst.Status.COMPLETED) {
            throw new AssertionError("expected COMPLETED, got " + state.status());
        }
        if (state.result() == null || state.result().asInt() != 42) {
            throw new AssertionError("expected result 42, got " + state.result());
        }
        System.out.println("[schema] schema-evolution criterion holds: a legacy-schema log (renamed type +"
                + " field, plus an unknown field) reads and folds under the current schema.");
    }

    private static byte[] bytes(String json) {
        return json.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * The v0.2 blob-store exit demo (roadmap increment ⑤): a payload larger than the 64 KiB offload
     * threshold is stored out-of-line in a content-addressed blob store, with the durable event stream
     * carrying only a small reference. Inspect/replay rehydrate it transparently — the task never sees a
     * reference.
     */
    private static void blobDemo(Path dir) throws Exception {
        String bigDocument = "LOREM-".repeat(30_000); // ~180 KB, well over the 64 KiB threshold

        Task<Integer> task = ctx -> ctx.effect("fetch-document", () -> bigDocument).length();

        try (CatalystRuntime runtime = Catalyst.builder().log(GumboEventLog.at(dir)).build()) {
            ExecutionHandle<Integer> handle = runtime.execute(task);
            int length = handle.result();
            ExecutionId id = handle.id();

            Path blobsDir = dir.resolve("blobs");
            long blobFiles;
            try (var walk = Files.walk(blobsDir)) {
                blobFiles = walk.filter(Files::isRegularFile).count();
            }
            boolean inlinedInLog = logInlinesDocument(dir, "LOREM-LOREM-LOREM-");

            System.out.println("[blob] recorded execution " + id.value() + " -> document length " + length);
            System.out.println("[blob] blob files stored out-of-line: " + blobFiles
                    + "; big payload inlined in the event log: " + inlinedInLog);

            ExecutionState replayed = runtime.replay(id, task);
            System.out.println("[blob] replayed -> " + replayed.status()
                    + " (the payload was rehydrated from the blob store transparently)");

            if (length != bigDocument.length()) throw new AssertionError("wrong document length");
            if (blobFiles < 1) throw new AssertionError("payload was not offloaded to the blob store");
            if (inlinedInLog) throw new AssertionError("payload was inlined in the event log");
            System.out.println("[blob] blob-store criterion holds: large payload stored out-of-line and"
                    + " rehydrated on replay.");
        }
    }

    /** True if a non-blob file under {@code dir} inlines {@code needle}. */
    private static boolean logInlinesDocument(Path dir, String needle) throws Exception {
        Path blobs = dir.resolve("blobs");
        try (var walk = Files.walk(dir)) {
            for (Path p : (Iterable<Path>) walk.filter(Files::isRegularFile)::iterator) {
                if (p.startsWith(blobs)) continue;
                if (new String(Files.readAllBytes(p), java.nio.charset.StandardCharsets.UTF_8).contains(needle)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** An item in the collections demo — a record carried inside a List payload. */
    record Item(String sku, int qty) {}

    /**
     * The v0.2 generic-collection payloads exit demo (roadmap increment ④): a task captures a
     * {@code List} of records through {@code ctx.effect(...)}. The list is recorded with element-type
     * fidelity and substituted on replay — the supplier does not run again, and the reconstructed list
     * still holds {@code Item} records (if they had decoded as maps, the typed stream would throw).
     */
    private static void collectionsDemo(Path dir) {
        int[] supplierCalls = {0};
        Task<Integer> task = ctx -> {
            java.util.List<Item> cart = ctx.effect("load-cart", () -> {
                supplierCalls[0]++;
                return java.util.List.of(new Item("A", 2), new Item("B", 3));
            });
            return cart.stream().mapToInt(Item::qty).sum(); // ClassCastException if fidelity is lost
        };

        try (CatalystRuntime runtime = Catalyst.builder().log(GumboEventLog.at(dir)).build()) {
            ExecutionHandle<Integer> handle = runtime.execute(task);
            int total = handle.result();
            ExecutionId id = handle.id();
            System.out.println("[collections] recorded execution " + id.value() + " -> total " + total
                    + " (supplier calls: " + supplierCalls[0] + ")");

            ExecutionState replayed = runtime.replay(id, task);
            int externalSupplierCalls = supplierCalls[0] - 1;
            System.out.println("[collections] replayed -> " + replayed.status()
                    + "; supplier calls during replay: " + externalSupplierCalls
                    + " (the List<Item> was substituted from the log with element types intact)");

            if (total != 5) throw new AssertionError("expected total 5, got " + total);
            if (externalSupplierCalls != 0) throw new AssertionError("replay re-ran the effect supplier");
            System.out.println("[collections] generic-collection criterion holds: List<record> round-trips"
                    + " and is substituted on replay with element-type fidelity.");
        }
    }

    /** A content-based model: "summarize" → SUMMARY, otherwise the given final text (with token usage). */
    private static MockModel answerModel(String finalText) {
        return MockModel.builder().respond(req -> {
            String last = req.prompt().messages().stream()
                    .filter(m -> m.role() == com.cajunsystems.catalyst.model.Role.USER)
                    .reduce((a, b) -> b).map(Message::content).orElse("");
            String text = last.contains("summarize") ? "SUMMARY" : finalText;
            long promptTokens = req.prompt().messages().stream()
                    .mapToLong(m -> Math.max(1, m.content().length() / 4)).sum();
            var usage = new com.cajunsystems.catalyst.model.Usage(promptTokens, Math.max(1, text.length() / 4));
            return new Completion(text, java.util.List.of(), usage, "stop");
        }).build();
    }

    /**
     * A deterministic, content-based mock model: it answers from the last user message rather than a
     * positional script, so it returns the same answer for the same request in any process — exactly
     * the property replay relies on.
     */
    private static MockModel summarizerModel() {
        return answerModel("FINAL");
    }

    private Demo() {}
}
