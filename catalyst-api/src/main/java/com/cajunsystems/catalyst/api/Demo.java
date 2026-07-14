package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionInfo;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.CostModel;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.InDoubtPolicy;
import com.cajunsystems.catalyst.engine.PayloadCodec;
import com.cajunsystems.catalyst.engine.ReplayingContext;
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
import java.time.Instant;
import java.util.Map;

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
