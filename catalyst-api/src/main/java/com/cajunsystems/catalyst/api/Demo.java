package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionInfo;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.engine.InDoubtPolicy;
import com.cajunsystems.catalyst.engine.PayloadCodec;
import com.cajunsystems.catalyst.engine.ReplayingContext;
import com.cajunsystems.catalyst.events.CatalystEvent;
import com.cajunsystems.catalyst.events.EventJson;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.model.Completion;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Message;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
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

    public static void main(String[] args) throws Exception {
        if (args.length >= 2 && args[1].equals("record")) {
            record(Path.of(args[0]));
            System.out.println("[record] step 1 durably recorded; simulating kill -9 (exit 137)...");
            Runtime.getRuntime().halt(137); // abrupt death: no completion event, no graceful close
        } else if (args.length >= 2 && args[1].equals("resume")) {
            resume(Path.of(args[0]));
        } else {
            Path dir = Files.createTempDirectory("catalyst-demo-");
            System.out.println("No args: running record + resume in-process under " + dir);
            record(dir);
            resume(dir);
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
                    EventJson.shared(), new PayloadCodec(), InDoubtPolicy.FAIL, Clock.systemUTC(),
                    LoggerFactory.getLogger("demo.record"), log.read(id), true);
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
     * A deterministic, content-based mock model: it answers from the last user message rather than a
     * positional script, so it returns the same answer for the same request in any process — exactly
     * the property replay relies on.
     */
    private static MockModel summarizerModel() {
        return MockModel.builder().respond(req -> {
            String last = req.prompt().messages().stream()
                    .filter(m -> m.role() == com.cajunsystems.catalyst.model.Role.USER)
                    .reduce((a, b) -> b).map(Message::content).orElse("");
            return Completion.ofText(last.contains("summarize") ? "SUMMARY" : "FINAL");
        }).build();
    }

    private Demo() {}
}
