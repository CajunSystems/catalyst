package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.engine.ExecutionState;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.cajunsystems.catalyst.tools.FilesystemTool;
import com.cajunsystems.catalyst.tools.HttpTool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The built-in HTTP + Filesystem tools (roadmap increment ③) as recorded boundaries: a task that
 * fetches over HTTP and writes the response to the sandboxed filesystem records both calls, and a
 * strict replay substitutes them — <strong>the HTTP request is not re-issued and the file write is
 * not re-applied</strong>. This is the durability guarantee that makes the tools safe to use inside a
 * resumable/replayable execution.
 */
class ToolsAcceptanceTest {

    @Test
    void httpAndFilesystemToolCallsAreRecordedAndSubstitutedOnReplay(@TempDir Path base) throws Exception {
        Path logDir = Files.createDirectory(base.resolve("log"));
        Path sandbox = Files.createDirectory(base.resolve("sandbox"));

        AtomicInteger httpCalls = new AtomicInteger();
        HttpTool http = new HttpTool(req -> {
            httpCalls.incrementAndGet();
            return new HttpTool.Response(200, "{\"answer\":42}", "application/json");
        });
        FilesystemTool fs = new FilesystemTool(sandbox);

        Task<String> fetchAndSave = ctx -> {
            HttpTool.Response resp = ctx.call(http, HttpTool.Request.get("https://api.example/data"));
            ctx.call(fs, FilesystemTool.Command.write("data.json", resp.body()));
            return "status=" + resp.status();
        };

        try (CatalystRuntime runtime = Catalyst.builder().log(GumboEventLog.at(logDir)).build()) {
            // ── Record ──
            var handle = runtime.execute(fetchAndSave);
            assertThat(handle.result()).isEqualTo("status=200");
            ExecutionId id = handle.id();
            assertThat(runtime.inspect(id).status()).isEqualTo(Status.COMPLETED);
            assertThat(httpCalls.get()).isEqualTo(1);
            assertThat(Files.readString(sandbox.resolve("data.json"))).isEqualTo("{\"answer\":42}");

            // Externally delete the written file: if replay re-applied the write, it would reappear.
            Files.delete(sandbox.resolve("data.json"));

            // ── Replay ──
            ExecutionState replayed = runtime.replay(id, fetchAndSave);
            assertThat(replayed.status()).isEqualTo(Status.COMPLETED);
            assertThat(httpCalls.get()).isEqualTo(1);                 // HTTP not re-issued
            assertThat(Files.exists(sandbox.resolve("data.json"))).isFalse(); // write not re-applied
        }
    }
}
