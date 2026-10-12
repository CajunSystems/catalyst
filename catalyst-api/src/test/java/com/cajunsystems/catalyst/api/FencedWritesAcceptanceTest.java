package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.Context;
import com.cajunsystems.catalyst.ExecutionId;
import com.cajunsystems.catalyst.ExecutionOptions;
import com.cajunsystems.catalyst.Status;
import com.cajunsystems.catalyst.Task;
import com.cajunsystems.catalyst.events.SequencedEvent;
import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.log.StaleWriterException;
import com.cajunsystems.catalyst.mock.MockModel;
import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Prompt;
import com.cajunsystems.catalyst.runtime.CatalystRuntime;
import com.cajunsystems.catalyst.runtime.ExecutionHandle;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Dist-1 on the durable log: the fence is Gumbo's own, storage-side. Two runtimes share one Gumbo log,
 * the first stalls mid-execution, the second takes the execution over and finishes it, and the first's
 * late write is refused by storage — leaving the second's history exactly as it wrote it.
 */
class FencedWritesAcceptanceTest {

    private static final CompletionRequest STEP1 = CompletionRequest.of(Prompt.builder().user("step one").build());
    private static final CompletionRequest STEP2 = CompletionRequest.of(Prompt.builder().user("step two").build());

    static final class GatedTask implements Task<String> {
        private final CountDownLatch reachedGate;
        private final CountDownLatch openGate;

        GatedTask(CountDownLatch reachedGate, CountDownLatch openGate) {
            this.reachedGate = reachedGate;
            this.openGate = openGate;
        }

        GatedTask() {
            this(null, null);
        }

        @Override public String execute(Context ctx) throws Exception {
            String one = ctx.model().complete(STEP1).message();
            if (openGate != null) {
                reachedGate.countDown();
                openGate.await();
            }
            return one + "|" + ctx.model().complete(STEP2).message();
        }
    }

    @Test
    void aStaleWriterIsRefusedByGumbosFence() throws Exception {
        GumboEventLog log = GumboEventLog.inMemory();
        assertThat(log.supportsConditionalAppend()).as("Gumbo fences").isTrue();
        CountDownLatch reachedGate = new CountDownLatch(1);
        CountDownLatch openGate = new CountDownLatch(1);

        CatalystRuntime zombie = Catalyst.builder().log(log).model(MockModel.alwaysReturn("Z")).build();
        CatalystRuntime owner = Catalyst.builder().log(log).model(MockModel.alwaysReturn("B"))
                .task(GatedTask.class, GatedTask::new).build();
        try {
            ExecutionHandle<String> stalled = zombie.execute(new GatedTask(reachedGate, openGate),
                    ExecutionOptions.withKey("job-1"));
            assertThat(reachedGate.await(5, TimeUnit.SECONDS)).isTrue();
            ExecutionId id = stalled.id();

            assertThat(owner.resume(id).result()).isEqualTo("Z|B");
            List<SequencedEvent> ownersHistory = log.read(id);

            openGate.countDown();
            assertThatThrownBy(stalled::result).isInstanceOf(StaleWriterException.class);

            assertThat(log.read(id)).as("storage refused the late write").isEqualTo(ownersHistory);
            assertThat(owner.inspect(id).status()).isEqualTo(Status.COMPLETED);
            assertThat(log.openExecutions()).doesNotContain(id);
        } finally {
            openGate.countDown();
            owner.close();
            zombie.close(); // the shared log is already closed; Gumbo's close is idempotent
        }
    }
}
