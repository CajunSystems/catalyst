package com.cajunsystems.catalyst.api;

import com.cajunsystems.catalyst.gumbo.GumboEventLog;
import com.cajunsystems.catalyst.mock.MockModel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Dist-2 on the durable log. The file-backed Gumbo log fences but is single-writer by construction,
 * since it takes an exclusive directory lock. So a node that declares {@code distributed()} on it is
 * refused at startup, never left to find out at the second writer. The FoundationDB side, where the
 * guard passes, is {@link FoundationDBAcceptanceTest}.
 */
class DistributionGuardAcceptanceTest {

    @Test
    void aFileBackedLogIsRefusedForDistributionAndItsDirectoryIsReleased(@TempDir Path dir) {
        assertThatThrownBy(() -> Catalyst.builder(dir).model(MockModel.alwaysReturn("x"))
                .nodeId("node-a").distributed().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("supportsConditionalAppend()=true")
                .hasMessageContaining("supportsMultiWriter()=false")
                .hasMessageContaining("GumboEventLog.foundationDB");

        // The builder opened the log, so the refusal released it. A held directory lock would make
        // this reopen fail with "already open".
        try (GumboEventLog reopened = GumboEventLog.at(dir)) {
            assertThat(reopened.supportsMultiWriter()).isFalse();
        }
    }
}
