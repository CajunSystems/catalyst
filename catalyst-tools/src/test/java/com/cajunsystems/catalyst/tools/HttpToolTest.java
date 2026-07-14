package com.cajunsystems.catalyst.tools;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HttpToolTest {

    @Test
    void sendsThroughTheInjectedSenderAndReturnsTheResponse() throws Exception {
        AtomicReference<HttpTool.Request> seen = new AtomicReference<>();
        HttpTool http = new HttpTool(req -> {
            seen.set(req);
            return new HttpTool.Response(200, "{\"ok\":true}", "application/json");
        });

        HttpTool.Response response = http.apply(HttpTool.Request.get("https://example.com/api"));

        assertThat(response.status()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("{\"ok\":true}");
        assertThat(response.contentType()).isEqualTo("application/json");
        assertThat(seen.get().method()).isEqualTo("GET");
        assertThat(seen.get().url()).isEqualTo("https://example.com/api");
    }

    @Test
    void normalizesABlankOrLowercaseMethodToUppercaseGetDefault() throws Exception {
        AtomicReference<HttpTool.Request> seen = new AtomicReference<>();
        HttpTool http = new HttpTool(req -> { seen.set(req); return new HttpTool.Response(204, "", ""); });

        http.apply(new HttpTool.Request(null, "https://example.com", null, null));
        assertThat(seen.get().method()).isEqualTo("GET");

        http.apply(new HttpTool.Request("post", "https://example.com", "{}", "application/json"));
        assertThat(seen.get().method()).isEqualTo("POST");
    }

    @Test
    void rejectsUnsupportedMethodsSchemesAndBlankUrls() {
        HttpTool http = new HttpTool(req -> new HttpTool.Response(200, "", ""));

        assertThatThrownBy(() -> http.apply(new HttpTool.Request("TRACE", "https://example.com", null, null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("method");
        assertThatThrownBy(() -> http.apply(HttpTool.Request.get("file:///etc/passwd")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("http/https");
        assertThatThrownBy(() -> http.apply(HttpTool.Request.get("  ")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("url");
    }

    @Test
    void defaultPolicyBlocksLoopbackPrivateAndMetadataTargets() {
        HttpTool http = new HttpTool(); // default sender, block-private-networks policy

        // The policy rejects before any connection is attempted, so these run offline.
        assertThatThrownBy(() -> http.apply(HttpTool.Request.get("http://127.0.0.1:1/x")))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("non-public");
        assertThatThrownBy(() -> http.apply(HttpTool.Request.get("http://169.254.169.254/latest/meta-data/")))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("non-public");
        assertThatThrownBy(() -> http.apply(HttpTool.Request.get("http://localhost:1/x")))
                .isInstanceOf(java.io.IOException.class);
    }

    @Test
    void aCustomPolicyCanPermitAnAddress() {
        // allowAll would reach the network; instead assert the seam is honored by a policy that throws
        // a distinctive error for every target, proving apply() routes through the configured policy.
        HttpTool http = new HttpTool(java.time.Duration.ofSeconds(1),
                (uri, address) -> { throw new java.io.IOException("policy: " + uri.getHost()); });
        assertThatThrownBy(() -> http.apply(HttpTool.Request.get("http://127.0.0.1:1/x")))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("policy: 127.0.0.1");
    }

    @Test
    void surfacesSenderFailures() {
        HttpTool http = new HttpTool(req -> { throw new java.io.IOException("connection refused"); });
        assertThatThrownBy(() -> http.apply(HttpTool.Request.get("http://localhost:1/x")))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("connection refused");
    }
}
