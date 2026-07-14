package com.cajunsystems.catalyst.tools;

import com.cajunsystems.catalyst.Tool;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Locale;
import java.util.Set;

/**
 * An HTTP tool (spec §4): performs a single request and returns the response. Invoked through
 * {@code ctx.call(tool, request)} it is a recorded boundary — the response is stored once and
 * substituted on replay/resume, so a recovered execution never re-issues the request. Deliberately
 * <em>not</em> {@link com.cajunsystems.catalyst.Deterministic}: a response depends on the server and
 * the moment it ran.
 *
 * <p>The network is reached through a pluggable {@link Sender}; the default wraps
 * {@link java.net.http.HttpClient}. Tests inject a fake sender so they exercise recording and
 * substitution without egress.
 *
 * <p>Payload note: {@link Response} carries the status, body, and {@code Content-Type} rather than a
 * full header map, because generic-collection payloads are a later increment (roadmap ④). Large bodies
 * are inlined until the blob store lands (roadmap ⑤). Bodies are decoded as UTF-8.
 */
public final class HttpTool implements Tool<HttpTool.Request, HttpTool.Response> {

    private static final Set<String> ALLOWED_METHODS =
            Set.of("GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS");

    /** Reaches the network. The default wraps {@link java.net.http.HttpClient}; tests supply a fake. */
    @FunctionalInterface
    public interface Sender {
        Response send(Request request) throws Exception;
    }

    /**
     * An HTTP request. {@code method} defaults to {@code GET} when blank; {@code body} and
     * {@code contentType} are used for the methods that carry a body.
     */
    public record Request(String method, String url, String body, String contentType) {
        public static Request get(String url) { return new Request("GET", url, null, null); }
        public static Request post(String url, String body, String contentType) {
            return new Request("POST", url, body, contentType);
        }
    }

    /** An HTTP response: the status code, the body, and the response {@code Content-Type} (may be empty). */
    public record Response(int status, String body, String contentType) {}

    private final Sender sender;

    /** Uses the default {@link java.net.http.HttpClient}-backed sender with a 30s timeout. */
    public HttpTool() {
        this(defaultSender(Duration.ofSeconds(30)));
    }

    /** Uses the default sender with a custom connect/request timeout. */
    public HttpTool(Duration timeout) {
        this(defaultSender(timeout));
    }

    /** Uses a caller-supplied sender — the seam tests use to run offline. */
    public HttpTool(Sender sender) {
        if (sender == null) throw new IllegalArgumentException("sender must not be null");
        this.sender = sender;
    }

    @Override
    public String name() {
        return "http";
    }

    @Override
    public Class<Request> inputType() {
        return Request.class;
    }

    @Override
    public Response apply(Request input) throws Exception {
        if (input == null) throw new IllegalArgumentException("request must not be null");
        String method = (input.method() == null || input.method().isBlank())
                ? "GET" : input.method().toUpperCase(Locale.ROOT);
        if (!ALLOWED_METHODS.contains(method)) {
            throw new IllegalArgumentException("Unsupported HTTP method: " + input.method());
        }
        if (input.url() == null || input.url().isBlank()) {
            throw new IllegalArgumentException("url must not be blank");
        }
        URI uri = URI.create(input.url());
        String scheme = uri.getScheme();
        if (scheme == null || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
            throw new IllegalArgumentException("Only http/https URLs are supported: " + input.url());
        }
        // Hand the sender a request with the method normalized so custom senders see a canonical value.
        return sender.send(new Request(method, input.url(), input.body(), input.contentType()));
    }

    private static Sender defaultSender(Duration timeout) {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(timeout)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        return request -> {
            HttpRequest.BodyPublisher body = (request.body() == null || request.body().isEmpty())
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(request.body());
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(request.url())).timeout(timeout);
            if (request.contentType() != null && !request.contentType().isBlank()) {
                builder.header("Content-Type", request.contentType());
            }
            builder.method(request.method(), body);
            HttpResponse<String> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            String contentType = response.headers().firstValue("content-type").orElse("");
            return new Response(response.statusCode(), response.body(), contentType);
        };
    }
}
