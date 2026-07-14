package com.cajunsystems.catalyst.tools;

import com.cajunsystems.catalyst.Tool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Locale;
import java.util.Optional;
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
 * <p><strong>Target policy (SSRF).</strong> The default sender is <em>safe by default</em>: it
 * resolves the host and refuses loopback, link-local (incl. the {@code 169.254.0.0/16} cloud-metadata
 * range), private, and other non-public addresses via {@link TargetPolicy#blockPrivateNetworks()}. It
 * follows redirects manually so every hop is re-validated against the same policy. Supply
 * {@link TargetPolicy#allowAll()} or a custom policy to reach internal hosts deliberately. A custom
 * {@link Sender} bypasses the built-in policy entirely — the caller owns egress control there.
 *
 * <p>Payload note: {@link Response} carries the status, body, and {@code Content-Type} rather than a
 * full header map, because generic-collection payloads are a later increment (roadmap ④). Large bodies
 * are inlined until the blob store lands (roadmap ⑤). Bodies are decoded as UTF-8.
 */
public final class HttpTool implements Tool<HttpTool.Request, HttpTool.Response> {

    private static final Set<String> ALLOWED_METHODS =
            Set.of("GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS");
    private static final int MAX_REDIRECTS = 5;

    /** Reaches the network. The default wraps {@link java.net.http.HttpClient}; tests supply a fake. */
    @FunctionalInterface
    public interface Sender {
        Response send(Request request) throws Exception;
    }

    /**
     * Decides whether a resolved request target may be contacted — the SSRF guard for the default
     * sender. {@code verify} is called for <em>every</em> address a host resolves to, and again for
     * each redirect hop.
     */
    @FunctionalInterface
    public interface TargetPolicy {

        /** @throws IOException if {@code address} (for {@code uri}) must not be contacted. */
        void verify(URI uri, InetAddress address) throws IOException;

        /** Permits any target. Use only when egress is already constrained by the environment. */
        static TargetPolicy allowAll() {
            return (uri, address) -> { };
        }

        /**
         * Blocks loopback, link-local (incl. {@code 169.254.0.0/16} / {@code fe80::/10}), site-local /
         * private ({@code 10/8}, {@code 172.16/12}, {@code 192.168/16}), wildcard, multicast, and
         * IPv6 unique-local ({@code fc00::/7}) addresses. This is the default. (It resolves at request
         * time; a host that re-resolves to a private address after the check — DNS rebinding — is not
         * covered, which is why redirects are re-validated but time-of-use pinning is left to callers.)
         */
        static TargetPolicy blockPrivateNetworks() {
            return (uri, address) -> {
                if (address.isLoopbackAddress() || address.isLinkLocalAddress()
                        || address.isSiteLocalAddress() || address.isAnyLocalAddress()
                        || address.isMulticastAddress() || isUniqueLocalIpv6(address)) {
                    throw new IOException("Blocked request to non-public address "
                            + address.getHostAddress() + " for " + uri + ". The default HttpTool policy"
                            + " refuses loopback/link-local/private/metadata targets; construct HttpTool"
                            + " with TargetPolicy.allowAll() or a custom policy to permit it.");
                }
            };
        }
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

    /** Uses the default {@link java.net.http.HttpClient} sender (30s timeout, block-private-networks policy). */
    public HttpTool() {
        this(Duration.ofSeconds(30), TargetPolicy.blockPrivateNetworks());
    }

    /** Uses the default sender with a custom timeout and the block-private-networks policy. */
    public HttpTool(Duration timeout) {
        this(timeout, TargetPolicy.blockPrivateNetworks());
    }

    /** Uses the default sender with a custom timeout and target policy. */
    public HttpTool(Duration timeout, TargetPolicy policy) {
        this(defaultSender(timeout, requireNonNull(policy, "policy")));
    }

    /** Uses a caller-supplied sender — the seam tests use to run offline. Bypasses the built-in policy. */
    public HttpTool(Sender sender) {
        this.sender = requireNonNull(sender, "sender");
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
        requireHttpScheme(URI.create(input.url()));
        // Hand the sender a request with the method normalized so custom senders see a canonical value.
        return sender.send(new Request(method, input.url(), input.body(), input.contentType()));
    }

    private static Sender defaultSender(Duration timeout, TargetPolicy policy) {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(timeout)
                .followRedirects(HttpClient.Redirect.NEVER) // follow manually so each hop is policy-checked
                .build();
        return request -> sendFollowingRedirects(client, request, timeout, policy);
    }

    private static Response sendFollowingRedirects(HttpClient client, Request request, Duration timeout,
                                                   TargetPolicy policy) throws Exception {
        String url = request.url();
        String method = request.method();
        String body = request.body();
        String contentType = request.contentType();

        for (int hop = 0; ; hop++) {
            URI uri = URI.create(url);
            requireHttpScheme(uri);
            verifyTarget(uri, policy);

            HttpRequest.BodyPublisher publisher = (body == null || body.isEmpty())
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(body);
            HttpRequest.Builder builder = HttpRequest.newBuilder(uri).timeout(timeout);
            if (contentType != null && !contentType.isBlank()) {
                builder.header("Content-Type", contentType);
            }
            builder.method(method, publisher);

            HttpResponse<String> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            int status = response.statusCode();
            Optional<String> location = response.headers().firstValue("location");
            if (isRedirect(status) && location.isPresent() && hop < MAX_REDIRECTS) {
                url = uri.resolve(location.get()).toString();
                // 303 (and, per prevailing browser behavior, 301/302) drop to GET without a body; 307/308
                // preserve the method and body.
                if (status == 301 || status == 302 || status == 303) {
                    method = "GET";
                    body = null;
                    contentType = null;
                }
                continue;
            }
            String responseContentType = response.headers().firstValue("content-type").orElse("");
            return new Response(status, response.body(), responseContentType);
        }
    }

    private static void verifyTarget(URI uri, TargetPolicy policy) throws IOException {
        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw new IOException("URL has no host: " + uri);
        }
        InetAddress[] addresses;
        try {
            addresses = InetAddress.getAllByName(host);
        } catch (UnknownHostException e) {
            throw new IOException("Cannot resolve host '" + host + "' for " + uri, e);
        }
        for (InetAddress address : addresses) {
            policy.verify(uri, address);
        }
    }

    private static void requireHttpScheme(URI uri) {
        String scheme = uri.getScheme();
        if (scheme == null || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
            throw new IllegalArgumentException("Only http/https URLs are supported: " + uri);
        }
    }

    private static boolean isRedirect(int status) {
        return status == 301 || status == 302 || status == 303 || status == 307 || status == 308;
    }

    private static boolean isUniqueLocalIpv6(InetAddress address) {
        byte[] bytes = address.getAddress();
        return bytes.length == 16 && (bytes[0] & 0xfe) == 0xfc; // fc00::/7
    }

    private static <T> T requireNonNull(T value, String name) {
        if (value == null) throw new IllegalArgumentException(name + " must not be null");
        return value;
    }
}
