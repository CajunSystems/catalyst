package com.cajunsystems.catalyst.engine;

import com.cajunsystems.catalyst.model.CompletionRequest;
import com.cajunsystems.catalyst.model.Message;
import com.cajunsystems.catalyst.model.ToolSpec;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.StringJoiner;

/**
 * Canonical hashing for replay (spec §6). Hashes a <em>canonicalized</em> completion request —
 * messages and tool specs only, never model name, temperature defaults, or timestamps — so benign
 * config drift does not shatter replay. Full requests are stored separately for forensics.
 *
 * <p>M0 records these hashes on every boundary; strict mismatch detection is wired in M1.
 */
public final class Hashing {

    private Hashing() {}

    /** A stable hash of the canonical form of a completion request. */
    public static String canonicalRequestHash(CompletionRequest request) {
        StringJoiner sj = new StringJoiner("\n");
        for (Message m : request.prompt().messages()) {
            sj.add(m.role().name() + ":" + m.content());
        }
        sj.add("--tools--");
        for (ToolSpec t : request.tools()) {
            sj.add(t.name() + ":" + (t.inputSchema() == null ? "" : t.inputSchema()));
        }
        return sha256(sj.toString());
    }

    /**
     * A stable hash of a JSON payload (e.g. a tool's canonical input). Uses the node's compact JSON
     * rendering; callers on both the record and replay sides serialize with the same mapper, so the
     * rendering — and therefore the hash — matches.
     */
    public static String canonicalJsonHash(com.fasterxml.jackson.databind.JsonNode node) {
        return sha256(node == null ? "null" : node.toString());
    }

    /** A stable hash of an arbitrary string payload (e.g. a canonical prompt rendering). */
    public static String sha256(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(64);
            for (byte b : digest) sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
