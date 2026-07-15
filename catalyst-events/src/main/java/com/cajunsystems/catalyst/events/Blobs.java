package com.cajunsystems.catalyst.events;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Content-addressing helpers shared by {@link BlobStore} implementations. */
final class Blobs {

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private Blobs() {}

    /** The {@code "sha256:<hex>"} reference for {@code content}. */
    static String ref(byte[] content) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(content);
            StringBuilder sb = new StringBuilder(7 + digest.length * 2).append("sha256:");
            for (byte b : digest) {
                sb.append(HEX[(b >> 4) & 0xf]).append(HEX[b & 0xf]);
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
