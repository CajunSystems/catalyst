package com.cajunsystems.catalyst.events;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BlobStoreTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void inMemoryStoreIsContentAddressedAndDeduplicates() {
        BlobStore store = BlobStore.inMemory();
        String ref = store.put(bytes("hello world"));
        assertThat(ref).startsWith("sha256:");
        assertThat(store.put(bytes("hello world"))).isEqualTo(ref); // same content → same ref
        assertThat(new String(store.get(ref), StandardCharsets.UTF_8)).isEqualTo("hello world");
        assertThat(store.has(ref)).isTrue();
        assertThat(store.has("sha256:deadbeef")).isFalse();
    }

    @Test
    void fileStorePersistsAndReturnsStableRefs(@TempDir Path dir) {
        FileBlobStore store = FileBlobStore.at(dir.resolve("blobs"));
        byte[] payload = bytes("x".repeat(10_000));
        String ref = store.put(payload);

        assertThat(store.has(ref)).isTrue();
        assertThat(store.get(ref)).isEqualTo(payload);
        assertThat(store.put(payload)).isEqualTo(ref); // idempotent

        // A fresh store over the same dir still resolves the ref (durable).
        assertThat(FileBlobStore.at(dir.resolve("blobs")).get(ref)).isEqualTo(payload);
    }

    @Test
    void missingBlobThrows(@TempDir Path dir) {
        assertThatThrownBy(() -> BlobStore.inMemory().get("sha256:00"))
                .isInstanceOf(NoSuchElementException.class);
        assertThatThrownBy(() -> FileBlobStore.at(dir).get("sha256:abcdef"))
                .isInstanceOf(NoSuchElementException.class);
    }
}
