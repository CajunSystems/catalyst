package com.cajunsystems.catalyst.events;

import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/** A simple in-memory {@link BlobStore}, content-addressed by SHA-256. */
public final class InMemoryBlobStore implements BlobStore {

    private final ConcurrentHashMap<String, byte[]> blobs = new ConcurrentHashMap<>();

    @Override
    public String put(byte[] content) {
        String ref = Blobs.ref(content);
        blobs.putIfAbsent(ref, content.clone());
        return ref;
    }

    @Override
    public byte[] get(String ref) {
        byte[] content = blobs.get(ref);
        if (content == null) throw new NoSuchElementException("No blob for reference: " + ref);
        return content.clone();
    }

    @Override
    public boolean has(String ref) {
        return blobs.containsKey(ref);
    }
}
