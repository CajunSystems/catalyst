package com.cajunsystems.catalyst.events;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.NoSuchElementException;

/**
 * A durable, content-addressed {@link BlobStore} that writes each blob as a file named by its SHA-256
 * under a root directory. Writes are atomic (temp file + atomic move) and idempotent — re-storing the
 * same bytes is a no-op — so a crash mid-write never leaves a partial, wrongly-named blob.
 */
public final class FileBlobStore implements BlobStore {

    private final Path root;

    private FileBlobStore(Path root) {
        this.root = root;
    }

    /** A blob store rooted at {@code dir} (created on first write). */
    public static FileBlobStore at(Path dir) {
        return new FileBlobStore(dir.toAbsolutePath().normalize());
    }

    @Override
    public String put(byte[] content) {
        String ref = Blobs.ref(content);
        Path target = fileFor(ref);
        if (Files.exists(target)) return ref; // content-addressed: identical bytes already stored
        try {
            Path shard = target.getParent();
            Files.createDirectories(shard); // root + shard dir
            // Temp file lives in the shard dir so the atomic move never crosses directories (some NFS/
            // CIFS mounts reject cross-directory ATOMIC_MOVE).
            Path tmp = Files.createTempFile(shard, "blob-", ".tmp");
            try {
                Files.write(tmp, content);
                try {
                    Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
                } catch (java.nio.file.FileAlreadyExistsException raced) {
                    Files.deleteIfExists(tmp); // another writer stored the same content first — fine
                }
            } catch (IOException e) {
                Files.deleteIfExists(tmp);
                throw e;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write blob " + ref, e);
        }
        return ref;
    }

    @Override
    public byte[] get(String ref) {
        Path target = fileFor(ref);
        try {
            return Files.readAllBytes(target);
        } catch (java.nio.file.NoSuchFileException e) {
            throw new NoSuchElementException("No blob for reference: " + ref);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read blob " + ref, e);
        }
    }

    @Override
    public boolean has(String ref) {
        return Files.exists(fileFor(ref));
    }

    private Path fileFor(String ref) {
        // "sha256:abcd..." → shard by the first two hex chars to avoid one huge directory.
        int colon = ref.indexOf(':');
        String hex = colon >= 0 ? ref.substring(colon + 1) : ref;
        if (hex.length() < 2 || !hex.chars().allMatch(c -> Character.digit(c, 16) >= 0)) {
            throw new IllegalArgumentException("Not a valid blob reference: " + ref);
        }
        return root.resolve(hex.substring(0, 2)).resolve(hex);
    }
}
