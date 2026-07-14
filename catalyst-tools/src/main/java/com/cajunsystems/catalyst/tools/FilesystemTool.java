package com.cajunsystems.catalyst.tools;

import com.cajunsystems.catalyst.Tool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SecureDirectoryStream;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Set;

/**
 * A filesystem tool sandboxed to a single root directory (spec §4). Every access is resolved against
 * the root and rejected if it escapes it — via {@code ..}, an absolute path, or a symlink whose real
 * target lands outside — so a task (or a tampered log driving a replay) can only ever touch files
 * under the root it was given.
 *
 * <p>Deliberately <em>not</em> {@link com.cajunsystems.catalyst.Deterministic}: a read depends on the
 * state of the filesystem when it ran, and a write/delete is a side effect. Invoked through
 * {@code ctx.call(tool, cmd)} the result is recorded once and substituted on replay — so a resumed or
 * replayed execution never re-reads changed bytes and never re-applies a write it already performed.
 *
 * <p><strong>Symlink-safe traversal.</strong> Where the platform provides a
 * {@link SecureDirectoryStream} (e.g. Linux), the path is walked one component at a time relative to
 * the parent directory's open file descriptor, each hop opened {@link LinkOption#NOFOLLOW_LINKS} — the
 * {@code openat(O_NOFOLLOW)} discipline. No intermediate <em>or</em> final symlink is ever followed, so
 * a concurrent local writer cannot swap a checked directory for a symlink between the check and the
 * operation (the classic TOCTOU escape). On platforms without a {@code SecureDirectoryStream}, the tool
 * falls back to a normalized-path check plus {@code NOFOLLOW} on the final component, which closes the
 * final-component swap but not an intermediate-directory race.
 *
 * <p>Payload note: {@link Action#LIST} returns its entries as a newline-joined string (with the count
 * in {@code size}) because generic-collection payloads are a later increment (roadmap ④). Bodies are
 * UTF-8.
 */
public final class FilesystemTool implements Tool<FilesystemTool.Command, FilesystemTool.Result> {

    /** The operation to perform, over a path relative to the sandbox root. */
    public enum Action { READ, WRITE, LIST, EXISTS, DELETE }

    /**
     * A filesystem command. {@code path} is always relative to the sandbox root; {@code content} is
     * used only by {@link Action#WRITE}.
     */
    public record Command(Action action, String path, String content) {
        public static Command read(String path) { return new Command(Action.READ, path, null); }
        public static Command write(String path, String content) { return new Command(Action.WRITE, path, content); }
        public static Command list(String path) { return new Command(Action.LIST, path, null); }
        public static Command exists(String path) { return new Command(Action.EXISTS, path, null); }
        public static Command delete(String path) { return new Command(Action.DELETE, path, null); }
    }

    /**
     * The outcome of a command. {@code content} holds a read's bytes or a listing's newline-joined
     * entries; {@code size} is a read/write's byte count or a listing's entry count; {@code exists}
     * reflects the path after the operation; {@code ok} is the operation's success where that is
     * distinct from an exception (notably {@link Action#DELETE}).
     */
    public record Result(Action action, String path, boolean ok, String content, long size, boolean exists) {}

    private final Path root;

    /**
     * Sandboxes the tool to {@code root}, which must be an existing directory. The root is canonicalized
     * once (following any symlinks) so containment checks compare real paths.
     */
    public FilesystemTool(Path root) {
        if (root == null) throw new IllegalArgumentException("root must not be null");
        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException("Sandbox root must be an existing directory: " + root);
        }
        try {
            this.root = root.toRealPath();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to canonicalize sandbox root: " + root, e);
        }
    }

    /** The canonicalized sandbox root every path is resolved against. */
    public Path root() {
        return root;
    }

    @Override
    public String name() {
        return "filesystem";
    }

    @Override
    public Class<Command> inputType() {
        return Command.class;
    }

    @Override
    public Result apply(Command input) throws IOException {
        if (input == null || input.action() == null) {
            throw new IllegalArgumentException("A filesystem command with an action is required");
        }
        // Logical containment: reject .. / absolute / symlinked-ancestor escapes up front with a clear
        // message. The secure walk below is the actual enforcement (it never follows a symlink).
        Path target = resolveWithin(input.path());
        String rel = input.path();
        return switch (input.action()) {
            case READ -> read(target, rel);
            case WRITE -> write(target, rel, input.content());
            case LIST -> list(target, rel);
            case EXISTS -> exists(target, rel);
            case DELETE -> delete(target, rel);
        };
    }

    // ── Operations (symlink-safe via SecureDirectoryStream, with a NOFOLLOW-final fallback) ──────────

    private Result read(Path target, String rel) throws IOException {
        byte[] bytes = withParentDir(target, (parent, leaf) -> {
            try (SeekableByteChannel ch = parent.newByteChannel(leaf,
                    Set.of(StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS))) {
                return Channels.newInputStream(ch).readAllBytes();
            }
        }, () -> readFallback(target, rel));
        return new Result(Action.READ, rel, true, new String(bytes, StandardCharsets.UTF_8), bytes.length, true);
    }

    private Result write(Path target, String rel, String content) throws IOException {
        byte[] bytes = (content == null ? "" : content).getBytes(StandardCharsets.UTF_8);
        Path parentPath = target.getParent();
        if (parentPath != null) Files.createDirectories(parentPath);
        withParentDir(target, (parent, leaf) -> {
            try (SeekableByteChannel ch = parent.newByteChannel(leaf, Set.of(StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, LinkOption.NOFOLLOW_LINKS))) {
                ch.write(ByteBuffer.wrap(bytes));
            }
            return null;
        }, () -> writeFallback(target, bytes));
        return new Result(Action.WRITE, rel, true, null, bytes.length, true);
    }

    private Result delete(Path target, String rel) throws IOException {
        boolean deleted = withParentDir(target, (parent, leaf) -> {
            try {
                parent.deleteFile(leaf); // operates on the entry itself — never follows a symlink
                return true;
            } catch (NoSuchFileException e) {
                return false;
            }
        }, () -> Files.deleteIfExists(target));
        return new Result(Action.DELETE, rel, deleted, null, 0, false);
    }

    private Result exists(Path target, String rel) throws IOException {
        boolean present = withParentDir(target, (parent, leaf) -> {
            try {
                parent.getFileAttributeView(leaf, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS)
                        .readAttributes();
                return true;
            } catch (NoSuchFileException e) {
                return false;
            }
        }, () -> Files.exists(target, LinkOption.NOFOLLOW_LINKS));
        return new Result(Action.EXISTS, rel, present, null, 0, present);
    }

    private Result list(Path target, String rel) throws IOException {
        List<String> names = new ArrayList<>();
        try (SecureDirectoryStream<Path> dir = openDirSecurely(target)) {
            if (dir != null) {
                for (Path entry : dir) names.add(entry.getFileName().toString());
            } else {
                listFallback(target, rel, names);
            }
        }
        names.sort(null);
        return new Result(Action.LIST, rel, true, String.join("\n", names), names.size(), true);
    }

    // ── Secure traversal ─────────────────────────────────────────────────────────────────────────

    /** An operation performed against the leaf's parent directory FD. */
    @FunctionalInterface
    private interface LeafAction<T> {
        T run(SecureDirectoryStream<Path> parentDir, Path leafName) throws IOException;
    }

    /** A fallback performed with plain path APIs where {@link SecureDirectoryStream} is unavailable. */
    @FunctionalInterface
    private interface Fallback<T> {
        T run() throws IOException;
    }

    /**
     * Opens a chain of {@link SecureDirectoryStream}s from the root to {@code target}'s parent, each hop
     * {@code NOFOLLOW}, then runs {@code action} against the parent-dir FD and the single-component leaf
     * name. If the platform has no {@code SecureDirectoryStream}, runs {@code fallback} instead.
     */
    private <T> T withParentDir(Path target, LeafAction<T> action, Fallback<T> fallback)
            throws IOException {
        if (target.equals(root)) {
            throw new IOException("Operation requires a path inside the sandbox root, not the root itself");
        }
        Path relative = root.relativize(target);
        int n = relative.getNameCount();
        Deque<SecureDirectoryStream<Path>> chain = new ArrayDeque<>();
        try {
            SecureDirectoryStream<Path> cursor = openRootSecurely();
            if (cursor == null) return fallback.run();
            chain.push(cursor);
            for (int i = 0; i < n - 1; i++) {
                cursor = cursor.newDirectoryStream(relative.getName(i), LinkOption.NOFOLLOW_LINKS);
                chain.push(cursor);
            }
            return action.run(cursor, relative.getName(n - 1));
        } finally {
            closeQuietly(chain);
        }
    }

    /** Opens {@code target} itself as a secure directory stream (NOFOLLOW at every hop), or null if unsupported. */
    private SecureDirectoryStream<Path> openDirSecurely(Path target) throws IOException {
        SecureDirectoryStream<Path> rootStream = openRootSecurely();
        if (rootStream == null) return null;
        if (target.equals(root)) return rootStream; // listing the sandbox root itself
        Path relative = root.relativize(target);
        Deque<SecureDirectoryStream<Path>> chain = new ArrayDeque<>();
        chain.push(rootStream);
        boolean handOff = false;
        try {
            SecureDirectoryStream<Path> cursor = rootStream;
            for (int i = 0; i < relative.getNameCount(); i++) {
                cursor = cursor.newDirectoryStream(relative.getName(i), LinkOption.NOFOLLOW_LINKS);
                chain.push(cursor);
            }
            handOff = true;
            return cursor; // caller closes it; ancestors are closed below
        } finally {
            // Close every intermediate stream; keep the returned leaf open on the success path.
            if (handOff) chain.pop();
            closeQuietly(chain);
        }
    }

    private SecureDirectoryStream<Path> openRootSecurely() throws IOException {
        DirectoryStream<Path> stream = Files.newDirectoryStream(root);
        if (stream instanceof SecureDirectoryStream<Path> secure) {
            return secure;
        }
        stream.close();
        return null; // platform does not support secure directory streams
    }

    private static void closeQuietly(Deque<SecureDirectoryStream<Path>> chain) {
        while (!chain.isEmpty()) {
            try {
                chain.pop().close();
            } catch (IOException ignored) {
                // best-effort close of a directory handle
            }
        }
    }

    // ── Fallbacks (no SecureDirectoryStream: NOFOLLOW on the final component only) ────────────────────

    private byte[] readFallback(Path target, String rel) throws IOException {
        if (!Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Not a regular file: " + rel);
        }
        try (var in = Files.newInputStream(target, LinkOption.NOFOLLOW_LINKS)) {
            return in.readAllBytes();
        }
    }

    private Void writeFallback(Path target, byte[] bytes) throws IOException {
        try (var out = Files.newOutputStream(target, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, LinkOption.NOFOLLOW_LINKS)) {
            out.write(bytes);
        }
        return null;
    }

    private void listFallback(Path target, String rel, List<String> into) throws IOException {
        if (Files.isSymbolicLink(target) || !Files.isDirectory(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Not a directory: " + rel);
        }
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(target)) {
            for (Path p : entries) into.add(p.getFileName().toString());
        }
    }

    /**
     * Logical pre-check: the normalized path must stay under the root (defeats {@code ..} and absolute
     * paths), and the nearest existing ancestor's <em>real</em> path must too (a fast, friendly rejection
     * of an existing symlink escape; the secure walk is the authoritative enforcement).
     */
    private Path resolveWithin(String rawPath) {
        if (rawPath == null || rawPath.isBlank()) {
            throw new IllegalArgumentException("path must not be blank");
        }
        Path target = root.resolve(rawPath).normalize();
        if (!target.startsWith(root)) {
            throw new IllegalArgumentException("path escapes the sandbox root '" + root + "': " + rawPath);
        }
        Path existing = target;
        while (existing != null && !Files.exists(existing, LinkOption.NOFOLLOW_LINKS)) {
            existing = existing.getParent();
        }
        if (existing != null) {
            try {
                if (!existing.toRealPath().startsWith(root)) {
                    throw new IllegalArgumentException("path escapes the sandbox root via a symlink: " + rawPath);
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to canonicalize path: " + rawPath, e);
            }
        }
        return target;
    }
}
