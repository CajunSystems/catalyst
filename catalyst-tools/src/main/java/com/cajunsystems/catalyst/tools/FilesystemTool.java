package com.cajunsystems.catalyst.tools;

import com.cajunsystems.catalyst.Tool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.stream.Stream;

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
        Path target = resolveWithin(input.path());
        return switch (input.action()) {
            case READ -> read(target, input.path());
            case WRITE -> write(target, input.path(), input.content());
            case LIST -> list(target, input.path());
            case EXISTS -> new Result(Action.EXISTS, input.path(),
                    Files.exists(target), null, 0, Files.exists(target));
            case DELETE -> delete(target, input.path());
        };
    }

    private Result read(Path target, String rel) throws IOException {
        if (!Files.isRegularFile(target)) {
            throw new IOException("Not a regular file: " + rel);
        }
        String content = Files.readString(target, StandardCharsets.UTF_8);
        return new Result(Action.READ, rel, true, content,
                content.getBytes(StandardCharsets.UTF_8).length, true);
    }

    private Result write(Path target, String rel, String content) throws IOException {
        String body = content == null ? "" : content;
        Path parent = target.getParent();
        if (parent != null) Files.createDirectories(parent);
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        Files.write(target, bytes);
        return new Result(Action.WRITE, rel, true, null, bytes.length, true);
    }

    private Result list(Path target, String rel) throws IOException {
        if (!Files.isDirectory(target)) {
            throw new IOException("Not a directory: " + rel);
        }
        try (Stream<Path> entries = Files.list(target)) {
            var names = entries.map(p -> p.getFileName().toString()).sorted().toList();
            String joined = String.join("\n", names);
            return new Result(Action.LIST, rel, true, joined, names.size(), true);
        }
    }

    private Result delete(Path target, String rel) throws IOException {
        boolean deleted = Files.deleteIfExists(target);
        return new Result(Action.DELETE, rel, deleted, null, 0, false);
    }

    /**
     * Resolves {@code rawPath} against the root and refuses anything that escapes it. Guards two ways:
     * the normalized logical path must stay under the root (defeats {@code ..} and absolute paths), and
     * the nearest existing ancestor's <em>real</em> path must too (defeats a symlink pointing outside).
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
