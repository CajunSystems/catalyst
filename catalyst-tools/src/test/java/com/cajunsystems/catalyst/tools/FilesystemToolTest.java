package com.cajunsystems.catalyst.tools;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class FilesystemToolTest {

    @Test
    void writeReadRoundTrip(@TempDir Path root) throws Exception {
        FilesystemTool fs = new FilesystemTool(root);
        Files.createDirectory(root.resolve("notes")); // WRITE does not auto-create parents

        FilesystemTool.Result written = fs.apply(FilesystemTool.Command.write("notes/a.txt", "hello"));
        assertThat(written.ok()).isTrue();
        assertThat(written.size()).isEqualTo(5);
        assertThat(Files.readString(root.resolve("notes/a.txt"))).isEqualTo("hello");

        FilesystemTool.Result read = fs.apply(FilesystemTool.Command.read("notes/a.txt"));
        assertThat(read.content()).isEqualTo("hello");
        assertThat(read.exists()).isTrue();
        assertThat(read.size()).isEqualTo(5);
    }

    @Test
    void listExistsAndDelete(@TempDir Path root) throws Exception {
        FilesystemTool fs = new FilesystemTool(root);
        fs.apply(FilesystemTool.Command.write("b.txt", "1"));
        fs.apply(FilesystemTool.Command.write("a.txt", "2"));

        FilesystemTool.Result listing = fs.apply(FilesystemTool.Command.list("."));
        assertThat(listing.content()).isEqualTo("a.txt\nb.txt"); // sorted
        assertThat(listing.size()).isEqualTo(2);

        assertThat(fs.apply(FilesystemTool.Command.exists("a.txt")).exists()).isTrue();
        assertThat(fs.apply(FilesystemTool.Command.exists("missing")).exists()).isFalse();

        FilesystemTool.Result deleted = fs.apply(FilesystemTool.Command.delete("a.txt"));
        assertThat(deleted.ok()).isTrue();
        assertThat(fs.apply(FilesystemTool.Command.exists("a.txt")).exists()).isFalse();
        // Deleting a missing path is not an error; ok reports nothing was removed.
        assertThat(fs.apply(FilesystemTool.Command.delete("a.txt")).ok()).isFalse();
    }

    @Test
    void readingANonFileFails(@TempDir Path root) throws Exception {
        FilesystemTool fs = new FilesystemTool(root);
        assertThatThrownBy(() -> fs.apply(FilesystemTool.Command.read("nope.txt")))
                .isInstanceOf(IOException.class);
        Files.createDirectory(root.resolve("dir"));
        assertThatThrownBy(() -> fs.apply(FilesystemTool.Command.read("dir")))
                .isInstanceOf(IOException.class);
    }

    @Test
    void rejectsParentTraversalAndAbsolutePaths(@TempDir Path root) {
        FilesystemTool fs = new FilesystemTool(root);
        assertThatThrownBy(() -> fs.apply(FilesystemTool.Command.read("../secret")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("escapes the sandbox root");
        assertThatThrownBy(() -> fs.apply(FilesystemTool.Command.write("../../etc/passwd", "x")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("escapes the sandbox root");
        assertThatThrownBy(() -> fs.apply(FilesystemTool.Command.read("/etc/hostname")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsBlankPathAndMissingRoot(@TempDir Path root) {
        FilesystemTool fs = new FilesystemTool(root);
        assertThatThrownBy(() -> fs.apply(FilesystemTool.Command.read("  ")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FilesystemTool(root.resolve("does-not-exist")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void writesAndReadsThroughNestedRealDirectories(@TempDir Path root) throws Exception {
        FilesystemTool fs = new FilesystemTool(root);
        Files.createDirectories(root.resolve("a/b/c")); // WRITE does not auto-create parents
        fs.apply(FilesystemTool.Command.write("a/b/c/deep.txt", "nested"));
        assertThat(Files.readString(root.resolve("a/b/c/deep.txt"))).isEqualTo("nested");
        assertThat(fs.apply(FilesystemTool.Command.read("a/b/c/deep.txt")).content()).isEqualTo("nested");
        assertThat(fs.apply(FilesystemTool.Command.list("a/b")).content()).isEqualTo("c");
    }

    @Test
    void insecureFallbackConstructorStillOperatesNormally(@TempDir Path root) throws Exception {
        // On a SecureDirectoryStream platform (Linux CI) the secure path is used regardless of the flag;
        // this just pins the opt-in constructor's normal behavior. The fail-closed branch only triggers
        // where no SecureDirectoryStream is available, which cannot be forced on this platform.
        FilesystemTool fs = new FilesystemTool(root, true);
        fs.apply(FilesystemTool.Command.write("f.txt", "v"));
        assertThat(fs.apply(FilesystemTool.Command.read("f.txt")).content()).isEqualTo("v");
    }

    @Test
    void writeDoesNotAutoCreateParentDirectories(@TempDir Path root) {
        FilesystemTool fs = new FilesystemTool(root);
        assertThatThrownBy(() -> fs.apply(FilesystemTool.Command.write("no/such/dir/file.txt", "x")))
                .isInstanceOf(java.nio.file.NoSuchFileException.class);
        assertThat(Files.exists(root.resolve("no"))).isFalse();
    }

    @Test
    void rejectsAnIntermediateDirectorySymlinkEscape(@TempDir Path base) throws Exception {
        Path root = Files.createDirectory(base.resolve("root"));
        Path outside = Files.createDirectory(base.resolve("outside"));
        Files.writeString(outside.resolve("secret.txt"), "top secret");

        try {
            Files.createSymbolicLink(root.resolve("link"), outside); // an *intermediate* component
        } catch (IOException | UnsupportedOperationException e) {
            assumeTrue(false, "symlinks unsupported on this platform");
        }

        FilesystemTool fs = new FilesystemTool(root);
        // Reading/writing *through* the symlinked directory must be refused, existing target or not.
        assertThatThrownBy(() -> fs.apply(FilesystemTool.Command.read("link/secret.txt")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> fs.apply(FilesystemTool.Command.write("link/planted.txt", "x")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(Files.exists(outside.resolve("planted.txt"))).isFalse();
    }

    @Test
    void rejectsSymlinkEscape(@TempDir Path base) throws Exception {
        Path root = Files.createDirectory(base.resolve("root"));
        Path outside = Files.createDirectory(base.resolve("outside"));
        Files.writeString(outside.resolve("secret.txt"), "top secret");

        Path link = root.resolve("escape");
        try {
            Files.createSymbolicLink(link, outside);
        } catch (IOException | UnsupportedOperationException e) {
            assumeTrue(false, "symlinks unsupported on this platform");
        }

        FilesystemTool fs = new FilesystemTool(root);
        assertThatThrownBy(() -> fs.apply(FilesystemTool.Command.read("escape/secret.txt")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("symlink");
    }
}
