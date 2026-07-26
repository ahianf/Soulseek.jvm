// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RemotePathTest {

    @Nested
    class Splitting {

        @Test
        void basenameTakesTheFinalSegmentOfEitherSeparator() {
            assertEquals("track.flac", RemotePath.basename("Music\\Album\\track.flac"));
            assertEquals("track.flac", RemotePath.basename("Music/Album/track.flac"));
            assertEquals("track.flac", RemotePath.basename("track.flac"));
        }

        @Test
        void basenameReturnsTheInputWhenItCannotSplit() {
            assertEquals("", RemotePath.basename(null));
            assertEquals("", RemotePath.basename(""));
            assertEquals("Music\\", RemotePath.basename("Music\\"));
        }

        @Test
        void parentTakesEverythingBeforeTheFinalSegment() {
            assertEquals("Music\\Album", RemotePath.parent("Music\\Album\\track.flac"));
            assertEquals("", RemotePath.parent("track.flac"));
            assertEquals("", RemotePath.parent(null));
        }

        @Test
        void lastFolderSegmentTakesTheDeepestFolder() {
            assertEquals("Album", RemotePath.lastFolderSegment("Music\\Album\\track.flac"));
            assertEquals("", RemotePath.lastFolderSegment("track.flac"));
        }
    }

    @Nested
    class ToRemote {

        @TempDir
        Path base;

        private Path root;
        private Path outside;

        @BeforeEach
        void layOutTheShare() throws IOException {
            // Two siblings under one temp dir, so "outside the share" is a
            // fixed relative distance rather than whatever the OS temp root
            // happens to contain.
            root = Files.createDirectories(base.resolve("share"));
            outside = Files.createDirectories(base.resolve("outside"));
        }

        @Test
        void buildsTheVirtualPathFromTheShareNameAndTheRelativeLocation() throws IOException {
            Path file = Files.createFile(
                    Files.createDirectories(root.resolve("Album")).resolve("track.flac"));

            assertEquals("Music\\Album\\track.flac", RemotePath.toRemote("Music", root, file));
        }

        @Test
        void placesAFileAtTheRootDirectlyUnderTheShareName() throws IOException {
            Path file = Files.createFile(root.resolve("track.flac"));

            assertEquals("Music\\track.flac", RemotePath.toRemote("Music", root, file));
        }

        @Test
        void escapesABackslashInsideALocalName() throws IOException {
            // Legal on POSIX; without escaping it would advertise a folder that
            // does not exist.
            Path file = root.resolve("AC\\DC.mp3");
            if (!file.getFileName().toString().equals("AC\\DC.mp3")) {
                return; // The platform treats it as a separator; nothing to escape.
            }
            Files.createFile(file);

            assertEquals("Music\\AC@@BACKSLASH@@DC.mp3", RemotePath.toRemote("Music", root, file));
        }

        @Test
        void rejectsAFileOutsideTheRoot() {
            Path stranger = outside.resolve("track.flac");

            assertThrows(IllegalArgumentException.class, () -> RemotePath.toRemote("Music", root, stranger));
        }

        @Test
        void rejectsTheRootItself() {
            assertThrows(IllegalArgumentException.class, () -> RemotePath.toRemote("Music", root, root));
        }

        @ParameterizedTest
        @ValueSource(strings = {"", "   ", "Mu\\sic", "Mu/sic"})
        void rejectsAnUnusableShareName(String shareName) {
            Path file = root.resolve("track.flac");

            assertThrows(IllegalArgumentException.class, () -> RemotePath.toRemote(shareName, root, file));
        }

        @Test
        void rejectsNullArguments() {
            assertThrows(NullPointerException.class, () -> RemotePath.toRemote(null, root, root.resolve("t")));
            assertThrows(NullPointerException.class, () -> RemotePath.toRemote("Music", null, root.resolve("t")));
            assertThrows(NullPointerException.class, () -> RemotePath.toRemote("Music", root, null));
        }
    }

    @Nested
    class ToLocal {

        @TempDir
        Path base;

        private Path root;
        private Path outside;

        @BeforeEach
        void layOutTheShare() throws IOException {
            root = Files.createDirectories(base.resolve("share"));
            outside = Files.createDirectories(base.resolve("outside"));
        }

        private Path shared(String... segments) throws IOException {
            Path path = root;
            for (int index = 0; index < segments.length - 1; index++) {
                path = Files.createDirectories(path.resolve(segments[index]));
            }
            return Files.createFile(path.resolve(segments[segments.length - 1]));
        }

        @Test
        void resolvesAPathThisClassProduced() throws IOException {
            Path file = shared("Album", "track.flac");
            String remote = RemotePath.toRemote("Music", root, file);

            assertEquals(Optional.of(file.toRealPath()), RemotePath.toLocal(remote, "Music", root));
        }

        @Test
        void acceptsForwardSlashesFromClientsThatSendThem() throws IOException {
            Path file = shared("Album", "track.flac");

            assertEquals(Optional.of(file.toRealPath()), RemotePath.toLocal("Music/Album/track.flac", "Music", root));
        }

        @Test
        void reversesTheBackslashEscape() throws IOException {
            Path file = root.resolve("AC\\DC.mp3");
            if (!file.getFileName().toString().equals("AC\\DC.mp3")) {
                return;
            }
            Files.createFile(file);

            assertEquals(
                    Optional.of(file.toRealPath()), RemotePath.toLocal("Music\\AC@@BACKSLASH@@DC.mp3", "Music", root));
        }

        @Test
        void rejectsTraversalOutOfTheShare() throws IOException {
            Files.createFile(outside.resolve("secret.txt"));

            assertTrue(RemotePath.toLocal("Music\\..\\outside\\secret.txt", "Music", root)
                    .isEmpty());
            assertTrue(RemotePath.toLocal("Music\\Album\\..\\..\\outside\\secret.txt", "Music", root)
                    .isEmpty());
            assertTrue(RemotePath.toLocal("Music/../outside/secret.txt", "Music", root)
                    .isEmpty());
        }

        @Test
        void rejectsASingleDotSegment() throws IOException {
            shared("Album", "track.flac");

            assertTrue(RemotePath.toLocal("Music\\.\\Album\\track.flac", "Music", root)
                    .isEmpty());
        }

        @ParameterizedTest
        @ValueSource(
                strings = {
                    "\\Music\\track.flac", // absolute POSIX path
                    "/etc/passwd",
                    "\\\\host\\share\\track.flac", // UNC
                    "C:\\Windows\\win.ini",
                    "Other\\track.flac", // a share we do not serve
                    "Music", // the share itself, not a file in it
                    "",
                    "   "
                })
        void rejectsPathsThatDoNotNameAFileInThisShare(String remotePath) {
            assertTrue(RemotePath.toLocal(remotePath, "Music", root).isEmpty());
        }

        @Test
        void rejectsAnEmbeddedNulByte() throws IOException {
            shared("track.flac");

            assertTrue(
                    RemotePath.toLocal("Music\\track.flac\0.txt", "Music", root).isEmpty());
        }

        @Test
        void rejectsAnEmptyInteriorSegment() throws IOException {
            shared("Album", "track.flac");

            // Collapsing separators must not be a way to smuggle an empty name.
            assertTrue(RemotePath.toLocal("Music\\\\Album\\track.flac", "Music", root)
                    .isEmpty());
        }

        @Test
        void rejectsAFileThatDoesNotExist() {
            assertTrue(RemotePath.toLocal("Music\\Album\\missing.flac", "Music", root)
                    .isEmpty());
        }

        @Test
        void rejectsASymlinkPointingOutOfTheShare() throws IOException {
            Path secret = Files.createFile(outside.resolve("secret.txt"));
            Path link = root.resolve("innocent.txt");
            try {
                Files.createSymbolicLink(link, secret);
            } catch (IOException | UnsupportedOperationException unsupported) {
                return; // The platform does not permit symlinks in this context.
            }

            // Syntactically flawless, inside the share by name, and still out of bounds.
            assertTrue(RemotePath.toLocal("Music\\innocent.txt", "Music", root).isEmpty());
        }

        @Test
        void rejectsAnUnusableShareNameOrRoot() throws IOException {
            shared("track.flac");

            assertTrue(RemotePath.toLocal("Music\\track.flac", "", root).isEmpty());
            assertTrue(RemotePath.toLocal("Music\\track.flac", "Mu\\sic", root).isEmpty());
            assertTrue(RemotePath.toLocal("Music\\track.flac", "Music", root.resolve("missing"))
                    .isEmpty());
        }

        @Test
        void rejectsNullArguments() {
            assertTrue(RemotePath.toLocal(null, "Music", root).isEmpty());
            assertTrue(RemotePath.toLocal("Music\\track.flac", null, root).isEmpty());
            assertTrue(RemotePath.toLocal("Music\\track.flac", "Music", null).isEmpty());
        }
    }
}
