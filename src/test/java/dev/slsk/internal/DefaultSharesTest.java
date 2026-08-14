// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.CancellationSignal;
import dev.slsk.Soulseek;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.search.SearchFile;
import dev.slsk.share.BrowseResponse;
import dev.slsk.share.Directory;
import dev.slsk.share.ShareIndex;
import dev.slsk.share.SharedFolder;
import dev.slsk.spi.ResolvedFile;
import dev.slsk.spi.ShareCatalog;
import dev.slsk.user.Username;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What a scan produces, and what replacing it means.
 *
 * <p>Before Phase 5 a scan counted files and served none of them: the client
 * told every peer it had ten thousand files and answered every browse with an
 * empty list, which is worse than sharing nothing. These assert that the scan
 * builds what it counted.
 *
 * <p>The engine is offline in all of these, so the announcement at the end of a
 * scan fails. That is the last thing {@code rescan} does, and everything under
 * test has already happened by then.
 */
class DefaultSharesTest {

    private static final Username PEER = Username.of("bob");

    private static Soulseek client() {
        return DefaultSoulseek.create("alice", "password", 157, new SoulseekClientOptions());
    }

    /**
     * Scans. The offline announcement at the end used to throw the whole index
     * away after the scan had succeeded; it is tolerated and logged now, the
     * same way the login-time announcement always tolerated it.
     */
    private static void scan(Soulseek slsk) {
        slsk.shares().rescan(CancellationSignal.none());
    }

    private static ShareCatalog catalogOf(Soulseek slsk) {
        return ((DefaultSoulseek) slsk).client().catalog();
    }

    private static Path share(Path root) throws IOException {
        Path music = root.resolve("Music");
        Files.createDirectories(music.resolve("Album"));
        Files.writeString(music.resolve("Album").resolve("one.mp3"), "aaaa");
        Files.writeString(music.resolve("Album").resolve("two.flac"), "bb");
        return music;
    }

    @Test
    @DisplayName("a scan builds the catalog peers are served from")
    void scanInstallsACatalogThatServesTheShare(@TempDir Path root) throws IOException {
        Path music = share(root);
        try (Soulseek slsk = client()) {
            slsk.shares().configure(List.of(SharedFolder.of(music)));
            scan(slsk);

            BrowseResponse browse = catalogOf(slsk).browse(PEER);
            assertEquals(1, browse.directories().size());
            assertEquals("Music\\Album", browse.directories().getFirst().name());
            assertEquals(2, browse.fileCount());
            assertEquals(List.of(), browse.lockedDirectories());
        }
    }

    @Test
    @DisplayName("the index reports what the catalog holds, and the counts announced are the same ones")
    void indexDescribesTheCatalog(@TempDir Path root) throws IOException {
        Path music = share(root);
        try (Soulseek slsk = client()) {
            slsk.shares().configure(List.of(SharedFolder.of(music)));
            scan(slsk);

            ShareIndex index = slsk.shares().index();
            assertEquals(2, index.fileCount());
            assertEquals(6, index.totalBytes());
            assertEquals(1, index.directoryCount());
            assertEquals(ShareIndex.ScanStatus.READY, index.status());
            assertEquals(index, catalogOf(slsk).index());
        }
    }

    @Test
    void searchMatchesEveryTermAndHonoursTheLimit(@TempDir Path root) throws IOException {
        Path music = share(root);
        try (Soulseek slsk = client()) {
            slsk.shares().configure(List.of(SharedFolder.of(music)));
            scan(slsk);
            ShareCatalog catalog = catalogOf(slsk);

            assertEquals(1, catalog.search(PEER, "album one", 10).size());
            assertEquals(2, catalog.search(PEER, "album", 10).size());
            assertEquals(1, catalog.search(PEER, "album", 1).size(), "the limit is honoured");
            assertEquals(List.of(), catalog.search(PEER, "album nothing", 10));
            assertEquals(List.of(), catalog.search(PEER, "album", 0));
            assertEquals(List.of(), catalog.search(PEER, "  ", 10));
        }
    }

    @Test
    void directoryAnswersOnlyTheDirectoryAsked(@TempDir Path root) throws IOException {
        Path music = share(root);
        try (Soulseek slsk = client()) {
            slsk.shares().configure(List.of(SharedFolder.of(music)));
            scan(slsk);
            ShareCatalog catalog = catalogOf(slsk);

            List<Directory> found = catalog.directory(PEER, "Music\\Album");
            assertEquals(1, found.size());
            assertEquals(2, found.getFirst().fileCount());
            assertEquals(List.of(), catalog.directory(PEER, "Music\\Nothing"));
        }
    }

    @Test
    @DisplayName("resolve opens the real file, and refuses anything that is not one of ours")
    void resolveMapsBackToTheLocalFile(@TempDir Path root) throws IOException {
        Path music = share(root);
        Files.writeString(root.resolve("secret.txt"), "not shared");
        try (Soulseek slsk = client()) {
            slsk.shares().configure(List.of(SharedFolder.of(music)));
            scan(slsk);
            ShareCatalog catalog = catalogOf(slsk);

            Optional<ResolvedFile> resolved = catalog.resolve(PEER, "Music\\Album\\one.mp3");
            assertTrue(resolved.isPresent());
            assertEquals(4, resolved.get().size());
            assertEquals("aaa", read(resolved.get(), 1));

            // One answer for every rejection: traversal, a share we do not
            // serve, and a file that is simply not there.
            assertFalse(catalog.resolve(PEER, "Music\\..\\secret.txt").isPresent());
            assertFalse(catalog.resolve(PEER, "Other\\Album\\one.mp3").isPresent());
            assertFalse(catalog.resolve(PEER, "Music\\Album\\absent.mp3").isPresent());
        }
    }

    @Test
    @DisplayName("a locked folder is listed but kept out of the open directories")
    void lockedFoldersAreListedSeparately(@TempDir Path root) throws IOException {
        Path music = share(root);
        try (Soulseek slsk = client()) {
            slsk.shares().configure(List.of(SharedFolder.locked(music)));
            scan(slsk);

            BrowseResponse browse = catalogOf(slsk).browse(PEER);
            assertEquals(List.of(), browse.directories());
            assertEquals(1, browse.lockedDirectories().size());
            assertEquals(2, browse.fileCount());
        }
    }

    @Test
    @DisplayName("an installed catalog replaces the scan, and a later rescan leaves it alone")
    void catalogReplacesTheBuiltInIndex(@TempDir Path root) throws IOException {
        Path music = share(root);
        ShareCatalog mine = ShareCatalog.empty();
        try (Soulseek slsk = client()) {
            slsk.shares().configure(List.of(SharedFolder.of(music)));
            slsk.shares().catalog(mine);
            assertSame(mine, catalogOf(slsk));
            assertEquals(mine.index(), slsk.shares().index());

            scan(slsk);
            assertSame(mine, catalogOf(slsk), "a rescan must not take back a catalog the consumer installed");
        }
    }

    @Test
    void nothingIsSharedUntilSomethingIsScanned() {
        try (Soulseek slsk = client()) {
            ShareCatalog catalog = catalogOf(slsk);
            assertEquals(BrowseResponse.empty(), catalog.browse(PEER));
            assertEquals(List.of(), catalog.search(PEER, "anything", 10));
            assertFalse(catalog.resolve(PEER, "Music\\Album\\one.mp3").isPresent());
        }
    }

    @Test
    void aScannedFileCarriesItsRemotePathAndSize(@TempDir Path root) throws IOException {
        Path music = share(root);
        try (Soulseek slsk = client()) {
            slsk.shares().configure(List.of(SharedFolder.of(music)));
            scan(slsk);

            SearchFile file = catalogOf(slsk).search(PEER, "one.mp3", 10).getFirst();
            assertEquals("Music\\Album\\one.mp3", file.path());
            assertEquals(4, file.size());
            assertEquals("mp3", file.extension());
        }
    }

    private static String read(ResolvedFile file, long offset) throws IOException {
        try (ReadableByteChannel channel = file.open(offset)) {
            ByteBuffer buffer = ByteBuffer.allocate((int) file.size());
            while (channel.read(buffer) > 0) {
                // read to the end
            }
            buffer.flip();
            return StandardCharsets.UTF_8.decode(buffer).toString();
        }
    }
}
