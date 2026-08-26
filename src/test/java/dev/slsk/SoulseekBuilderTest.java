// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.download.DownloadPolicy;
import dev.slsk.share.SharedFolder;
import dev.slsk.spi.ShareCatalog;
import dev.slsk.spi.UploadPolicy;
import dev.slsk.user.UserProfile;
import dev.slsk.user.Username;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The only way to get a client.
 *
 * <p>What used to be a static factory plus a thirty-parameter options record is
 * a named method per setting, so a client can be read back rather than counted
 * in commas.
 */
class SoulseekBuilderTest {

    private static SoulseekBuilder minimal() {
        return Soulseek.builder().credentials("alice", "password").applicationMinorVersion(157);
    }

    @Test
    @DisplayName("credentials and the minor version are the only two required settings")
    void theShortestCorrectClientIsThreeCalls() {
        try (Soulseek slsk = minimal().build()) {
            assertEquals(Username.of("alice"), slsk.me().username());
            assertEquals(DownloadPolicy.defaults(), slsk.downloads().policy());
            assertEquals(UserProfile.empty(), slsk.me().profile());
            assertEquals(List.of(), slsk.shares().configured());
        }
    }

    /**
     * The server uses it to tell client builds apart, and every client that
     * shipped with a borrowed one made someone else's traffic look like its own.
     */
    @Test
    void theMinorVersionIsRequiredAndCannotBeBorrowed() {
        assertThrows(
                IllegalStateException.class,
                () -> Soulseek.builder().credentials("alice", "password").build());
        assertThrows(
                IllegalStateException.class,
                () -> Soulseek.builder().applicationMinorVersion(157).build());
        assertThrows(IllegalArgumentException.class, () -> Soulseek.builder().applicationMinorVersion(100));
    }

    @Test
    void everySettingReachesTheClientItConfigures(@TempDir Path directory) {
        DownloadPolicy downloads =
                DownloadPolicy.defaults().maxConcurrent(9).queuePositionPollInterval(Duration.ofMinutes(2));
        UploadPolicy uploads = UploadPolicy.refuseAll();
        UserProfile profile = new UserProfile("hello", Optional.empty(), 3, 1, true);

        try (Soulseek slsk = minimal()
                .downloads(downloads)
                .uploads(uploads)
                .profile(profile)
                .share(directory)
                .listenPort(2235)
                .build()) {
            assertEquals(downloads, slsk.downloads().policy());
            assertSame(uploads, slsk.uploads().policy());
            assertEquals(profile, slsk.me().profile());
            assertEquals(List.of(SharedFolder.of(directory)), slsk.shares().configured());
        }
    }

    @Test
    @DisplayName("an installed catalog is what peers are served from, without a scan")
    void aCatalogCanBeInstalledAtBuildTime() {
        ShareCatalog mine = ShareCatalog.empty();
        try (Soulseek slsk = minimal().catalog(mine).build()) {
            assertEquals(mine.index(), slsk.shares().index());
        }
    }

    @Test
    void rejectsSettingsThatCannotMeanAnything() {
        assertThrows(IllegalArgumentException.class, () -> Soulseek.builder().listenPort(80));
        assertThrows(IllegalArgumentException.class, () -> Soulseek.builder().listenPort(70_000));
        assertThrows(NullPointerException.class, () -> Soulseek.builder().credentials(null, "password"));
        assertThrows(NullPointerException.class, () -> Soulseek.builder().downloads(null));
        assertThrows(NullPointerException.class, () -> Soulseek.builder().uploads(null));
        assertThrows(NullPointerException.class, () -> Soulseek.builder().profile(null));
        assertThrows(NullPointerException.class, () -> Soulseek.builder().transferStore(null));
    }

    /**
     * The `tenine` migration found these missing: five timeouts had been raised
     * from the library's defaults, each with a recorded reason, and the 1.0
     * builder had nowhere to put them. Three named settings cover what actually
     * mattered, without exporting a seven-field options record.
     */
    @Test
    @DisplayName("the timeouts a real consumer had to raise are settable")
    void theTimeoutsThatMatterAreSettable() {
        try (Soulseek slsk = minimal()
                .peerTimeout(Duration.ofSeconds(90))
                .transferTimeout(Duration.ofSeconds(120))
                .messageTimeout(Duration.ofSeconds(15))
                .build()) {
            assertEquals(Username.of("alice"), slsk.me().username());
        }
    }

    @Test
    void rejectsTimeoutsThatCannotMeanAnything() {
        assertThrows(IllegalArgumentException.class, () -> Soulseek.builder().peerTimeout(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> Soulseek.builder().transferTimeout(Duration.ofSeconds(-1)));
        assertThrows(NullPointerException.class, () -> Soulseek.builder().messageTimeout(null));
    }
}
