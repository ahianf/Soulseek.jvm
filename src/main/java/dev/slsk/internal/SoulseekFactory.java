// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.DiagnosticLevel;
import dev.slsk.DownloadPolicy;
import dev.slsk.SharedFolder;
import dev.slsk.Soulseek;
import dev.slsk.UserProfile;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.spi.ShareCatalog;
import dev.slsk.spi.TransferStore;
import dev.slsk.spi.UploadPolicy;
import java.net.InetAddress;
import java.util.List;

/**
 * What the builder calls.
 *
 * <p>The builder is exported and the engine is not, so something has to sit
 * between them. This is that and nothing else: it translates the builder's
 * named settings into the option record the engine still takes, and applies the
 * ones that are not options at all but state on the assembled client.
 */
public final class SoulseekFactory {

    private SoulseekFactory() {}

    /**
     * Assembles a client.
     *
     * @param username the account to log in as
     * @param password the account password
     * @param minorVersion the application minor version
     * @param listenPort the port peers connect to us on
     * @param diagnostics how much the library says
     * @param shares what to share
     * @param downloads how the download queue is run
     * @param uploads who we serve and in what order
     * @param store where the download queue survives a restart
     * @param catalog a catalog of your own, or {@code null} for the scanned one
     * @param profile what peers are told about this account
     * @return the client
     */
    public static Soulseek create(
            String username,
            String password,
            int minorVersion,
            int listenPort,
            DiagnosticLevel diagnostics,
            List<SharedFolder> shares,
            DownloadPolicy downloads,
            UploadPolicy uploads,
            TransferStore store,
            ShareCatalog catalog,
            UserProfile profile) {
        SoulseekClientOptions options = new SoulseekClientOptions(
                true, InetAddress.getLoopbackAddress(), listenPort, 5_000, level(diagnostics));
        Soulseek client = DefaultSoulseek.create(username, password, minorVersion, options, store);

        client.me().profile(profile);
        client.uploads().policy(uploads);
        client.downloads().policy(downloads);
        if (!shares.isEmpty()) {
            client.shares().configure(shares);
        }
        if (catalog != null) {
            client.shares().catalog(catalog);
        }
        return client;
    }

    /** The public level, as the internal sink spells it. */
    private static dev.slsk.internal.diagnostics.DiagnosticLevel level(DiagnosticLevel level) {
        return switch (level) {
            case NONE -> dev.slsk.internal.diagnostics.DiagnosticLevel.NONE;
            case WARNING -> dev.slsk.internal.diagnostics.DiagnosticLevel.WARNING;
            case INFO -> dev.slsk.internal.diagnostics.DiagnosticLevel.INFO;
            case DEBUG -> dev.slsk.internal.diagnostics.DiagnosticLevel.DEBUG;
            case TRACE -> dev.slsk.internal.diagnostics.DiagnosticLevel.TRACE;
        };
    }
}
