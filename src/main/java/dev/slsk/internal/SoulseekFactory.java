// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.Soulseek;
import dev.slsk.download.DownloadPolicy;
import dev.slsk.internal.options.ConnectionOptions;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.share.SharedFolder;
import dev.slsk.spi.ShareCatalog;
import dev.slsk.spi.TransferStore;
import dev.slsk.spi.UploadPolicy;
import dev.slsk.user.UserProfile;
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
     * @param shares what to share
     * @param downloads how the download queue is run
     * @param uploads who we serve and in what order
     * @param store where the download queue survives a restart
     * @param catalog a catalog of your own, or {@code null} for the scanned one
     * @param profile what peers are told about this account
     * @param peerTimeout how long a peer connection may sit idle
     * @param transferTimeout how long a transfer may move no bytes
     * @param messageTimeout how long to wait for a server response
     * @return the client
     */
    public static Soulseek create(
            String username,
            String password,
            int minorVersion,
            int listenPort,
            List<SharedFolder> shares,
            DownloadPolicy downloads,
            UploadPolicy uploads,
            TransferStore store,
            ShareCatalog catalog,
            UserProfile profile,
            java.time.Duration peerTimeout,
            java.time.Duration transferTimeout,
            java.time.Duration messageTimeout) {
        SoulseekClientOptions options = SoulseekClientOptions.builder()
                .enableListener(true)
                // Every address, which is what a listener peers are told to
                // connect to has to be. This bound the loopback address from
                // the day the builder was written, so the port every client
                // built this way advertised to the server was a port no peer
                // could ever reach: inbound direct connections, inbound browse
                // and every peer that answers a solicitation by connecting back
                // arrived at an interface nothing outside this machine can see.
                // Nothing chose that; there is no option for the address, and
                // the one javadoc claim about the port — "the port peers
                // connect to us on" — is only true now.
                .listenPort(listenPort)
                .messageTimeout(messageTimeout)
                .peerConnectionOptions(connection(peerTimeout))
                .transferConnectionOptions(connection(transferTimeout))
                .build();
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

    /** A connection's options, with only its idle budget changed. */
    private static ConnectionOptions connection(java.time.Duration idle) {
        return ConnectionOptions.builder().inactivityTimeout(idle).build();
    }
}
