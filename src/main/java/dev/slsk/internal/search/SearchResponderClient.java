// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import dev.slsk.CancellationSignal;
import dev.slsk.internal.network.PeerConnectionManager;
import dev.slsk.internal.options.SoulseekClientOptions;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

/**
 * The subset of internal client orchestration consumed by a search responder.
 */
public interface SearchResponderClient {
    SoulseekClientOptions getOptions();

    PeerConnectionManager getPeerConnectionManager();

    int getNextToken();

    CompletableFuture<InetSocketAddress> getUserEndpointOperation(
            String username, CancellationSignal cancellationSignal);

    /**
     * Returns what peers are served from.
     *
     * @return the installed share catalog, never {@code null}
     */
    dev.slsk.spi.ShareCatalog getShareCatalog();

    /**
     * Returns our own logged-in username, which the peer keys our response on.
     *
     * @return the logged-in username, or {@code null}
     */
    String getLoggedInUsername();
}
