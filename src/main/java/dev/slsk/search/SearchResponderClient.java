// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.search;

import dev.slsk.CancellationToken;
import dev.slsk.network.PeerConnectionManager;
import dev.slsk.options.SoulseekClientOptions;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

/**
 * The subset of internal client orchestration consumed by a search responder.
 */
public interface SearchResponderClient {
    SoulseekClientOptions getOptions();

    PeerConnectionManager getPeerConnectionManager();

    int getNextToken();

    CompletableFuture<InetSocketAddress> getUserEndpointAsync(String username, CancellationToken cancellationToken);
}
