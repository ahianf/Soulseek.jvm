// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.handlers;

import dev.slsk.CancellationToken;
import dev.slsk.common.Waiter;
import dev.slsk.network.DistributedConnectionManager;
import dev.slsk.network.PeerConnectionManager;
import dev.slsk.options.SoulseekClientOptions;
import dev.slsk.search.SearchInternal;
import dev.slsk.search.SearchResponder;
import dev.slsk.transfer.TransferInternal;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Internal client state and operations used by the server handler. */
public interface ServerMessageHandlerClient {
    SoulseekClientOptions getOptions();

    String getUsername();

    Waiter getWaiter();

    Map<Integer, SearchInternal> getSearches();

    Map<Integer, TransferInternal> getDownloadDictionary();

    PeerConnectionManager getPeerConnectionManager();

    DistributedConnectionManager getDistributedConnectionManager();

    DistributedMessageHandler getDistributedMessageHandler();

    SearchResponder getSearchResponder();

    CompletableFuture<Void> acknowledgePrivateMessageAsync(int id, CancellationToken cancellationToken);

    CompletableFuture<Void> acknowledgePrivilegeNotificationAsync(int id, CancellationToken cancellationToken);
}
