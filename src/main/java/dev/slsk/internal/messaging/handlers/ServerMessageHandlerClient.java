// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.handlers;

import dev.slsk.CancellationSignal;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.network.DistributedConnectionManager;
import dev.slsk.internal.network.PeerConnectionManager;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.internal.search.SearchInternal;
import dev.slsk.internal.search.SearchResponder;
import dev.slsk.internal.transfer.TransferInternal;
import java.util.Map;

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

    void acknowledgePrivateMessageOperation(int id, CancellationSignal cancellationSignal);

    void acknowledgePrivilegeNotificationOperation(int id, CancellationSignal cancellationSignal);
}
