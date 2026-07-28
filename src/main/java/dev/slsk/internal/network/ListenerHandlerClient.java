// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.network.tcp.Listener;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.internal.search.SearchResponder;

/** Internal client state consumed by the incoming-connection handler. */
public interface ListenerHandlerClient {
    SoulseekClientOptions getOptions();

    Listener getListener();

    PeerConnectionManager getPeerConnectionManager();

    DistributedConnectionManager getDistributedConnectionManager();

    Waiter getWaiter();

    SearchResponder getSearchResponder();
}
