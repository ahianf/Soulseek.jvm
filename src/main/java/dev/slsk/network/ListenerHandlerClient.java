// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network;

import dev.slsk.common.IWaiter;
import dev.slsk.network.tcp.Listener;
import dev.slsk.options.SoulseekClientOptions;
import dev.slsk.search.ISearchResponder;

/** Internal client state consumed by the incoming-connection handler. */
public interface ListenerHandlerClient {
    SoulseekClientOptions getOptions();

    Listener getListener();

    PeerConnectionManager getPeerConnectionManager();

    DistributedConnectionManager getDistributedConnectionManager();

    IWaiter getWaiter();

    ISearchResponder getSearchResponder();
}
