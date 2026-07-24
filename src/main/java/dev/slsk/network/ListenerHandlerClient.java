// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network;

import dev.slsk.common.IWaiter;
import dev.slsk.network.tcp.IListener;
import dev.slsk.options.SoulseekClientOptions;
import dev.slsk.search.ISearchResponder;

/** Internal client state consumed by the incoming-connection handler. */
public interface ListenerHandlerClient {
    SoulseekClientOptions getOptions();

    IListener getListener();

    IPeerConnectionManager getPeerConnectionManager();

    IDistributedConnectionManager getDistributedConnectionManager();

    IWaiter getWaiter();

    ISearchResponder getSearchResponder();
}
