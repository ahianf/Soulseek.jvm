// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.handlers;

import dev.slsk.common.IWaiter;
import dev.slsk.network.DistributedConnectionManager;
import dev.slsk.options.SoulseekClientOptions;
import dev.slsk.search.ISearchResponder;

/** Internal client state consumed by the distributed message handler. */
public interface DistributedMessageHandlerClient {
    SoulseekClientOptions getOptions();

    String getUsername();

    int getNextToken();

    IWaiter getWaiter();

    DistributedConnectionManager getDistributedConnectionManager();

    ISearchResponder getSearchResponder();
}
