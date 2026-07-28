// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.handlers;

import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.network.DistributedConnectionManager;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.internal.search.SearchResponder;

/** Internal client state consumed by the distributed message handler. */
public interface DistributedMessageHandlerClient {
    SoulseekClientOptions getOptions();

    String getUsername();

    int getNextToken();

    Waiter getWaiter();

    DistributedConnectionManager getDistributedConnectionManager();

    SearchResponder getSearchResponder();
}
