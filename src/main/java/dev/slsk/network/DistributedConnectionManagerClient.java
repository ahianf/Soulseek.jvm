// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network;

import dev.slsk.SoulseekClientStates;
import dev.slsk.common.Waiter;
import dev.slsk.messaging.handlers.DistributedMessageHandler;
import dev.slsk.options.SoulseekClientOptions;

/** Internal client state consumed by the distributed manager. */
public interface DistributedConnectionManagerClient {
    SoulseekClientOptions getOptions();

    String getUsername();

    SoulseekClientStates getState();

    int getNextToken();

    Waiter getWaiter();

    MessageConnection getServerConnection();

    DistributedMessageHandler getDistributedMessageHandler();
}
