// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import dev.slsk.internal.SoulseekClientState;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.messaging.handlers.DistributedMessageHandler;
import dev.slsk.internal.options.SoulseekClientOptions;

/** Internal client state consumed by the distributed manager. */
public interface DistributedConnectionManagerClient {
    SoulseekClientOptions getOptions();

    String getUsername();

    SoulseekClientState getState();

    int getNextToken();

    Waiter getWaiter();

    MessageConnection getServerConnection();

    DistributedMessageHandler getDistributedMessageHandler();
}
