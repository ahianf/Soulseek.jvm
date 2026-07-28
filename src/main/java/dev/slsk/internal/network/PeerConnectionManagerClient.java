// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.messaging.handlers.PeerMessageHandler;
import dev.slsk.internal.options.SoulseekClientOptions;

/** Internal client state consumed by the peer-connection manager. */
public interface PeerConnectionManagerClient {
    SoulseekClientOptions getOptions();

    String getUsername();

    int getNextToken();

    Waiter getWaiter();

    MessageConnection getServerConnection();

    PeerMessageHandler getPeerMessageHandler();
}
