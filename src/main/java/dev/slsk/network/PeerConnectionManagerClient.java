// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network;

import dev.slsk.common.IWaiter;
import dev.slsk.messaging.handlers.IPeerMessageHandler;
import dev.slsk.options.SoulseekClientOptions;

/** Internal client state consumed by the peer-connection manager. */
public interface PeerConnectionManagerClient {
    SoulseekClientOptions getOptions();

    String getUsername();

    int getNextToken();

    IWaiter getWaiter();

    IMessageConnection getServerConnection();

    IPeerMessageHandler getPeerMessageHandler();
}
