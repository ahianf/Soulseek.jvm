// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.handlers;

import dev.slsk.network.IMessageConnection;
import dev.slsk.network.MessageReceivedEventArgs;

/** Handles messages received from peer connections. */
public interface IPeerMessageHandler extends IMessageHandler {
    void handleMessageReceived(IMessageConnection sender, MessageReceivedEventArgs eventArgs);
}
