// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.handlers;

import dev.slsk.network.IMessageConnection;
import dev.slsk.network.MessageEventArgs;

/** Handles messages on distributed parent and child connections. */
public interface DistributedMessageHandler extends MessageHandler {
    void handleChildMessageRead(IMessageConnection sender, MessageEventArgs eventArgs);

    void handleChildMessageRead(IMessageConnection sender, byte[] message);

    void handleChildMessageWritten(IMessageConnection sender, MessageEventArgs eventArgs);

    void handleEmbeddedMessage(byte[] message);
}
