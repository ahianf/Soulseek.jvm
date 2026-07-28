// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.handlers;

import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.MessageEvent;

/** Handles messages on distributed parent and child connections. */
public interface DistributedMessageHandler extends MessageHandler {
    void handleChildMessageRead(MessageConnection sender, MessageEvent eventData);

    void handleChildMessageRead(MessageConnection sender, byte[] message);

    void handleChildMessageWritten(MessageConnection sender, MessageEvent eventData);

    void handleEmbeddedMessage(byte[] message);
}
