// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.handlers;

import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.MessageEvent;

/** Handles incoming and outgoing protocol messages. */
public interface MessageHandler {
    void handleMessageRead(MessageEvent eventData);

    void handleMessageRead(MessageConnection sender, byte[] message);

    void handleMessageWritten(MessageEvent eventData);
}
