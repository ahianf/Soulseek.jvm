// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.handlers;

import dev.slsk.diagnostics.DiagnosticSource;
import dev.slsk.network.MessageConnection;
import dev.slsk.network.MessageEvent;

/** Handles incoming and outgoing protocol messages. */
public interface MessageHandler extends DiagnosticSource {
    void handleMessageRead(MessageConnection sender, MessageEvent eventData);

    void handleMessageRead(MessageConnection sender, byte[] message);

    void handleMessageWritten(MessageConnection sender, MessageEvent eventData);
}
