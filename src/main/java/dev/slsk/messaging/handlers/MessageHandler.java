// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.handlers;

import dev.slsk.diagnostics.IDiagnosticGenerator;
import dev.slsk.network.MessageConnection;
import dev.slsk.network.MessageEventArgs;

/** Handles incoming and outgoing protocol messages. */
public interface MessageHandler extends IDiagnosticGenerator {
    void handleMessageRead(MessageConnection sender, MessageEventArgs eventArgs);

    void handleMessageRead(MessageConnection sender, byte[] message);

    void handleMessageWritten(MessageConnection sender, MessageEventArgs eventArgs);
}
