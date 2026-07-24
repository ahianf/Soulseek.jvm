// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.handlers;

import dev.slsk.diagnostics.IDiagnosticGenerator;
import dev.slsk.network.IMessageConnection;
import dev.slsk.network.MessageEventArgs;

/** Handles incoming and outgoing protocol messages. */
public interface IMessageHandler extends IDiagnosticGenerator {
    void handleMessageRead(IMessageConnection sender, MessageEventArgs eventArgs);

    void handleMessageRead(IMessageConnection sender, byte[] message);

    void handleMessageWritten(IMessageConnection sender, MessageEventArgs eventArgs);
}
