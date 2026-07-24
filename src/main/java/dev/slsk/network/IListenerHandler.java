// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network;

import dev.slsk.diagnostics.IDiagnosticGenerator;
import dev.slsk.network.tcp.Connection;
import dev.slsk.network.tcp.Listener;

/** Handles connections accepted by the TCP listener. */
public interface IListenerHandler extends IDiagnosticGenerator {
    void handleConnection(Listener sender, Connection connection);
}
