// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network;

import dev.slsk.diagnostics.IDiagnosticGenerator;
import dev.slsk.network.tcp.IConnection;
import dev.slsk.network.tcp.IListener;

/** Handles connections accepted by the TCP listener. */
public interface IListenerHandler extends IDiagnosticGenerator {
    void handleConnection(IListener sender, IConnection connection);
}
