// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import dev.slsk.internal.diagnostics.DiagnosticSource;
import dev.slsk.internal.network.tcp.Connection;

/** Handles connections accepted by the TCP listener. */
public interface ListenerHandler extends DiagnosticSource {
    void handleConnection(Connection connection);
}
