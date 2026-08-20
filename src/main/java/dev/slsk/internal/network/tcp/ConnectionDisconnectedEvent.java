// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

/** Data describing a TCP disconnection. */
public record ConnectionDisconnectedEvent(TransportConnection connection, String message, Exception exception)
        implements TransportEvent {

    /** Creates a disconnection without an exception. */
    public ConnectionDisconnectedEvent(String message) {
        this(null, message, null);
    }

    /** Creates disconnection event data. */
    public ConnectionDisconnectedEvent(String message, Exception exception) {
        this(null, message, exception);
    }
}
