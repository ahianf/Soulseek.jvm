// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

/** Data describing a TCP connection state change. */
public record ConnectionStateChangedEvent(
        Connection connection,
        ConnectionState previousState,
        ConnectionState currentState,
        String message,
        Exception exception)
        implements ConnectionEvent {

    /** Creates state-change data without optional details. */
    public ConnectionStateChangedEvent(ConnectionState previousState, ConnectionState currentState) {
        this(null, previousState, currentState, null, null);
    }

    /** Creates state-change data with a message. */
    public ConnectionStateChangedEvent(ConnectionState previousState, ConnectionState currentState, String message) {
        this(null, previousState, currentState, message, null);
    }

    /** Creates state-change data. */
    public ConnectionStateChangedEvent(
            ConnectionState previousState, ConnectionState currentState, String message, Exception exception) {
        this(null, previousState, currentState, message, exception);
    }
}
