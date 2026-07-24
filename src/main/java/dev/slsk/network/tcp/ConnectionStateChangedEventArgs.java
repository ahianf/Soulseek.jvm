// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network.tcp;

/** Data describing a TCP connection state change. */
public final class ConnectionStateChangedEventArgs extends ConnectionEventArgs {

    private final ConnectionState currentState;
    private final Exception exception;
    private final String message;
    private final ConnectionState previousState;

    /** Creates state-change data without optional details. */
    public ConnectionStateChangedEventArgs(ConnectionState previousState, ConnectionState currentState) {
        this(previousState, currentState, null, null);
    }

    /** Creates state-change data with a message. */
    public ConnectionStateChangedEventArgs(
            ConnectionState previousState, ConnectionState currentState, String message) {
        this(previousState, currentState, message, null);
    }

    /** Creates state-change data. */
    public ConnectionStateChangedEventArgs(
            ConnectionState previousState, ConnectionState currentState, String message, Exception exception) {
        this.previousState = previousState;
        this.currentState = currentState;
        this.message = message;
        this.exception = exception;
    }

    public ConnectionState getCurrentState() {
        return currentState;
    }

    public Exception getException() {
        return exception;
    }

    public String getMessage() {
        return message;
    }

    public ConnectionState getPreviousState() {
        return previousState;
    }
}
