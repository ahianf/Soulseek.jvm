// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network.tcp;

/** Data describing a TCP disconnection. */
public final class ConnectionDisconnectedEvent extends ConnectionEvent {

    private final Exception exception;
    private final String message;

    /** Creates a disconnection without an exception. */
    public ConnectionDisconnectedEvent(String message) {
        this(message, null);
    }

    /** Creates disconnection event data. */
    public ConnectionDisconnectedEvent(String message, Exception exception) {
        this.message = message;
        this.exception = exception;
    }

    public Exception getException() {
        return exception;
    }

    public String getMessage() {
        return message;
    }
}
