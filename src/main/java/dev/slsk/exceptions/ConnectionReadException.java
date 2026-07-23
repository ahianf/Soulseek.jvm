// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/** Represents errors while reading from a connection. */
public class ConnectionReadException extends ConnectionException {
    private static final long serialVersionUID = 1L;

    public ConnectionReadException() {
        super();
    }

    public ConnectionReadException(String message) {
        super(message);
    }

    public ConnectionReadException(String message, Throwable cause) {
        super(message, cause);
    }
}
