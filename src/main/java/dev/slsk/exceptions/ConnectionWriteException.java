// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/** Represents errors while writing to a connection. */
public class ConnectionWriteException extends ConnectionException {
    private static final long serialVersionUID = 1L;

    public ConnectionWriteException() {
        super();
    }

    public ConnectionWriteException(String message) {
        super(message);
    }

    public ConnectionWriteException(String message, Throwable cause) {
        super(message, cause);
    }
}
