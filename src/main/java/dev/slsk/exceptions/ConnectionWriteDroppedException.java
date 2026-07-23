// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/** Indicates that a connection write was dropped. */
public class ConnectionWriteDroppedException extends ConnectionException {
    private static final long serialVersionUID = 1L;

    public ConnectionWriteDroppedException() {
        super();
    }

    public ConnectionWriteDroppedException(String message) {
        super(message);
    }

    public ConnectionWriteDroppedException(String message, Throwable cause) {
        super(message, cause);
    }
}
