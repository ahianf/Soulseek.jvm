// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/** Indicates that a requested transfer was rejected. */
public class TransferRejectedException extends TransferException {
    private static final long serialVersionUID = 1L;

    public TransferRejectedException() {
        super();
    }

    public TransferRejectedException(String message) {
        super(message);
    }

    public TransferRejectedException(String message, Throwable cause) {
        super(message, cause);
    }
}
