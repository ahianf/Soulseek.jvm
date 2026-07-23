// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/** Indicates that a requested transfer could not be found. */
public class TransferNotFoundException extends TransferException {
    private static final long serialVersionUID = 1L;

    public TransferNotFoundException() {
        super();
    }

    public TransferNotFoundException(String message) {
        super(message);
    }

    public TransferNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
