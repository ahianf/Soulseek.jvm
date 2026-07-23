// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/** Indicates an attempt to create a transfer that already exists. */
public class DuplicateTransferException extends TransferException {
    private static final long serialVersionUID = 1L;

    public DuplicateTransferException() {
        super();
    }

    public DuplicateTransferException(String message) {
        super(message);
    }

    public DuplicateTransferException(String message, Throwable cause) {
        super(message, cause);
    }
}
