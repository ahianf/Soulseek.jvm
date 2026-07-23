// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/** Represents errors involving a transfer data stream. */
public class TransferStreamException extends SoulseekClientException {
    private static final long serialVersionUID = 1L;

    public TransferStreamException() {
        super();
    }

    public TransferStreamException(String message) {
        super(message);
    }

    public TransferStreamException(String message, Throwable cause) {
        super(message, cause);
    }
}
