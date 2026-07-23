// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/** Indicates that a remote peer reported a transfer failure. */
public class TransferReportedFailedException extends SoulseekClientException {
    private static final long serialVersionUID = 1L;

    public TransferReportedFailedException() {
        super();
    }

    public TransferReportedFailedException(String message) {
        super(message);
    }

    public TransferReportedFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
