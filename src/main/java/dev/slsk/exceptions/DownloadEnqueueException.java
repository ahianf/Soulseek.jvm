// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/** Represents errors that occur while enqueueing a download. */
public class DownloadEnqueueException extends SoulseekClientException {
    private static final long serialVersionUID = 1L;

    public DownloadEnqueueException() {
        super();
    }

    public DownloadEnqueueException(String message) {
        super(message);
    }

    public DownloadEnqueueException(String message, Throwable cause) {
        super(message, cause);
    }
}
