// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/** Represents errors that occur while listening for peer connections. */
public class ListenException extends SoulseekClientException {
    private static final long serialVersionUID = 1L;

    public ListenException() {
        super();
    }

    public ListenException(String message) {
        super(message);
    }

    public ListenException(String message, Throwable cause) {
        super(message, cause);
    }
}
