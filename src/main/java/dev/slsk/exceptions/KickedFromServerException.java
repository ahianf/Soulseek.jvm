// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/** Indicates that the server disconnected the authenticated user. */
public class KickedFromServerException extends SoulseekClientException {
    private static final long serialVersionUID = 1L;

    public KickedFromServerException() {
        super();
    }

    public KickedFromServerException(String message) {
        super(message);
    }

    public KickedFromServerException(String message, Throwable cause) {
        super(message, cause);
    }
}
