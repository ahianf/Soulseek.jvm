// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/** Represents an attempt to reuse an active protocol token. */
public class DuplicateTokenException extends SoulseekClientException {
    private static final long serialVersionUID = 1L;

    public DuplicateTokenException() {
        super();
    }

    public DuplicateTokenException(String message) {
        super(message);
    }

    public DuplicateTokenException(String message, Throwable cause) {
        super(message, cause);
    }
}
