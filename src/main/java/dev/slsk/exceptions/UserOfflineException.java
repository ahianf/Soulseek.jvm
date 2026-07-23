// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/** Indicates that an operation requires a user who is currently offline. */
public class UserOfflineException extends SoulseekClientException {
    private static final long serialVersionUID = 1L;

    public UserOfflineException() {
        super();
    }

    public UserOfflineException(String message) {
        super(message);
    }

    public UserOfflineException(String message, Throwable cause) {
        super(message, cause);
    }
}
