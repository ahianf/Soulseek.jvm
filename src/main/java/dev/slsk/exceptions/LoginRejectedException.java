// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/** Indicates that the server rejected a login request. */
public class LoginRejectedException extends SoulseekClientException {
    private static final long serialVersionUID = 1L;

    public LoginRejectedException() {
        super();
    }

    public LoginRejectedException(String message) {
        super(message);
    }

    public LoginRejectedException(String message, Throwable cause) {
        super(message, cause);
    }
}
