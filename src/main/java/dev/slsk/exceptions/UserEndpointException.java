// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/** Represents errors resolving a user's network endpoint. */
public class UserEndpointException extends SoulseekClientException {
    private static final long serialVersionUID = 1L;

    public UserEndpointException() {
        super();
    }

    public UserEndpointException(String message) {
        super(message);
    }

    public UserEndpointException(String message, Throwable cause) {
        super(message, cause);
    }
}
