// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/** Represents errors involving the user endpoint cache. */
public class UserEndpointCacheException extends UserEndpointException {
    private static final long serialVersionUID = 1L;

    public UserEndpointCacheException() {
        super();
    }

    public UserEndpointCacheException(String message) {
        super(message);
    }

    public UserEndpointCacheException(String message, Throwable cause) {
        super(message, cause);
    }
}
