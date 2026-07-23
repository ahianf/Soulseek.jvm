// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/** Represents errors involving the user endpoint cache. */
public class UserEndPointCacheException extends UserEndPointException {
    private static final long serialVersionUID = 1L;

    public UserEndPointCacheException() {
        super();
    }

    public UserEndPointCacheException(String message) {
        super(message);
    }

    public UserEndPointCacheException(String message, Throwable cause) {
        super(message, cause);
    }
}
