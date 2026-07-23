// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/** Represents errors involving network addresses. */
public class AddressException extends SoulseekClientException {
    private static final long serialVersionUID = 1L;

    public AddressException() {
        super();
    }

    public AddressException(String message) {
        super(message);
    }

    public AddressException(String message, Throwable cause) {
        super(message, cause);
    }
}
