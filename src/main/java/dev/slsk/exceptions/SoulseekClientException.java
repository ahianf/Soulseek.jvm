// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/**
 * Represents errors that occur during Soulseek client operations.
 *
 * <p>This is unchecked because C# exceptions do not participate in method
 * signatures and a checked base type would change every Java call site.</p>
 */
public class SoulseekClientException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public SoulseekClientException() {
        super();
    }

    public SoulseekClientException(String message) {
        super(message);
    }

    public SoulseekClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
