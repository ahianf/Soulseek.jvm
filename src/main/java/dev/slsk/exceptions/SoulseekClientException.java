// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/**
 * Represents errors that occur during Soulseek client operations.
 *
 * <p>This is unchecked because it is the common base for both programming
 * errors and operational failures; forcing every subtype into every method
 * signature would obscure the recoverable exceptions those methods declare.
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
