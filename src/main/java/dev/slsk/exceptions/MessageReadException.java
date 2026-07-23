// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/** Represents errors reading a protocol message. */
public class MessageReadException extends MessageException {
    private static final long serialVersionUID = 1L;

    public MessageReadException() {
        super();
    }

    public MessageReadException(String message) {
        super(message);
    }

    public MessageReadException(String message, Throwable cause) {
        super(message, cause);
    }
}
