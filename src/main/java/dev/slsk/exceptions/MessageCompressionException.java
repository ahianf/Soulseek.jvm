// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/** Represents errors compressing or decompressing protocol messages. */
public class MessageCompressionException extends MessageException {
    private static final long serialVersionUID = 1L;

    public MessageCompressionException() {
        super();
    }

    public MessageCompressionException(String message) {
        super(message);
    }

    public MessageCompressionException(String message, Throwable cause) {
        super(message, cause);
    }
}
