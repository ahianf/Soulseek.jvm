// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageReader;
import dev.slsk.messaging.ProtocolCode;

/** Parses a single-string response without validating its message code. */
public final class StringResponse implements IncomingMessage {
    private StringResponse() {}

    /**
     * Parses the first payload string using the specified code width.
     *
     * @param bytes the framed message
     * @param codeType the message-code enum type
     * @return the payload string
     */
    public static <T extends Enum<T> & ProtocolCode> String fromByteArray(byte[] bytes, Class<T> codeType) {
        return new MessageReader<>(bytes, codeType).readString();
    }
}
