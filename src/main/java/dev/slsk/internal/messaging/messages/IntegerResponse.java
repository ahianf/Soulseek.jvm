// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageReader;
import dev.slsk.internal.messaging.ProtocolCode;

/** Parses a single-integer response without validating its message code. */
public final class IntegerResponse implements IncomingMessage {
    private IntegerResponse() {}

    /**
     * Parses the first payload integer using the specified code width.
     *
     * @param bytes the framed message
     * @param codeType the message-code enum type
     * @return the payload integer
     */
    public static <T extends Enum<T> & ProtocolCode> int fromByteArray(byte[] bytes, Class<T> codeType) {
        return new MessageReader<>(bytes, codeType).readInteger();
    }
}
