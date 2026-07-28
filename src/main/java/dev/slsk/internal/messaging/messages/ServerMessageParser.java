// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.exceptions.MessageException;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Shared code validation for incoming server messages. */
final class ServerMessageParser {
    private ServerMessageParser() {}

    static MessageReader<MessageCode.Server> reader(byte[] bytes, MessageCode.Server expected, String messageName) {
        return reader(bytes, expected, messageName, true);
    }

    static MessageReader<MessageCode.Server> reader(
            byte[] bytes, MessageCode.Server expected, String messageName, boolean closeParenthesis) {
        MessageReader<MessageCode.Server> reader = new MessageReader<>(bytes, MessageCode.Server.class);
        int received = ByteBuffer.wrap(bytes, 4, Integer.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt();
        if (received != expected.getValue()) {
            throw new MessageException("Message Code mismatch creating " + messageName
                    + " (expected: " + expected.getValue()
                    + ", received: " + received
                    + (closeParenthesis ? ")" : ""));
        }
        return reader;
    }
}
