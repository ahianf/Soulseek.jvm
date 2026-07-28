// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.exceptions.MessageException;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

/**
 * A server message intended for forwarding to the distributed network.
 */
public final class EmbeddedMessage implements IncomingMessage {
    private final MessageCode.Distributed distributedCode;
    private final byte[] distributedMessage;

    /** Creates an embedded distributed message. */
    public EmbeddedMessage(MessageCode.Distributed distributedCode, byte[] distributedMessage) {
        this.distributedCode = Objects.requireNonNull(distributedCode, "distributedCode");
        this.distributedMessage = distributedMessage;
    }

    /** Returns the embedded distributed message code. */
    public MessageCode.Distributed getDistributedCode() {
        return distributedCode;
    }

    /**
     * Returns the original embedded framed-message array.
     *
     * <p>The source exposes its original mutable array, so this port does
     * likewise.</p>
     */
    public byte[] getDistributedMessage() {
        return distributedMessage;
    }

    /** Parses a server embedded-message frame. */
    public static EmbeddedMessage fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader = new MessageReader<>(bytes, MessageCode.Server.class);
        int rawCode = ByteBuffer.wrap(bytes, 4, Integer.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt();
        if (rawCode != MessageCode.Server.EMBEDDED_MESSAGE.getValue()) {
            throw new MessageException(
                    "Message Code mismatch creating EmbeddedMessage " + "(expected: 93, received: " + rawCode + ")");
        }

        MessageCode.Distributed code = MessageCode.Distributed.fromValue(reader.readByte());
        byte[] distributedMessage = new MessageBuilder()
                .writeCode(code)
                .writeBytes(Arrays.copyOfRange(bytes, 9, bytes.length))
                .build();
        return new EmbeddedMessage(code, distributedMessage);
    }
}
