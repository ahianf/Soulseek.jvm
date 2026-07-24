// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.exceptions.MessageException;
import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;

/** A distributed ping request. */
public final class DistributedPingRequest implements IncomingMessage, OutgoingMessage {
    /** Creates a ping request. */
    public DistributedPingRequest() {}

    /**
     * Parses a ping request.
     *
     * @param bytes the framed message
     * @return the parsed message
     */
    public static DistributedPingRequest fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Distributed> reader = new MessageReader<>(bytes, MessageCode.Distributed.class);
        MessageCode.Distributed code = reader.readCode();
        if (code != MessageCode.Distributed.PING) {
            throw new MessageException("Message Code mismatch creating DistributedPingRequest "
                    + "(expected: 0, received: " + code.getValue() + ")");
        }

        return new DistributedPingRequest();
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder().writeCode(MessageCode.Distributed.PING).build();
    }
}
