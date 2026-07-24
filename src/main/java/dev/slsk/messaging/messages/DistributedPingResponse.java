// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.exceptions.MessageException;
import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;

/** A distributed ping response. */
public final class DistributedPingResponse implements IncomingMessage, OutgoingMessage {
    private final int token;

    /** Creates a ping response. */
    public DistributedPingResponse(int token) {
        this.token = token;
    }

    /** Returns the response token. */
    public int getToken() {
        return token;
    }

    /**
     * Parses a ping response.
     *
     * @param bytes the framed message
     * @return the parsed message
     */
    public static DistributedPingResponse fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Distributed> reader = new MessageReader<>(bytes, MessageCode.Distributed.class);
        MessageCode.Distributed code = reader.readCode();
        if (code != MessageCode.Distributed.PING) {
            throw new MessageException("Message Code mismatch creating DistributedPingResponse "
                    + "(expected: 0, received: " + code.getValue() + ")");
        }

        int parsedToken = reader.hasMoreData() ? reader.readInteger() : 0;
        return new DistributedPingResponse(parsedToken);
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Distributed.PING)
                .writeInteger(token)
                .build();
    }
}
