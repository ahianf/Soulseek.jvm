// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;

/** Reports an unsuccessful peer-connection attempt. */
public final class CannotConnect implements IncomingMessage, OutgoingMessage {

    private final int token;
    private final String username;

    /** Creates a token-only cannot-connect message. */
    public CannotConnect(int token) {
        this(token, null);
    }

    /** Creates a cannot-connect message. */
    public CannotConnect(int token, String username) {
        this.token = token;
        this.username = username;
    }

    public int getToken() {
        return token;
    }

    public String getUsername() {
        return username;
    }

    /** Parses a cannot-connect message. */
    public static CannotConnect fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader =
                ServerMessageParser.reader(bytes, MessageCode.Server.CANNOT_CONNECT, "CannotConnect");
        int token = reader.readInteger();
        String username = reader.hasMoreData() ? reader.readString() : null;
        return new CannotConnect(token, username);
    }

    /** Serializes this cannot-connect message. */
    @Override
    public byte[] toByteArray() {
        MessageBuilder builder = new MessageBuilder()
                .writeCode(MessageCode.Server.CANNOT_CONNECT)
                .writeInteger(token);
        if (username != null && !username.isEmpty()) {
            builder.writeString(username);
        }
        return builder.build();
    }
}
