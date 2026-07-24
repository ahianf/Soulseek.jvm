// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;

/** Shared data and serialization for private-room user/operator changes. */
abstract class PrivateRoomUserMessage implements IncomingMessage, OutgoingMessage {

    private final MessageCode.Server code;
    private final String roomName;
    private final String username;

    PrivateRoomUserMessage(MessageCode.Server code, String roomName, String username) {
        this.code = code;
        this.roomName = roomName;
        this.username = username;
    }

    public final String getRoomName() {
        return roomName;
    }

    public final String getUsername() {
        return username;
    }

    @Override
    public final byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(code)
                .writeString(roomName)
                .writeString(username)
                .build();
    }

    static Fields parse(byte[] bytes, MessageCode.Server code, String messageName) {
        MessageReader<MessageCode.Server> reader = ServerMessageParser.reader(bytes, code, messageName);
        return new Fields(reader.readString(), reader.readString());
    }

    record Fields(String roomName, String username) {}
}
