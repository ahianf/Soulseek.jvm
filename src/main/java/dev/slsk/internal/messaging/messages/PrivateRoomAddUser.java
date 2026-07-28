// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;

/** Adds a member to a private chat room. */
public final class PrivateRoomAddUser extends PrivateRoomUserMessage {
    /** Creates an add-user message. */
    public PrivateRoomAddUser(String roomName, String username) {
        super(MessageCode.Server.PRIVATE_ROOM_ADD_USER, roomName, username);
    }

    /** Parses an add-user message. */
    public static PrivateRoomAddUser fromByteArray(byte[] bytes) {
        Fields fields = parse(bytes, MessageCode.Server.PRIVATE_ROOM_ADD_USER, "PrivateRoomAddUser");
        return new PrivateRoomAddUser(fields.roomName(), fields.username());
    }
}
