// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageCode;

/** Removes a member from a private chat room. */
public final class PrivateRoomRemoveUser extends PrivateRoomUserMessage {
    /** Creates a remove-user message. */
    public PrivateRoomRemoveUser(String roomName, String username) {
        super(MessageCode.Server.PRIVATE_ROOM_REMOVE_USER, roomName, username);
    }

    /** Parses a remove-user message. */
    public static PrivateRoomRemoveUser fromByteArray(byte[] bytes) {
        Fields fields = parse(bytes, MessageCode.Server.PRIVATE_ROOM_REMOVE_USER, "PrivateRoomRemoveUser");
        return new PrivateRoomRemoveUser(fields.roomName(), fields.username());
    }
}
