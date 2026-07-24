// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageCode;

/** Removes an operator from a private chat room. */
public final class PrivateRoomRemoveOperator extends PrivateRoomUserMessage {

    /** Creates a remove-operator message. */
    public PrivateRoomRemoveOperator(String roomName, String username) {
        super(MessageCode.Server.PRIVATE_ROOM_REMOVE_OPERATOR, roomName, username);
    }

    /** Parses a remove-operator message. */
    public static PrivateRoomRemoveOperator fromByteArray(byte[] bytes) {
        Fields fields = parse(bytes, MessageCode.Server.PRIVATE_ROOM_REMOVE_OPERATOR, "PrivateRoomRemoveOperator");
        return new PrivateRoomRemoveOperator(fields.roomName(), fields.username());
    }
}
