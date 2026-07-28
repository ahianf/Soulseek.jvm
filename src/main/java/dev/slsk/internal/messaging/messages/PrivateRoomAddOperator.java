// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;

/** Adds an operator to a private chat room. */
public final class PrivateRoomAddOperator extends PrivateRoomUserMessage {

    /** Creates an add-operator message. */
    public PrivateRoomAddOperator(String roomName, String username) {
        super(MessageCode.Server.PRIVATE_ROOM_ADD_OPERATOR, roomName, username);
    }

    /** Parses an add-operator message. */
    public static PrivateRoomAddOperator fromByteArray(byte[] bytes) {
        Fields fields = parse(bytes, MessageCode.Server.PRIVATE_ROOM_ADD_OPERATOR, "PrivateRoomAddOperator");
        return new PrivateRoomAddOperator(fields.roomName(), fields.username());
    }
}
