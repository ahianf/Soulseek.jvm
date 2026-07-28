// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;

/** The response to a request to leave a chat room. */
public final class LeaveRoomResponse implements IncomingMessage {
    private final String roomName;

    /** Creates a leave-room response. */
    public LeaveRoomResponse(String roomName) {
        this.roomName = roomName;
    }

    public String getRoomName() {
        return roomName;
    }

    /** Parses a leave-room response. */
    public static LeaveRoomResponse fromByteArray(byte[] bytes) {
        return new LeaveRoomResponse(
                ServerMessageParser.reader(bytes, MessageCode.Server.LEAVE_ROOM, "LeaveRoomResponse", false)
                        .readString());
    }
}
