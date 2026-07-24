// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageCode;

/** Leaves a chat room. */
public final class LeaveRoomRequest extends StringServerMessage {
    public LeaveRoomRequest(String roomName) {
        super(MessageCode.Server.LEAVE_ROOM, roomName);
    }

    public String getRoomName() {
        return value();
    }
}
