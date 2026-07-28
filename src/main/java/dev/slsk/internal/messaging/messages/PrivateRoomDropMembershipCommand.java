// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;

/** Drops membership in a private room. */
public final class PrivateRoomDropMembershipCommand extends StringServerMessage {
    public PrivateRoomDropMembershipCommand(String roomName) {
        super(MessageCode.Server.PRIVATE_ROOM_DROP_MEMBERSHIP, roomName);
    }

    public String getRoomName() {
        return value();
    }
}
