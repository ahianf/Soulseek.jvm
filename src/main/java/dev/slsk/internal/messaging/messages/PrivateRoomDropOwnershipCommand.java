// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;

/** Drops ownership of a private room. */
public final class PrivateRoomDropOwnershipCommand extends StringServerMessage {
    public PrivateRoomDropOwnershipCommand(String roomName) {
        super(MessageCode.Server.PRIVATE_ROOM_DROP_OWNERSHIP, roomName);
    }

    public String getRoomName() {
        return value();
    }
}
