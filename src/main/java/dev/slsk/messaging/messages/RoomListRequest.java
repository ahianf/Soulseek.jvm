// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageCode;

/** Requests the list of chat rooms. */
public final class RoomListRequest extends EmptyServerMessage {
    public RoomListRequest() {
        super(MessageCode.Server.ROOM_LIST);
    }
}
