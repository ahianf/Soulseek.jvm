// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

/**
 * Base event payload for chat-room ticker events.
 */
public abstract class RoomTickerEvent extends SoulseekClientEvent {
    private final String roomName;

    /**
     * Creates room-ticker event payload.
     *
     * @param roomName the associated chat room
     */
    protected RoomTickerEvent(String roomName) {
        this.roomName = roomName;
    }

    /**
     * Returns the associated chat room.
     *
     * @return the room name
     */
    public final String getRoomName() {
        return roomName;
    }
}
