// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

/**
 * Base event arguments for chat-room ticker events.
 */
public abstract class RoomTickerEventArgs extends SoulseekClientEventArgs {
    private final String roomName;

    /**
     * Creates room-ticker event arguments.
     *
     * @param roomName the associated chat room
     */
    protected RoomTickerEventArgs(String roomName) {
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
