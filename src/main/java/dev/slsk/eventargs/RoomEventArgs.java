// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

/**
 * Base event arguments for chat-room events.
 */
public abstract class RoomEventArgs extends SoulseekClientEventArgs {
    private final String roomName;
    private final String username;

    /**
     * Creates chat-room event arguments.
     *
     * @param roomName the room in which the event took place
     * @param username the user associated with the event
     */
    protected RoomEventArgs(String roomName, String username) {
        this.roomName = roomName;
        this.username = username;
    }

    /**
     * Returns the room in which the event took place.
     *
     * @return the room name
     */
    public final String getRoomName() {
        return roomName;
    }

    /**
     * Returns the user associated with the event.
     *
     * @return the username
     */
    public final String getUsername() {
        return username;
    }
}
