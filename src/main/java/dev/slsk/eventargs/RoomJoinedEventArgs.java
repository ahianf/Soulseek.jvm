// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

import dev.slsk.UserData;

/**
 * Event arguments raised when a user joins a chat room.
 */
public class RoomJoinedEventArgs extends RoomEventArgs {
    private final UserData userData;

    /**
     * Creates room-joined event arguments.
     *
     * @param roomName the room in which the event took place
     * @param username the user who joined
     * @param userData the user's data
     */
    public RoomJoinedEventArgs(String roomName, String username, UserData userData) {
        super(roomName, username);
        this.userData = userData;
    }

    /**
     * Returns the joined user's data.
     *
     * @return the user data
     */
    public final UserData getUserData() {
        return userData;
    }
}
