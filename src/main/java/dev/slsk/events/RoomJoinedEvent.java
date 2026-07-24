// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

import dev.slsk.UserData;
import dev.slsk.messaging.messages.UserJoinedRoomNotification;

/**
 * Event arguments raised when a user joins a chat room.
 */
public class RoomJoinedEvent extends RoomEvent {
    private final UserData userData;

    /**
     * Creates room-joined event payload.
     *
     * @param roomName the room in which the event took place
     * @param username the user who joined
     * @param userData the user's data
     */
    public RoomJoinedEvent(String roomName, String username, UserData userData) {
        super(roomName, username);
        this.userData = userData;
    }

    /**
     * Creates event payload from an internal protocol notification.
     *
     * @param notification the notification that raised the event
     */
    public RoomJoinedEvent(UserJoinedRoomNotification notification) {
        this(notification.getRoomName(), notification.getUsername(), notification.getUserData());
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
