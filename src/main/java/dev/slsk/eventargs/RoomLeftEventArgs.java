// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

import dev.slsk.messaging.messages.UserLeftRoomNotification;

/**
 * Event arguments raised when a user leaves a chat room.
 */
public class RoomLeftEventArgs extends RoomEventArgs {
    /**
     * Creates room-left event arguments.
     *
     * @param roomName the room in which the event took place
     * @param username the user who left
     */
    public RoomLeftEventArgs(String roomName, String username) {
        super(roomName, username);
    }

    /**
     * Creates event arguments from an internal protocol notification.
     *
     * @param notification the notification that raised the event
     */
    public RoomLeftEventArgs(UserLeftRoomNotification notification) {
        this(notification.getRoomName(), notification.getUsername());
    }
}
