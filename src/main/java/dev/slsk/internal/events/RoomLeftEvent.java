// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.internal.messaging.messages.UserLeftRoomNotification;

/**
 * Event arguments raised when a user leaves a chat room.
 */
public class RoomLeftEvent extends RoomEvent {
    /**
     * Creates room-left event payload.
     *
     * @param roomName the room in which the event took place
     * @param username the user who left
     */
    public RoomLeftEvent(String roomName, String username) {
        super(roomName, username);
    }

    /**
     * Creates event payload from an internal protocol notification.
     *
     * @param notification the notification that raised the event
     */
    public RoomLeftEvent(UserLeftRoomNotification notification) {
        this(notification.getRoomName(), notification.getUsername());
    }
}
