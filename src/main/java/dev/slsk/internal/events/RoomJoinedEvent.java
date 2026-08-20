// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.internal.messaging.messages.UserJoinedRoomNotification;
import dev.slsk.internal.user.UserData;

/** Event payload emitted when a user joins a chat room. */
public record RoomJoinedEvent(String roomName, String username, UserData userData) implements SoulseekClientEvent {

    /**
     * Creates event payload from an internal protocol notification.
     *
     * @param notification the notification that raised the event
     */
    public RoomJoinedEvent(UserJoinedRoomNotification notification) {
        this(notification.getRoomName(), notification.getUsername(), notification.getUserData());
    }
}
