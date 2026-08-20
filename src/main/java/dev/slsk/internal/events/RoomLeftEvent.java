// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.internal.messaging.messages.UserLeftRoomNotification;

/** Event payload emitted when a user leaves a chat room. */
public record RoomLeftEvent(String roomName, String username) implements SoulseekClientEvent {

    /**
     * Creates event payload from an internal protocol notification.
     *
     * @param notification the notification that raised the event
     */
    public RoomLeftEvent(UserLeftRoomNotification notification) {
        this(notification.getRoomName(), notification.getUsername());
    }
}
