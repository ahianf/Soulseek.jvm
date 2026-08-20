// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.internal.messaging.messages.RoomMessageNotification;

/** Event payload emitted when a chat-room message is received. */
public record RoomMessageReceivedEvent(String roomName, String username, String message)
        implements SoulseekClientEvent {

    /**
     * Creates event payload from an internal protocol notification.
     *
     * @param notification the notification that raised the event
     */
    public RoomMessageReceivedEvent(RoomMessageNotification notification) {
        this(notification.getRoomName(), notification.getUsername(), notification.getMessage());
    }
}
