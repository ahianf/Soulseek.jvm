// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.internal.messaging.messages.RoomMessageNotification;

/**
 * Event arguments raised when a chat-room message is received.
 */
public class RoomMessageReceivedEvent extends RoomEvent {
    private final String message;

    /**
     * Creates room-message event payload.
     *
     * @param roomName the room in which the message was sent
     * @param username the user who sent the message
     * @param message the message content
     */
    public RoomMessageReceivedEvent(String roomName, String username, String message) {
        super(roomName, username);
        this.message = message;
    }

    /**
     * Creates event payload from an internal protocol notification.
     *
     * @param notification the notification that raised the event
     */
    public RoomMessageReceivedEvent(RoomMessageNotification notification) {
        this(notification.getRoomName(), notification.getUsername(), notification.getMessage());
    }

    /**
     * Returns the message content.
     *
     * @return the message
     */
    public final String getMessage() {
        return message;
    }
}
