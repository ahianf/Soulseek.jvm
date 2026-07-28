// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.internal.messaging.messages.PublicChatMessageNotification;

/**
 * Event arguments raised when a public chat message is received.
 */
public class PublicChatMessageReceivedEvent extends SoulseekClientEvent {
    private final String message;
    private final String roomName;
    private final String username;

    /**
     * Creates public-chat event payload.
     *
     * @param roomName the room in which the message was sent
     * @param username the user who sent the message
     * @param message the message content
     */
    public PublicChatMessageReceivedEvent(String roomName, String username, String message) {
        this.roomName = roomName;
        this.username = username;
        this.message = message;
    }

    /**
     * Creates event payload from an internal protocol notification.
     *
     * @param notification the notification that raised the event
     */
    public PublicChatMessageReceivedEvent(PublicChatMessageNotification notification) {
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

    /**
     * Returns the room in which the message was sent.
     *
     * @return the room name
     */
    public final String getRoomName() {
        return roomName;
    }

    /**
     * Returns the user who sent the message.
     *
     * @return the username
     */
    public final String getUsername() {
        return username;
    }
}
