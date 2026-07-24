// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

import dev.slsk.messaging.messages.PrivateMessageNotification;
import java.time.Instant;

/**
 * Event arguments raised when a private message is received.
 */
public class PrivateMessageReceivedEvent extends SoulseekClientEvent {
    private final int id;
    private final String message;
    private final boolean replayed;
    private final Instant timestamp;
    private final String username;

    /**
     * Creates private-message event payload.
     *
     * @param id the unique message identifier
     * @param timestamp the UTC time at which the message was sent
     * @param username the user who sent the message
     * @param message the message content
     * @param replayed whether the message was replayed from an earlier time
     */
    public PrivateMessageReceivedEvent(int id, Instant timestamp, String username, String message, boolean replayed) {
        this.id = id;
        this.timestamp = timestamp;
        this.username = username;
        this.message = message;
        this.replayed = replayed;
    }

    /**
     * Creates event payload from an internal protocol notification.
     *
     * @param notification the notification that raised the event
     */
    public PrivateMessageReceivedEvent(PrivateMessageNotification notification) {
        this(
                notification.getId(),
                notification.getTimestamp(),
                notification.getUsername(),
                notification.getMessage(),
                notification.isReplayed());
    }

    /**
     * Returns the unique message identifier.
     *
     * @return the identifier
     */
    public final int getId() {
        return id;
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
     * Returns whether the message was replayed.
     *
     * @return {@code true} when replayed
     */
    public final boolean isReplayed() {
        return replayed;
    }

    /**
     * Returns the UTC time at which the message was sent.
     *
     * @return the timestamp
     */
    public final Instant getTimestamp() {
        return timestamp;
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
