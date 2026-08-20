// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.internal.messaging.messages.PrivateMessageNotification;
import java.time.Instant;

/** Event payload emitted when a private message is received. */
public record PrivateMessageReceivedEvent(int id, Instant timestamp, String username, String message, boolean replayed)
        implements SoulseekClientEvent {

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
}
