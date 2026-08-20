// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.internal.messaging.messages.PublicChatMessageNotification;

/** Event payload emitted when a public chat message is received. */
public record PublicChatMessageReceivedEvent(String roomName, String username, String message)
        implements SoulseekClientEvent {

    /**
     * Creates event payload from an internal protocol notification.
     *
     * @param notification the notification that raised the event
     */
    public PublicChatMessageReceivedEvent(PublicChatMessageNotification notification) {
        this(notification.getRoomName(), notification.getUsername(), notification.getMessage());
    }
}
