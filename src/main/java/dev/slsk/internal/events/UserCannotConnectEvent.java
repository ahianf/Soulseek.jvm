// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.internal.messaging.messages.CannotConnect;

/** Event payload emitted when a user reports that they cannot connect. */
public record UserCannotConnectEvent(int token, String username) implements SoulseekClientEvent {

    /**
     * Creates event payload from an internal protocol message.
     *
     * @param cannotConnect the message that raised the event
     */
    public UserCannotConnectEvent(CannotConnect cannotConnect) {
        this(cannotConnect.getToken(), cannotConnect.getUsername());
    }
}
