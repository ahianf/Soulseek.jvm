// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.internal.messaging.messages.CannotConnect;

/**
 * Event arguments raised when a user reports that they cannot connect.
 */
public class UserCannotConnectEvent extends UserEvent {
    private final int token;

    /**
     * Creates cannot-connect event payload.
     *
     * @param token the unique connection token
     * @param username the associated username
     */
    public UserCannotConnectEvent(int token, String username) {
        super(username);
        this.token = token;
    }

    /**
     * Creates event payload from an internal protocol message.
     *
     * @param cannotConnect the message that raised the event
     */
    public UserCannotConnectEvent(CannotConnect cannotConnect) {
        this(cannotConnect.getToken(), cannotConnect.getUsername());
    }

    /**
     * Returns the unique connection token.
     *
     * @return the connection token
     */
    public final int getToken() {
        return token;
    }
}
