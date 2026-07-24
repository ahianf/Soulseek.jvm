// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

import dev.slsk.messaging.messages.CannotConnect;

/**
 * Event arguments raised when a user reports that they cannot connect.
 */
public class UserCannotConnectEventArgs extends UserEventArgs {
    private final int token;

    /**
     * Creates cannot-connect event arguments.
     *
     * @param token the unique connection token
     * @param username the associated username
     */
    public UserCannotConnectEventArgs(int token, String username) {
        super(username);
        this.token = token;
    }

    /**
     * Creates event arguments from an internal protocol message.
     *
     * @param cannotConnect the message that raised the event
     */
    public UserCannotConnectEventArgs(CannotConnect cannotConnect) {
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
