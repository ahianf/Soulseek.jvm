// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

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
     * Returns the unique connection token.
     *
     * @return the connection token
     */
    public final int getToken() {
        return token;
    }
}
