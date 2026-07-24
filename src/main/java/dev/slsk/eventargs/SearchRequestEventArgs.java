// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

/**
 * Event arguments for an incoming search request.
 */
public class SearchRequestEventArgs extends SoulseekClientEventArgs {
    private final String query;
    private final int token;
    private final String username;

    /**
     * Creates search-request event arguments.
     *
     * @param username the requesting username
     * @param token the request token
     * @param query the search query text
     */
    public SearchRequestEventArgs(String username, int token, String query) {
        this.username = username;
        this.token = token;
        this.query = query;
    }

    /**
     * Returns the query text.
     *
     * @return the query text
     */
    public final String getQuery() {
        return query;
    }

    /**
     * Returns the request token.
     *
     * @return the token
     */
    public final int getToken() {
        return token;
    }

    /**
     * Returns the requesting username.
     *
     * @return the username
     */
    public final String getUsername() {
        return username;
    }
}
