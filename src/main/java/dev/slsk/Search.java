// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.util.Objects;

/**
 * A snapshot of a single file search.
 */
public class Search {
    private final int fileCount;
    private final int lockedFileCount;
    private final SearchQuery query;
    private final int responseCount;
    private final SearchScope scope;
    private final SearchStates state;
    private final int token;

    /**
     * Creates a search snapshot.
     *
     * @param query the search query
     * @param scope the search scope
     * @param token the unique search token
     * @param state the search state
     * @param responseCount the number of responses received
     * @param fileCount the number of files in received responses
     * @param lockedFileCount the number of locked files in received responses
     */
    public Search(
            SearchQuery query,
            SearchScope scope,
            int token,
            SearchStates state,
            int responseCount,
            int fileCount,
            int lockedFileCount) {
        this.query = query;
        this.scope = scope;
        this.token = token;
        this.state = Objects.requireNonNull(state, "state");
        this.responseCount = responseCount;
        this.fileCount = fileCount;
        this.lockedFileCount = lockedFileCount;
    }

    /**
     * Returns the total file count.
     *
     * @return the file count
     */
    public final int getFileCount() {
        return fileCount;
    }

    /**
     * Returns the total locked-file count.
     *
     * @return the locked-file count
     */
    public final int getLockedFileCount() {
        return lockedFileCount;
    }

    /**
     * Returns the search query.
     *
     * @return the search query
     */
    public final SearchQuery getQuery() {
        return query;
    }

    /**
     * Returns the response count.
     *
     * @return the response count
     */
    public final int getResponseCount() {
        return responseCount;
    }

    /**
     * Returns the search scope.
     *
     * @return the search scope
     */
    public final SearchScope getScope() {
        return scope;
    }

    /**
     * Returns the search state.
     *
     * @return the search state
     */
    public final SearchStates getState() {
        return state;
    }

    /**
     * Returns the unique search token.
     *
     * @return the search token
     */
    public final int getToken() {
        return token;
    }
}
