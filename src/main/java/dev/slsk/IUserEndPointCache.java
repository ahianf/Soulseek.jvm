// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.net.InetSocketAddress;

/** A cache for user endpoints. */
public interface IUserEndPointCache {
    /**
     * Attempts to fetch the endpoint cached for a username.
     *
     * @param username the username
     * @return a result containing the cached endpoint when present
     */
    CacheLookupResult<InetSocketAddress> tryGet(String username);

    /**
     * Adds or updates the endpoint cached for a username.
     *
     * @param username the username
     * @param endPoint the endpoint to cache
     */
    void addOrUpdate(String username, InetSocketAddress endPoint);
}
