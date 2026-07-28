// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import java.net.InetSocketAddress;

/** A cache for user endpoints. */
public interface UserEndpointCache {
    /**
     * Attempts to fetch the endpoint cached for a username.
     *
     * @param username the username
     * @return a result containing the cached endpoint when present
     */
    CacheLookupResult<InetSocketAddress> lookup(String username);

    /**
     * Adds or updates the endpoint cached for a username.
     *
     * @param username the username
     * @param endpoint the endpoint to cache
     */
    void put(String username, InetSocketAddress endpoint);
}
