// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.options;

import dev.slsk.BrowseResponse;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

/** Resolves a response to an incoming browse request. */
@FunctionalInterface
public interface BrowseResponseResolver {
    /**
     * Resolves a browse response.
     *
     * @param username the requesting username
     * @param endpoint the requesting endpoint
     * @return the asynchronous response
     */
    CompletableFuture<BrowseResponse> resolve(String username, InetSocketAddress endpoint);
}
