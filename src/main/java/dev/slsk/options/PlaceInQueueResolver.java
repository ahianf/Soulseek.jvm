// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.options;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

/** Resolves the place in queue for an incoming request. */
@FunctionalInterface
public interface PlaceInQueueResolver {
    /**
     * Resolves the place in queue.
     *
     * @param username the requesting username
     * @param endPoint the requesting endpoint
     * @param filename the requested filename
     * @return the nullable queue position
     */
    CompletableFuture<Integer> resolve(String username, InetSocketAddress endPoint, String filename);
}
