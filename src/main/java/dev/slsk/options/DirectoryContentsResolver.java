// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.options;

import dev.slsk.Directory;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

/** Resolves directory contents for an incoming folder request. */
@FunctionalInterface
public interface DirectoryContentsResolver {
    /**
     * Resolves directory contents.
     *
     * @param username the requesting username
     * @param endPoint the requesting endpoint
     * @param token the request token
     * @param directoryName the requested directory name
     * @return the asynchronous directory sequence
     */
    CompletableFuture<Iterable<Directory>> resolve(
            String username, InetSocketAddress endPoint, int token, String directoryName);
}
