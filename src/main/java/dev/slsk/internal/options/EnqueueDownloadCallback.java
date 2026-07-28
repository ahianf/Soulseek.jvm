// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

/** Handles an incoming queue-download request. */
@FunctionalInterface
public interface EnqueueDownloadCallback {
    /**
     * Enqueues or rejects a download.
     *
     * @param username the requesting username
     * @param endpoint the requesting endpoint
     * @param filename the requested filename
     * @return completion of the enqueue operation
     */
    CompletableFuture<Void> enqueue(String username, InetSocketAddress endpoint, String filename);
}
