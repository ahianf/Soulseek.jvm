// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;

/** Asynchronously creates an output stream for a download. */
@FunctionalInterface
public interface DownloadStreamFactory {
    /**
     * Creates the stream that will receive downloaded data.
     *
     * @return a future containing the output stream
     */
    CompletableFuture<OutputStream> openAsync();
}
