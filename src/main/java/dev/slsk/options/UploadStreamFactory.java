// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.options;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

/** Asynchronously creates an input stream for an upload. */
@FunctionalInterface
public interface UploadStreamFactory {
    /**
     * Creates the stream from which upload data is read.
     *
     * @param startOffset the offset requested by the remote peer
     * @return a future containing the input stream
     */
    CompletableFuture<InputStream> openAsync(long startOffset);
}
