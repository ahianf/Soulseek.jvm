// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network.tcp;

import dev.slsk.CancellationSignal;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** Provides the underlying stream of data for network access. */
public interface NetworkStream extends AutoCloseable {
    /** Returns the read timeout in milliseconds. */
    int getReadTimeout() throws IOException;

    /** Sets the read timeout in milliseconds. */
    void setReadTimeout(int timeout) throws IOException;

    /** Returns the write timeout in milliseconds. */
    int getWriteTimeout();

    /** Sets the write timeout in milliseconds. */
    void setWriteTimeout(int timeout);

    /** Reads up to {@code size} bytes asynchronously. */
    CompletableFuture<Integer> readAsync(byte[] buffer, int offset, int size, CancellationSignal cancellationSignal);

    /** Writes {@code size} bytes asynchronously. */
    CompletableFuture<Void> writeAsync(byte[] buffer, int offset, int size, CancellationSignal cancellationSignal);

    /** Closes the stream. */
    @Override
    void close() throws IOException;
}
