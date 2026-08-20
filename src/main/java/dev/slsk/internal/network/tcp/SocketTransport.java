// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

import java.io.IOException;

/** Exchanges bytes over an established socket. */
public interface SocketTransport extends AutoCloseable {
    /**
     * Reads up to {@code size} bytes, blocking until some arrive.
     *
     * <p>Callers are already on a virtual thread, so blocking here costs
     * nothing while parked. This replaced a {@code CompletableFuture}-returning
     * pair that dispatched every read onto a <em>second</em> virtual thread and
     * then blocked the caller on {@code get()} — two threads and a future per
     * 16 KiB, for work the caller could do itself.
     *
     * @param buffer the destination buffer
     * @param offset the offset to write at
     * @param size the maximum number of bytes to read
     * @return the number of bytes read, or {@code 0} at end of stream
     * @throws java.net.SocketTimeoutException when the read timeout elapses
     *     with no data; the stream stays usable and no bytes are lost
     * @throws IOException on any other transport failure
     */
    int read(byte[] buffer, int offset, int size) throws IOException;

    /**
     * Writes {@code size} bytes, blocking until they are handed to the
     * transport.
     *
     * @param buffer the source buffer
     * @param offset the offset to read from
     * @param size the number of bytes to write
     * @throws IOException on transport failure
     */
    void write(byte[] buffer, int offset, int size) throws IOException;

    /** Closes the transport. */
    @Override
    void close() throws IOException;
}
