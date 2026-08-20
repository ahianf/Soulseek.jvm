// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Objects;

/**
 * Pass-through implementation of {@link NetworkStream} over a socket.
 */
final class NetworkStreamAdapter implements NetworkStream {
    private final Socket socket;
    private final InputStream inputStream;
    private final OutputStream outputStream;
    private int writeTimeout = -1;

    NetworkStreamAdapter(Socket socket) throws IOException {
        this.socket = Objects.requireNonNull(socket, "socket");
        inputStream = socket.getInputStream();
        outputStream = socket.getOutputStream();
    }

    @Override
    public int getReadTimeout() throws IOException {
        int timeout = socket.getSoTimeout();
        return timeout == 0 ? -1 : timeout;
    }

    @Override
    public void setReadTimeout(int timeout) throws IOException {
        validateTimeout(timeout);
        socket.setSoTimeout(timeout == -1 ? 0 : timeout);
    }

    @Override
    public int getWriteTimeout() {
        return writeTimeout;
    }

    @Override
    public void setWriteTimeout(int timeout) {
        validateTimeout(timeout);
        writeTimeout = timeout;
    }

    @Override
    public int read(byte[] buffer, int offset, int size) throws IOException {
        Objects.requireNonNull(buffer, "buffer");
        Objects.checkFromIndexSize(offset, size, buffer.length);
        int bytesRead = inputStream.read(buffer, offset, size);
        return bytesRead < 0 ? 0 : bytesRead;
    }

    @Override
    public void write(byte[] buffer, int offset, int size) throws IOException {
        Objects.requireNonNull(buffer, "buffer");
        Objects.checkFromIndexSize(offset, size, buffer.length);
        outputStream.write(buffer, offset, size);
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }

    private static void validateTimeout(int timeout) {
        if (timeout <= 0 && timeout != -1) {
            throw new IllegalArgumentException("timeout must be positive or -1: " + timeout);
        }
    }
}
