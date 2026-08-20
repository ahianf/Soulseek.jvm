// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Objects;

/** Pass-through {@link SocketTransport} backed by a JDK socket. */
final class JdkSocketTransport implements SocketTransport {
    private final Socket socket;
    private final InputStream inputStream;
    private final OutputStream outputStream;

    JdkSocketTransport(Socket socket) throws IOException {
        this.socket = Objects.requireNonNull(socket, "socket");
        inputStream = socket.getInputStream();
        outputStream = socket.getOutputStream();
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
}
