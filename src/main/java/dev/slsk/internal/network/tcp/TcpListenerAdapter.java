// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Objects;

/**
 * Pass-through implementation of {@link TcpListener} over a server socket.
 */
final class TcpListenerAdapter implements TcpListener {
    private ServerSocket serverSocket;
    private final InetSocketAddress localEndpoint;
    private Socket pendingSocket;

    TcpListenerAdapter() {
        this(new InetSocketAddress(1));
    }

    TcpListenerAdapter(InetSocketAddress localEndpoint) {
        this.localEndpoint = Objects.requireNonNull(localEndpoint, "localEndpoint");
    }

    TcpListenerAdapter(ServerSocket serverSocket) {
        this.serverSocket = Objects.requireNonNull(serverSocket, "serverSocket");
        localEndpoint = (InetSocketAddress) serverSocket.getLocalSocketAddress();
    }

    @Override
    public Socket acceptTcpClient() {
        ensureStarted();
        Socket accepted;
        synchronized (this) {
            accepted = pendingSocket;
            pendingSocket = null;
        }
        if (accepted != null) {
            return accepted;
        }
        try {
            return serverSocket.accept();
        } catch (IOException exception) {
            // The same UncheckedIOException every other method here throws.
            // Building a CompletionException by hand only meant the caller
            // saw a stack trace pointing at a lambda rather than at the socket.
            throw new UncheckedIOException(exception);
        }
    }

    @Override
    public boolean pending() {
        ensureStarted();
        synchronized (this) {
            if (pendingSocket != null) {
                return true;
            }
            try {
                int timeout = serverSocket.getSoTimeout();
                serverSocket.setSoTimeout(1);
                try {
                    pendingSocket = serverSocket.accept();
                    return true;
                } catch (SocketTimeoutException exception) {
                    return false;
                } finally {
                    serverSocket.setSoTimeout(timeout);
                }
            } catch (IOException exception) {
                throw new UncheckedIOException(exception);
            }
        }
    }

    @Override
    public void start() {
        if (serverSocket != null && serverSocket.isBound() && !serverSocket.isClosed()) {
            return;
        }
        try {
            serverSocket = new ServerSocket();
            serverSocket.bind(localEndpoint);
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    @Override
    public void stop() {
        if (serverSocket == null) {
            return;
        }
        try {
            synchronized (this) {
                if (pendingSocket != null) {
                    pendingSocket.close();
                    pendingSocket = null;
                }
            }
            serverSocket.close();
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    ServerSocket getServerSocket() {
        return serverSocket;
    }

    private void ensureStarted() {
        if (serverSocket == null || !serverSocket.isBound() || serverSocket.isClosed()) {
            throw new IllegalStateException("The listener has not been started");
        }
    }
}
