// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

import dev.slsk.internal.concurrent.CancellationSignal;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

/** Opens and exposes a client socket connection. */
public interface SocketConnector extends AutoCloseable {
    /** Address and port reported by a SOCKS proxy after connection. */
    record ProxyEndpoint(String proxyAddress, int proxyPort) {}

    /** Returns the underlying socket. */
    Socket socket();

    /** Returns whether the socket has connected and remains open locally. */
    boolean isConnected();

    /** Returns the client remote endpoint. */
    InetSocketAddress getRemoteEndpoint();

    /** Connects the client to a remote endpoint, blocking until it lands. */
    void connect(InetAddress address, int port) throws IOException;

    /** Connects to a destination through a SOCKS5 proxy, blocking throughout. */
    ProxyEndpoint connectThroughProxy(
            InetAddress proxyAddress,
            int proxyPort,
            InetAddress destinationAddress,
            int destinationPort,
            String username,
            String password,
            CancellationSignal cancellationSignal);

    /** Returns the byte transport over the connected socket. */
    SocketTransport transport() throws IOException;

    /** Closes the socket. */
    @Override
    void close() throws IOException;
}
