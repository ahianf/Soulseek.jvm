// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import dev.slsk.internal.network.tcp.Connection;
import dev.slsk.internal.network.tcp.ConnectionDisconnectedEvent;
import dev.slsk.internal.network.tcp.ConnectionEventListener;
import dev.slsk.internal.network.tcp.ConnectionMonitor;
import dev.slsk.internal.network.tcp.SocketConnection;
import dev.slsk.internal.network.tcp.TcpClient;
import dev.slsk.internal.options.ConnectionOptions;
import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * Creates protocol and transfer connections.
 *
 * <p>Holds the client's {@link ConnectionMonitor} and hands it to every
 * connection it makes, which is what makes liveness and inactivity sweeping a
 * per-client thing rather than a static one.
 */
public final class DefaultConnectionFactory implements ConnectionFactory {

    private final ConnectionMonitor monitor;

    /**
     * Creates a factory over a client's connection monitor.
     *
     * @param monitor the monitor every connection this makes is swept by
     */
    public DefaultConnectionFactory(ConnectionMonitor monitor) {
        this.monitor = Objects.requireNonNull(monitor, "monitor");
    }

    @Override
    public MessageConnection getDistributedConnection(
            String username, InetSocketAddress ipEndpoint, ConnectionOptions options, TcpClient tcpClient) {
        return new DefaultMessageConnection(username, ipEndpoint, defaultOptions(options), 1, tcpClient, monitor);
    }

    @Override
    public MessageConnection getMessageConnection(
            String username, InetSocketAddress ipEndpoint, ConnectionOptions options, TcpClient tcpClient) {
        return new DefaultMessageConnection(username, ipEndpoint, defaultOptions(options), 4, tcpClient, monitor);
    }

    @Override
    public MessageConnection getServerConnection(
            InetSocketAddress ipEndpoint,
            ConnectionEventListener<Void> connectedEventHandler,
            ConnectionEventListener<ConnectionDisconnectedEvent> disconnectedEventHandler,
            MessageConnectionEventListener<MessageEvent> messageReadEventHandler,
            MessageConnectionEventListener<MessageEvent> messageWrittenEventHandler,
            ConnectionOptions options,
            TcpClient tcpClient) {
        DefaultMessageConnection connection = new DefaultMessageConnection(
                ipEndpoint, defaultOptions(options).withoutInactivityTimeout(), 4, tcpClient, monitor);
        if (connectedEventHandler != null) {
            connection.addConnectedListener(connectedEventHandler);
        }
        if (disconnectedEventHandler != null) {
            connection.addDisconnectedListener(disconnectedEventHandler);
        }
        if (messageReadEventHandler != null) {
            connection.addMessageReadListener(messageReadEventHandler);
        }
        if (messageWrittenEventHandler != null) {
            connection.addMessageWrittenListener(messageWrittenEventHandler);
        }
        return connection;
    }

    @Override
    public Connection getTransferConnection(
            InetSocketAddress ipEndpoint, ConnectionOptions options, TcpClient tcpClient) {
        return new SocketConnection(ipEndpoint, defaultOptions(options), tcpClient, monitor);
    }

    private static ConnectionOptions defaultOptions(ConnectionOptions options) {
        return options == null ? new ConnectionOptions() : options;
    }
}
