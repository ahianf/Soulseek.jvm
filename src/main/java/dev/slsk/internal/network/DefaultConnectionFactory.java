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
import java.util.concurrent.ExecutorService;

/**
 * Creates protocol and transfer connections.
 *
 * <p>Holds the client's {@link ConnectionMonitor} and hands it to every
 * connection it makes, which is what makes liveness and inactivity sweeping a
 * per-client thing rather than a static one.
 */
public final class DefaultConnectionFactory implements ConnectionFactory {

    private final ConnectionMonitor monitor;
    private final ExecutorService executor;

    /**
     * Creates a factory over a client's connection monitor.
     *
     * @param monitor the monitor every connection this makes is swept by
     */
    public DefaultConnectionFactory(ConnectionMonitor monitor) {
        this(monitor, null);
    }

    /** Creates a factory sharing a client's I/O executor. */
    public DefaultConnectionFactory(ConnectionMonitor monitor, ExecutorService executor) {
        this.monitor = Objects.requireNonNull(monitor, "monitor");
        this.executor = executor;
    }

    @Override
    public MessageConnection getDistributedConnection(
            String username, InetSocketAddress ipEndpoint, ConnectionOptions options, TcpClient tcpClient) {
        return messageConnection(username, ipEndpoint, defaultOptions(options), 1, tcpClient);
    }

    @Override
    public MessageConnection getMessageConnection(
            String username, InetSocketAddress ipEndpoint, ConnectionOptions options, TcpClient tcpClient) {
        return messageConnection(username, ipEndpoint, defaultOptions(options), 4, tcpClient);
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
        DefaultMessageConnection connection = executor == null
                ? new DefaultMessageConnection(
                        ipEndpoint, defaultOptions(options).withoutInactivityTimeout(), 4, tcpClient, monitor)
                : new DefaultMessageConnection(
                        ipEndpoint,
                        defaultOptions(options).withoutInactivityTimeout(),
                        4,
                        tcpClient,
                        monitor,
                        executor);
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
        return executor == null
                ? new SocketConnection(ipEndpoint, defaultOptions(options), tcpClient, monitor)
                : new SocketConnection(ipEndpoint, defaultOptions(options), tcpClient, monitor, executor);
    }

    private DefaultMessageConnection messageConnection(
            String username, InetSocketAddress endpoint, ConnectionOptions options, int codeLength, TcpClient client) {
        return executor == null
                ? new DefaultMessageConnection(username, endpoint, options, codeLength, client, monitor)
                : new DefaultMessageConnection(username, endpoint, options, codeLength, client, monitor, executor);
    }

    private static ConnectionOptions defaultOptions(ConnectionOptions options) {
        return options == null ? new ConnectionOptions() : options;
    }
}
