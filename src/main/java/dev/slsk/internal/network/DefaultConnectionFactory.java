// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import dev.slsk.internal.network.tcp.ConnectionDisconnectedEvent;
import dev.slsk.internal.network.tcp.ConnectionMonitor;
import dev.slsk.internal.network.tcp.SocketConnection;
import dev.slsk.internal.network.tcp.SocketConnector;
import dev.slsk.internal.network.tcp.TransportConnection;
import dev.slsk.internal.options.ConnectionOptions;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

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
            String username, InetSocketAddress ipEndpoint, ConnectionOptions options, SocketConnector connector) {
        return messageConnection(username, ipEndpoint, defaultOptions(options), 1, connector);
    }

    @Override
    public MessageConnection getMessageConnection(
            String username, InetSocketAddress ipEndpoint, ConnectionOptions options, SocketConnector connector) {
        return messageConnection(username, ipEndpoint, defaultOptions(options), 4, connector);
    }

    @Override
    public MessageConnection getServerConnection(
            InetSocketAddress ipEndpoint,
            Consumer<TransportConnection> connectedEventHandler,
            Consumer<ConnectionDisconnectedEvent> disconnectedEventHandler,
            Consumer<MessageEvent> messageReadEventHandler,
            Consumer<MessageEvent> messageWrittenEventHandler,
            ConnectionOptions options,
            SocketConnector connector) {
        DefaultMessageConnection connection = executor == null
                ? new DefaultMessageConnection(
                        ipEndpoint, defaultOptions(options).withoutInactivityTimeout(), 4, connector, monitor)
                : new DefaultMessageConnection(
                        ipEndpoint,
                        defaultOptions(options).withoutInactivityTimeout(),
                        4,
                        connector,
                        monitor,
                        executor);
        if (connectedEventHandler != null) {
            connection.subscribe(TransportConnection.Kind.CONNECTED, connectedEventHandler);
        }
        if (disconnectedEventHandler != null) {
            connection.subscribe(TransportConnection.Kind.DISCONNECTED, disconnectedEventHandler);
        }
        if (messageReadEventHandler != null) {
            connection.subscribe(MessageConnection.MessageKind.READ, messageReadEventHandler);
        }
        if (messageWrittenEventHandler != null) {
            connection.subscribe(MessageConnection.MessageKind.WRITTEN, messageWrittenEventHandler);
        }
        return connection;
    }

    @Override
    public TransportConnection getTransferConnection(
            InetSocketAddress ipEndpoint, ConnectionOptions options, SocketConnector connector) {
        return executor == null
                ? new SocketConnection(ipEndpoint, defaultOptions(options), connector, monitor)
                : new SocketConnection(ipEndpoint, defaultOptions(options), connector, monitor, executor);
    }

    private DefaultMessageConnection messageConnection(
            String username,
            InetSocketAddress endpoint,
            ConnectionOptions options,
            int codeLength,
            SocketConnector client) {
        return executor == null
                ? new DefaultMessageConnection(username, endpoint, options, codeLength, client, monitor)
                : new DefaultMessageConnection(username, endpoint, options, codeLength, client, monitor, executor);
    }

    private static ConnectionOptions defaultOptions(ConnectionOptions options) {
        return options == null ? new ConnectionOptions() : options;
    }
}
