// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network;

import dev.slsk.network.tcp.Connection;
import dev.slsk.network.tcp.ConnectionDisconnectedEventArgs;
import dev.slsk.network.tcp.ConnectionEventListener;
import dev.slsk.network.tcp.SocketConnection;
import dev.slsk.network.tcp.TcpClient;
import dev.slsk.options.ConnectionOptions;
import java.net.InetSocketAddress;

/** Creates protocol and transfer connections. */
public final class DefaultConnectionFactory implements ConnectionFactory {
    @Override
    public MessageConnection getDistributedConnection(
            String username, InetSocketAddress ipEndpoint, ConnectionOptions options, TcpClient tcpClient) {
        return new DefaultMessageConnection(username, ipEndpoint, defaultOptions(options), 1, tcpClient);
    }

    @Override
    public MessageConnection getMessageConnection(
            String username, InetSocketAddress ipEndpoint, ConnectionOptions options, TcpClient tcpClient) {
        return new DefaultMessageConnection(username, ipEndpoint, defaultOptions(options), 4, tcpClient);
    }

    @Override
    public MessageConnection getServerConnection(
            InetSocketAddress ipEndpoint,
            ConnectionEventListener<Void> connectedEventHandler,
            ConnectionEventListener<ConnectionDisconnectedEventArgs> disconnectedEventHandler,
            MessageConnectionEventListener<MessageEvent> messageReadEventHandler,
            MessageConnectionEventListener<MessageEvent> messageWrittenEventHandler,
            ConnectionOptions options,
            TcpClient tcpClient) {
        DefaultMessageConnection connection = new DefaultMessageConnection(
                ipEndpoint, defaultOptions(options).withoutInactivityTimeout(), 4, tcpClient);
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
        return new SocketConnection(ipEndpoint, defaultOptions(options), tcpClient);
    }

    private static ConnectionOptions defaultOptions(ConnectionOptions options) {
        return options == null ? new ConnectionOptions() : options;
    }
}
