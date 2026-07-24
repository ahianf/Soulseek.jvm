// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network;

import dev.slsk.network.tcp.Connection;
import dev.slsk.network.tcp.ConnectionDisconnectedEventArgs;
import dev.slsk.network.tcp.ConnectionEventListener;
import dev.slsk.network.tcp.IConnection;
import dev.slsk.network.tcp.ITcpClient;
import dev.slsk.options.ConnectionOptions;
import java.net.InetSocketAddress;

/** Creates protocol and transfer connections. */
public final class ConnectionFactory implements IConnectionFactory {
    @Override
    public IMessageConnection getDistributedConnection(
            String username, InetSocketAddress ipEndPoint, ConnectionOptions options, ITcpClient tcpClient) {
        return new MessageConnection(username, ipEndPoint, defaultOptions(options), 1, tcpClient);
    }

    @Override
    public IMessageConnection getMessageConnection(
            String username, InetSocketAddress ipEndPoint, ConnectionOptions options, ITcpClient tcpClient) {
        return new MessageConnection(username, ipEndPoint, defaultOptions(options), 4, tcpClient);
    }

    @Override
    public IMessageConnection getServerConnection(
            InetSocketAddress ipEndPoint,
            ConnectionEventListener<Void> connectedEventHandler,
            ConnectionEventListener<ConnectionDisconnectedEventArgs> disconnectedEventHandler,
            MessageConnectionEventListener<MessageEventArgs> messageReadEventHandler,
            MessageConnectionEventListener<MessageEventArgs> messageWrittenEventHandler,
            ConnectionOptions options,
            ITcpClient tcpClient) {
        MessageConnection connection =
                new MessageConnection(ipEndPoint, defaultOptions(options).withoutInactivityTimeout(), 4, tcpClient);
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
    public IConnection getTransferConnection(
            InetSocketAddress ipEndPoint, ConnectionOptions options, ITcpClient tcpClient) {
        return new Connection(ipEndPoint, defaultOptions(options), tcpClient);
    }

    private static ConnectionOptions defaultOptions(ConnectionOptions options) {
        return options == null ? new ConnectionOptions() : options;
    }
}
