// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import dev.slsk.internal.network.tcp.Connection;
import dev.slsk.internal.network.tcp.ConnectionDisconnectedEvent;
import dev.slsk.internal.network.tcp.TcpClient;
import dev.slsk.internal.options.ConnectionOptions;
import java.net.InetSocketAddress;
import java.util.function.Consumer;

/** Creates protocol and transfer connections. */
public interface ConnectionFactory {
    MessageConnection getDistributedConnection(
            String username, InetSocketAddress ipEndpoint, ConnectionOptions options, TcpClient tcpClient);

    default MessageConnection getDistributedConnection(String username, InetSocketAddress ipEndpoint) {
        return getDistributedConnection(username, ipEndpoint, null, null);
    }

    default MessageConnection getDistributedConnection(
            String username, InetSocketAddress ipEndpoint, ConnectionOptions options) {
        return getDistributedConnection(username, ipEndpoint, options, null);
    }

    MessageConnection getMessageConnection(
            String username, InetSocketAddress ipEndpoint, ConnectionOptions options, TcpClient tcpClient);

    default MessageConnection getMessageConnection(String username, InetSocketAddress ipEndpoint) {
        return getMessageConnection(username, ipEndpoint, null, null);
    }

    default MessageConnection getMessageConnection(
            String username, InetSocketAddress ipEndpoint, ConnectionOptions options) {
        return getMessageConnection(username, ipEndpoint, options, null);
    }

    MessageConnection getServerConnection(
            InetSocketAddress ipEndpoint,
            Consumer<Connection> connectedEventHandler,
            Consumer<ConnectionDisconnectedEvent> disconnectedEventHandler,
            Consumer<MessageEvent> messageReadEventHandler,
            Consumer<MessageEvent> messageWrittenEventHandler,
            ConnectionOptions options,
            TcpClient tcpClient);

    default MessageConnection getServerConnection(
            InetSocketAddress ipEndpoint,
            Consumer<Connection> connectedEventHandler,
            Consumer<ConnectionDisconnectedEvent> disconnectedEventHandler,
            Consumer<MessageEvent> messageReadEventHandler,
            Consumer<MessageEvent> messageWrittenEventHandler) {
        return getServerConnection(
                ipEndpoint,
                connectedEventHandler,
                disconnectedEventHandler,
                messageReadEventHandler,
                messageWrittenEventHandler,
                null,
                null);
    }

    default MessageConnection getServerConnection(
            InetSocketAddress ipEndpoint,
            Consumer<Connection> connectedEventHandler,
            Consumer<ConnectionDisconnectedEvent> disconnectedEventHandler,
            Consumer<MessageEvent> messageReadEventHandler,
            Consumer<MessageEvent> messageWrittenEventHandler,
            ConnectionOptions options) {
        return getServerConnection(
                ipEndpoint,
                connectedEventHandler,
                disconnectedEventHandler,
                messageReadEventHandler,
                messageWrittenEventHandler,
                options,
                null);
    }

    Connection getTransferConnection(InetSocketAddress ipEndpoint, ConnectionOptions options, TcpClient tcpClient);

    default Connection getTransferConnection(InetSocketAddress ipEndpoint) {
        return getTransferConnection(ipEndpoint, null, null);
    }

    default Connection getTransferConnection(InetSocketAddress ipEndpoint, ConnectionOptions options) {
        return getTransferConnection(ipEndpoint, options, null);
    }
}
