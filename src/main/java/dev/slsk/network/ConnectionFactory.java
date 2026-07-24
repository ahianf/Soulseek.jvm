// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network;

import dev.slsk.network.tcp.Connection;
import dev.slsk.network.tcp.ConnectionDisconnectedEventArgs;
import dev.slsk.network.tcp.ConnectionEventListener;
import dev.slsk.network.tcp.TcpClient;
import dev.slsk.options.ConnectionOptions;
import java.net.InetSocketAddress;

/** Creates protocol and transfer connections. */
public interface ConnectionFactory {
    MessageConnection getDistributedConnection(
            String username, InetSocketAddress ipEndPoint, ConnectionOptions options, TcpClient tcpClient);

    default MessageConnection getDistributedConnection(String username, InetSocketAddress ipEndPoint) {
        return getDistributedConnection(username, ipEndPoint, null, null);
    }

    default MessageConnection getDistributedConnection(
            String username, InetSocketAddress ipEndPoint, ConnectionOptions options) {
        return getDistributedConnection(username, ipEndPoint, options, null);
    }

    MessageConnection getMessageConnection(
            String username, InetSocketAddress ipEndPoint, ConnectionOptions options, TcpClient tcpClient);

    default MessageConnection getMessageConnection(String username, InetSocketAddress ipEndPoint) {
        return getMessageConnection(username, ipEndPoint, null, null);
    }

    default MessageConnection getMessageConnection(
            String username, InetSocketAddress ipEndPoint, ConnectionOptions options) {
        return getMessageConnection(username, ipEndPoint, options, null);
    }

    MessageConnection getServerConnection(
            InetSocketAddress ipEndPoint,
            ConnectionEventListener<Void> connectedEventHandler,
            ConnectionEventListener<ConnectionDisconnectedEventArgs> disconnectedEventHandler,
            MessageConnectionEventListener<MessageEventArgs> messageReadEventHandler,
            MessageConnectionEventListener<MessageEventArgs> messageWrittenEventHandler,
            ConnectionOptions options,
            TcpClient tcpClient);

    default MessageConnection getServerConnection(
            InetSocketAddress ipEndPoint,
            ConnectionEventListener<Void> connectedEventHandler,
            ConnectionEventListener<ConnectionDisconnectedEventArgs> disconnectedEventHandler,
            MessageConnectionEventListener<MessageEventArgs> messageReadEventHandler,
            MessageConnectionEventListener<MessageEventArgs> messageWrittenEventHandler) {
        return getServerConnection(
                ipEndPoint,
                connectedEventHandler,
                disconnectedEventHandler,
                messageReadEventHandler,
                messageWrittenEventHandler,
                null,
                null);
    }

    default MessageConnection getServerConnection(
            InetSocketAddress ipEndPoint,
            ConnectionEventListener<Void> connectedEventHandler,
            ConnectionEventListener<ConnectionDisconnectedEventArgs> disconnectedEventHandler,
            MessageConnectionEventListener<MessageEventArgs> messageReadEventHandler,
            MessageConnectionEventListener<MessageEventArgs> messageWrittenEventHandler,
            ConnectionOptions options) {
        return getServerConnection(
                ipEndPoint,
                connectedEventHandler,
                disconnectedEventHandler,
                messageReadEventHandler,
                messageWrittenEventHandler,
                options,
                null);
    }

    Connection getTransferConnection(InetSocketAddress ipEndPoint, ConnectionOptions options, TcpClient tcpClient);

    default Connection getTransferConnection(InetSocketAddress ipEndPoint) {
        return getTransferConnection(ipEndPoint, null, null);
    }

    default Connection getTransferConnection(InetSocketAddress ipEndPoint, ConnectionOptions options) {
        return getTransferConnection(ipEndPoint, options, null);
    }
}
