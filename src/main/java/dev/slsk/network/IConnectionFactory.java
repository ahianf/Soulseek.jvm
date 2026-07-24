// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network;

import dev.slsk.network.tcp.ConnectionDisconnectedEventArgs;
import dev.slsk.network.tcp.ConnectionEventListener;
import dev.slsk.network.tcp.IConnection;
import dev.slsk.network.tcp.ITcpClient;
import dev.slsk.options.ConnectionOptions;
import java.net.InetSocketAddress;

/** Creates protocol and transfer connections. */
public interface IConnectionFactory {
    IMessageConnection getDistributedConnection(
            String username, InetSocketAddress ipEndPoint, ConnectionOptions options, ITcpClient tcpClient);

    default IMessageConnection getDistributedConnection(String username, InetSocketAddress ipEndPoint) {
        return getDistributedConnection(username, ipEndPoint, null, null);
    }

    default IMessageConnection getDistributedConnection(
            String username, InetSocketAddress ipEndPoint, ConnectionOptions options) {
        return getDistributedConnection(username, ipEndPoint, options, null);
    }

    IMessageConnection getMessageConnection(
            String username, InetSocketAddress ipEndPoint, ConnectionOptions options, ITcpClient tcpClient);

    default IMessageConnection getMessageConnection(String username, InetSocketAddress ipEndPoint) {
        return getMessageConnection(username, ipEndPoint, null, null);
    }

    default IMessageConnection getMessageConnection(
            String username, InetSocketAddress ipEndPoint, ConnectionOptions options) {
        return getMessageConnection(username, ipEndPoint, options, null);
    }

    IMessageConnection getServerConnection(
            InetSocketAddress ipEndPoint,
            ConnectionEventListener<Void> connectedEventHandler,
            ConnectionEventListener<ConnectionDisconnectedEventArgs> disconnectedEventHandler,
            MessageConnectionEventListener<MessageEventArgs> messageReadEventHandler,
            MessageConnectionEventListener<MessageEventArgs> messageWrittenEventHandler,
            ConnectionOptions options,
            ITcpClient tcpClient);

    default IMessageConnection getServerConnection(
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

    default IMessageConnection getServerConnection(
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

    IConnection getTransferConnection(InetSocketAddress ipEndPoint, ConnectionOptions options, ITcpClient tcpClient);

    default IConnection getTransferConnection(InetSocketAddress ipEndPoint) {
        return getTransferConnection(ipEndPoint, null, null);
    }

    default IConnection getTransferConnection(InetSocketAddress ipEndPoint, ConnectionOptions options) {
        return getTransferConnection(ipEndPoint, options, null);
    }
}
