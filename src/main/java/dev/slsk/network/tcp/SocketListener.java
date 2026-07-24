// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network.tcp;

import dev.slsk.common.CommonUtils;
import dev.slsk.options.ConnectionOptions;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/** Listens for client connections for TCP network services. */
public final class SocketListener implements Listener {
    private final CopyOnWriteArrayList<ListenerAcceptedEventListener> acceptedListeners = new CopyOnWriteArrayList<>();
    private final InetAddress ipAddress;
    private final int port;
    private final ConnectionOptions connectionOptions;
    private final TcpListener tcpListener;
    private volatile boolean listening;

    /** Creates a listener with a socket adapter. */
    public SocketListener(InetAddress ipAddress, int port, ConnectionOptions connectionOptions) {
        this(ipAddress, port, connectionOptions, null);
    }

    /** Creates a listener over an optional listener adapter. */
    public SocketListener(
            InetAddress ipAddress, int port, ConnectionOptions connectionOptions, TcpListener tcpListener) {
        this.ipAddress = ipAddress;
        this.port = port;
        this.connectionOptions = connectionOptions == null ? new ConnectionOptions() : connectionOptions;
        this.tcpListener =
                tcpListener == null ? new TcpListenerAdapter(new InetSocketAddress(ipAddress, port)) : tcpListener;
    }

    @Override
    public void addAcceptedListener(ListenerAcceptedEventListener listener) {
        acceptedListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeAcceptedListener(ListenerAcceptedEventListener listener) {
        acceptedListeners.remove(listener);
    }

    @Override
    public ConnectionOptions getConnectionOptions() {
        return connectionOptions;
    }

    @Override
    public InetAddress getIpAddress() {
        return ipAddress;
    }

    @Override
    public boolean isListening() {
        return listening;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public void start() {
        tcpListener.start();
        listening = true;
        CommonUtils.forget(CompletableFuture.runAsync(this::listenContinuously));
    }

    @Override
    public void stop() {
        tcpListener.stop();
        listening = false;
    }

    private void listenContinuously() {
        while (listening) {
            Socket client = tcpListener.acceptTcpClientAsync().join();
            CommonUtils.forget(CompletableFuture.runAsync(() -> raiseAccepted(client)));
        }
    }

    private void raiseAccepted(Socket client) {
        InetSocketAddress endpoint = (InetSocketAddress) client.getRemoteSocketAddress();
        Connection connection = new SocketConnection(endpoint, connectionOptions, new TcpClientAdapter(client));
        for (ListenerAcceptedEventListener listener : acceptedListeners) {
            listener.handle(this, connection);
        }
    }
}
