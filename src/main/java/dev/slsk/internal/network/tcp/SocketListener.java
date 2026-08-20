// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

import dev.slsk.Subscription;
import dev.slsk.internal.events.Subscriptions;
import dev.slsk.internal.options.ConnectionOptions;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/** Listens for client connections for TCP network services. */
public final class SocketListener implements Listener {
    private final CopyOnWriteArrayList<Consumer<? super Connection>> acceptedListeners = new CopyOnWriteArrayList<>();
    private final InetAddress ipAddress;
    private final int port;
    private final ConnectionOptions connectionOptions;
    private final TcpListener tcpListener;
    private final ConnectionMonitor monitor;
    private final ExecutorService executor;
    private volatile boolean listening;

    /** Creates a listener with a socket adapter. */
    public SocketListener(
            InetAddress ipAddress, int port, ConnectionOptions connectionOptions, ConnectionMonitor monitor) {
        this(ipAddress, port, connectionOptions, monitor, null, null);
    }

    /** Creates a listener sharing its client's I/O executor. */
    public SocketListener(
            InetAddress ipAddress,
            int port,
            ConnectionOptions connectionOptions,
            ConnectionMonitor monitor,
            ExecutorService executor) {
        this(ipAddress, port, connectionOptions, monitor, null, executor);
    }

    /**
     * Creates a listener over an optional listener adapter.
     *
     * @param ipAddress what to bind
     * @param port what to bind
     * @param connectionOptions the options every accepted connection gets
     * @param monitor the client's monitor, which every accepted connection is
     *     swept by
     * @param tcpListener a listener adapter to use, or {@code null}
     */
    public SocketListener(
            InetAddress ipAddress,
            int port,
            ConnectionOptions connectionOptions,
            ConnectionMonitor monitor,
            TcpListener tcpListener) {
        this(ipAddress, port, connectionOptions, monitor, tcpListener, null);
    }

    /** Creates a listener over an adapter and a caller-owned executor. */
    public SocketListener(
            InetAddress ipAddress,
            int port,
            ConnectionOptions connectionOptions,
            ConnectionMonitor monitor,
            TcpListener tcpListener,
            ExecutorService executor) {
        this.monitor = Objects.requireNonNull(monitor, "monitor");
        this.executor = executor;
        this.ipAddress = ipAddress;
        this.port = port;
        this.connectionOptions = connectionOptions == null ? new ConnectionOptions() : connectionOptions;
        this.tcpListener =
                tcpListener == null ? new TcpListenerAdapter(new InetSocketAddress(ipAddress, port)) : tcpListener;
    }

    @Override
    public Subscription subscribe(Consumer<? super Connection> listener) {
        return Subscriptions.add(acceptedListeners, listener);
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
        runInBackground(this::listenContinuously);
    }

    @Override
    public void stop() {
        // Before the socket closes, not after. Closing it makes the pending
        // accept fail, and the loop reads this flag to tell that failure —
        // which is expected — from one that means the listener is broken.
        listening = false;
        tcpListener.stop();
    }

    private void listenContinuously() {
        while (listening) {
            Socket client;
            try {
                client = tcpListener.acceptTcpClient();
            } catch (RuntimeException failure) {
                if (!listening) {
                    // Our own stop() closed the socket out from under the
                    // accept. Shutting down is a state, not a failure: routing
                    // it to the uncaught-exception handler trains an operator
                    // to ignore the one channel that should never be noisy.
                    return;
                }
                // The accept loop really is dead; stop claiming otherwise
                // before letting the failure reach the uncaught handler.
                listening = false;
                throw failure;
            }
            runInBackground(() -> raiseAccepted(client));
        }
    }

    /**
     * Runs a fire-and-forget task on a virtual thread without swallowing its
     * failure.
     *
     * <p>These call sites used to wrap the task in a future and attach
     * {@code exceptionally(e -> null)}, so a dead accept loop or a throwing
     * accept handler left no trace at all. Submitting to the executor directly
     * rather than through {@code CompletableFuture} means an escaping throwable
     * reaches the thread's uncaught exception handler, which is the JVM's
     * standard place for exactly this and is never silent.
     */
    private void runInBackground(Runnable task) {
        if (executor == null) {
            Thread.ofVirtual().name("soulseek-standalone-listener").start(task);
        } else {
            executor.execute(task);
        }
    }

    private void raiseAccepted(Socket client) {
        InetSocketAddress endpoint = (InetSocketAddress) client.getRemoteSocketAddress();
        Connection connection = executor == null
                ? new SocketConnection(endpoint, connectionOptions, new TcpClientAdapter(client), monitor)
                : new SocketConnection(endpoint, connectionOptions, new TcpClientAdapter(client), monitor, executor);
        for (Consumer<? super Connection> listener : acceptedListeners) {
            listener.accept(connection);
        }
    }
}
