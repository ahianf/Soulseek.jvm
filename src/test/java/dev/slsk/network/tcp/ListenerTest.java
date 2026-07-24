// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network.tcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.options.ConnectionOptions;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ListenerTest {
    @Test
    @DisplayName("SocketListener construction preserves endpoint and options")
    void constructs() {
        InetAddress address = InetAddress.getLoopbackAddress();
        ConnectionOptions options = new ConnectionOptions();
        FakeTcpListener tcpListener = new FakeTcpListener();
        SocketListener listener = new SocketListener(address, 2234, options, tcpListener);

        assertSame(address, listener.getIpAddress());
        assertEquals(2234, listener.getPort());
        assertSame(options, listener.getConnectionOptions());
        assertFalse(listener.isListening());
    }

    @Test
    @DisplayName("SocketListener start and stop delegate and update state")
    void startsAndStops() {
        FakeTcpListener tcpListener = new FakeTcpListener();
        SocketListener listener = new SocketListener(InetAddress.getLoopbackAddress(), 2234, null, tcpListener);

        listener.start();
        assertTrue(listener.isListening());
        assertTrue(tcpListener.started);

        listener.stop();
        assertFalse(listener.isListening());
        assertTrue(tcpListener.stopped);
        tcpListener.accept.completeExceptionally(new IllegalStateException("stopped"));
    }

    @Test
    @DisplayName("SocketListener accepts and wraps a connected socket")
    void acceptsConnection() throws Exception {
        InetAddress address = InetAddress.getLoopbackAddress();
        try (ServerSocket server = new ServerSocket(0, 1, address)) {
            TcpListenerAdapter adapter = new TcpListenerAdapter(server);
            ConnectionOptions options = new ConnectionOptions(8, 8, 3, 100, -1);
            SocketListener listener = new SocketListener(address, server.getLocalPort(), options, adapter);
            AtomicReference<Connection> accepted = new AtomicReference<>();
            AtomicReference<Listener> sender = new AtomicReference<>();
            CountDownLatch raised = new CountDownLatch(1);
            listener.addAcceptedListener((eventSender, connection) -> {
                sender.set(eventSender);
                accepted.set(connection);
                raised.countDown();
            });

            listener.start();
            try (Socket peer = new Socket(address, server.getLocalPort())) {
                assertTrue(raised.await(2, TimeUnit.SECONDS));
                assertSame(listener, sender.get());
                assertEquals(ConnectionState.CONNECTED, accepted.get().getState());
                assertEquals(peer.getLocalPort(), accepted.get().getIpEndpoint().getPort());
            } finally {
                listener.stop();
                if (accepted.get() != null) {
                    accepted.get().close();
                }
            }
        }
    }

    private static final class FakeTcpListener implements TcpListener {
        private final CompletableFuture<Socket> accept = new CompletableFuture<>();
        private boolean started;
        private boolean stopped;

        @Override
        public CompletableFuture<Socket> acceptTcpClientAsync() {
            return accept;
        }

        @Override
        public boolean pending() {
            return false;
        }

        @Override
        public void start() {
            started = true;
        }

        @Override
        public void stop() {
            stopped = true;
        }
    }
}
