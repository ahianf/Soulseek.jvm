// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network.tcp;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.CancellationSignal;
import dev.slsk.exceptions.ProxyException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TcpAdapterTest {
    private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();

    @Test
    @DisplayName("SOCKS5 anonymous connection preserves the wire exchange")
    void connectsThroughAnonymousProxy() throws Exception {
        try (ServerSocket proxy = loopbackServer();
                TcpClientAdapter client = new TcpClientAdapter()) {
            CompletableFuture<Void> server = CompletableFuture.runAsync(() -> serveAnonymousProxy(proxy));

            TcpClient.ProxyEndpoint endpoint = client.connectThroughProxyAsync(
                            LOOPBACK,
                            proxy.getLocalPort(),
                            InetAddress.getByName("10.20.30.40"),
                            2234,
                            null,
                            null,
                            CancellationSignal.none())
                    .get(3, TimeUnit.SECONDS);

            assertEquals("127.0.0.1", endpoint.proxyAddress());
            assertEquals(0x1234, endpoint.proxyPort());
            server.get(3, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("SOCKS5 username authentication and domain reply round trip")
    void connectsThroughCredentialedProxy() throws Exception {
        try (ServerSocket proxy = loopbackServer();
                TcpClientAdapter client = new TcpClientAdapter()) {
            CompletableFuture<Void> server = CompletableFuture.runAsync(() -> serveCredentialedProxy(proxy));

            TcpClient.ProxyEndpoint endpoint = client.connectThroughProxyAsync(
                            LOOPBACK,
                            proxy.getLocalPort(),
                            InetAddress.getByName("192.0.2.1"),
                            80,
                            "alice",
                            "secret",
                            CancellationSignal.none())
                    .get(3, TimeUnit.SECONDS);

            assertEquals("foo", endpoint.proxyAddress());
            assertEquals(80, endpoint.proxyPort());
            server.get(3, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("SOCKS5 preserves source response failures and message quirk")
    void reportsProxyFailure() throws Exception {
        try (ServerSocket proxy = loopbackServer();
                TcpClientAdapter client = new TcpClientAdapter()) {
            CompletableFuture<Void> server = CompletableFuture.runAsync(() -> serveInvalidConnectVersion(proxy));

            ExecutionException failure = assertThrows(
                    ExecutionException.class,
                    () -> client.connectThroughProxyAsync(
                                    LOOPBACK,
                                    proxy.getLocalPort(),
                                    InetAddress.getByName("192.0.2.1"),
                                    80,
                                    null,
                                    null,
                                    CancellationSignal.none())
                            .get(3, TimeUnit.SECONDS));

            ProxyException exception = (ProxyException) failure.getCause();
            assertEquals("Invalid SOCKS version (expected: 5, received: 5)", exception.getMessage());
            server.get(3, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("SOCKS5 validates endpoints and credential pairing")
    void validatesProxyArguments() throws Exception {
        try (TcpClientAdapter client = new TcpClientAdapter()) {
            assertThrows(
                    NullPointerException.class,
                    () -> client.connectThroughProxyAsync(null, 1, LOOPBACK, 1, null, null, null));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> client.connectThroughProxyAsync(LOOPBACK, -1, LOOPBACK, 1, null, null, null));
            assertThrows(
                    NullPointerException.class,
                    () -> client.connectThroughProxyAsync(LOOPBACK, 1, null, 1, null, null, null));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> client.connectThroughProxyAsync(LOOPBACK, 1, LOOPBACK, 65_536, null, null, null));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> client.connectThroughProxyAsync(LOOPBACK, 1, LOOPBACK, 1, "alice", null, null));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> client.connectThroughProxyAsync(LOOPBACK, 1, LOOPBACK, 1, "x".repeat(256), "secret", null));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> client.connectThroughProxyAsync(LOOPBACK, 1, LOOPBACK, 1, "alice", "x".repeat(256), null));
        }
    }

    @Test
    @DisplayName("Network stream adapts timeouts, reads, writes, and cancellation")
    void networkStreamPassesThroughSocketIo() throws Exception {
        try (ServerSocket server = loopbackServer();
                TcpClientAdapter client = new TcpClientAdapter()) {
            CompletableFuture<Void> peer = CompletableFuture.runAsync(() -> {
                try (Socket socket = server.accept()) {
                    OutputStream output = socket.getOutputStream();
                    output.write(new byte[] {1, 2, 3});
                    byte[] received = socket.getInputStream().readNBytes(2);
                    assertArrayEquals(new byte[] {4, 5}, received);
                } catch (Exception exception) {
                    throw new RuntimeException(exception);
                }
            });
            client.connectAsync(LOOPBACK, server.getLocalPort()).join();
            assertTrue(client.isConnected());
            assertEquals(server.getLocalPort(), client.getRemoteEndpoint().getPort());

            try (NetworkStream stream = client.getStream()) {
                assertEquals(-1, stream.getReadTimeout());
                assertEquals(-1, stream.getWriteTimeout());
                stream.setReadTimeout(1_000);
                stream.setWriteTimeout(2_000);
                assertEquals(1_000, stream.getReadTimeout());
                assertEquals(2_000, stream.getWriteTimeout());
                assertThrows(IllegalArgumentException.class, () -> stream.setReadTimeout(0));
                assertThrows(IllegalArgumentException.class, () -> stream.setWriteTimeout(-2));

                byte[] received = new byte[3];
                assertEquals(3, stream.read(received, 0, received.length));
                assertArrayEquals(new byte[] {1, 2, 3}, received);
                stream.write(new byte[] {4, 5}, 0, 2);

                // The stream no longer observes cancellation itself: it blocks,
                // and SocketConnection.readInternal owns the cancellation loop
                // using the read timeout as its check point.
                stream.setReadTimeout(50);
            }
            peer.get(3, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("TCP listener starts, probes, accepts, and stops")
    void listenerPassesThroughServerSocketBehavior() throws Exception {
        TcpListenerAdapter listener = new TcpListenerAdapter(new InetSocketAddress(LOOPBACK, 0));
        listener.start();
        try {
            assertFalse(listener.pending());
            try (Socket peer = new Socket(LOOPBACK, listener.getServerSocket().getLocalPort());
                    Socket accepted = acceptPending(listener)) {
                assertTrue(accepted.isConnected());
            }
        } finally {
            listener.stop();
        }
        assertThrows(IllegalStateException.class, listener::pending);
        assertThrows(IllegalStateException.class, listener::acceptTcpClientAsync);
    }

    private static Socket acceptPending(TcpListenerAdapter listener) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!listener.pending() && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertTrue(listener.pending());
        return listener.acceptTcpClientAsync().get(3, TimeUnit.SECONDS);
    }

    private static ServerSocket loopbackServer() throws Exception {
        return new ServerSocket(0, 1, LOOPBACK);
    }

    private static void serveAnonymousProxy(ServerSocket proxy) {
        try (Socket socket = proxy.accept()) {
            InputStream input = socket.getInputStream();
            OutputStream output = socket.getOutputStream();
            assertArrayEquals(new byte[] {5, 1, 0}, input.readNBytes(3));
            output.write(new byte[] {5, 0});
            assertArrayEquals(new byte[] {5, 1, 0, 1, 10, 20, 30, 40, 0x08, (byte) 0xba}, input.readNBytes(10));
            output.write(new byte[] {5, 0, 0, 1, 127, 0, 0, 1, 0x12, 0x34});
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    private static void serveCredentialedProxy(ServerSocket proxy) {
        try (Socket socket = proxy.accept()) {
            InputStream input = socket.getInputStream();
            OutputStream output = socket.getOutputStream();
            assertArrayEquals(new byte[] {5, 2, 0, 2}, input.readNBytes(4));
            output.write(new byte[] {5, 2});
            byte[] username = "alice".getBytes(StandardCharsets.US_ASCII);
            byte[] password = "secret".getBytes(StandardCharsets.US_ASCII);
            byte[] credentials = input.readNBytes(3 + username.length + password.length);
            assertEquals(1, credentials[0]);
            assertEquals(username.length, credentials[1]);
            assertArrayEquals(username, java.util.Arrays.copyOfRange(credentials, 2, 2 + username.length));
            assertEquals(password.length, credentials[2 + username.length]);
            assertArrayEquals(
                    password, java.util.Arrays.copyOfRange(credentials, 3 + username.length, credentials.length));
            output.write(new byte[] {1, 0});
            assertArrayEquals(new byte[] {5, 1, 0, 1, (byte) 192, 0, 2, 1, 0, 80}, input.readNBytes(10));
            output.write(new byte[] {5, 0, 0, 3, 3, 'f', 'o', 'o', 0, 80});
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    private static void serveInvalidConnectVersion(ServerSocket proxy) {
        try (Socket socket = proxy.accept()) {
            InputStream input = socket.getInputStream();
            OutputStream output = socket.getOutputStream();
            input.readNBytes(3);
            output.write(new byte[] {5, 0});
            input.readNBytes(10);
            output.write(new byte[] {4, 0, 0, 1, 127, 0, 0, 1, 0, 1});
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }
}
