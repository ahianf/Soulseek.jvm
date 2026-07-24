// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network.tcp;

import dev.slsk.CancellationToken;
import dev.slsk.exceptions.ProxyException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Pass-through implementation of {@link TcpClient} over a socket.
 */
final class TcpClientAdapter implements TcpClient {
    private static final byte SOCKS_5 = 0x05;
    private static final byte AUTH_ANONYMOUS = 0x00;
    private static final byte AUTH_USERNAME = 0x02;
    private static final byte AUTH_VERSION = 0x01;
    private static final byte CONNECT = 0x01;
    private static final byte IPV4 = 0x01;
    private static final byte DOMAIN = 0x03;
    private static final byte IPV6 = 0x04;
    private static final byte EMPTY = 0x00;
    private static final byte ERROR = (byte) 0xff;

    private final Socket socket;
    private boolean disposed;

    TcpClientAdapter() {
        this(new Socket());
    }

    TcpClientAdapter(Socket socket) {
        this.socket = Objects.requireNonNull(socket, "socket");
    }

    @Override
    public Socket getClient() {
        return socket;
    }

    @Override
    public boolean isConnected() {
        return socket.isConnected() && !socket.isClosed();
    }

    @Override
    public InetSocketAddress getRemoteEndpoint() {
        return (InetSocketAddress) socket.getRemoteSocketAddress();
    }

    @Override
    public CompletableFuture<Void> connectAsync(InetAddress address, int port) {
        Objects.requireNonNull(address, "address");
        validatePort(port, "port");
        return CompletableFuture.runAsync(() -> {
            try {
                socket.connect(new InetSocketAddress(address, port));
            } catch (IOException exception) {
                throw new CompletionException(exception);
            }
        });
    }

    @Override
    public CompletableFuture<ProxyEndpoint> connectThroughProxyAsync(
            InetAddress proxyAddress,
            int proxyPort,
            InetAddress destinationAddress,
            int destinationPort,
            String username,
            String password,
            CancellationToken cancellationToken) {
        Objects.requireNonNull(proxyAddress, "proxyAddress");
        validatePort(proxyPort, "proxyPort");
        Objects.requireNonNull(destinationAddress, "destinationAddress");
        validatePort(destinationPort, "destinationPort");
        if ((username == null) != (password == null)) {
            throw new IllegalArgumentException("Username and password must both be supplied");
        }
        if (username != null && username.length() > 255) {
            throw new IllegalArgumentException("The username length must be less than or equal to " + "255 characters");
        }
        if (password != null && password.length() > 255) {
            throw new IllegalArgumentException("The password length must be less than or equal to " + "255 characters");
        }
        CancellationToken token = cancellationToken == null ? CancellationToken.none() : cancellationToken;
        return CompletableFuture.supplyAsync(() -> connectThroughProxyInternal(
                proxyAddress, proxyPort, destinationAddress, destinationPort, token, username, password));
    }

    @Override
    public NetworkStream getStream() throws IOException {
        if (!isConnected()) {
            throw new IllegalStateException("The operation is not allowed on non-connected sockets");
        }
        return new NetworkStreamAdapter(socket);
    }

    @Override
    public void close() throws IOException {
        socket.close();
        disposed = true;
    }

    boolean isDisposed() {
        return disposed;
    }

    private ProxyEndpoint connectThroughProxyInternal(
            InetAddress proxyAddress,
            int proxyPort,
            InetAddress destinationAddress,
            int destinationPort,
            CancellationToken cancellationToken,
            String username,
            String password) {
        boolean usingCredentials = username != null && !username.isEmpty() && password != null && !password.isEmpty();
        byte[] buffer = new byte[1024];

        try {
            await(connectAsync(proxyAddress, proxyPort));
            NetworkStream stream = getStream();

            byte[] auth = usingCredentials
                    ? new byte[] {SOCKS_5, 0x02, AUTH_ANONYMOUS, AUTH_USERNAME}
                    : new byte[] {SOCKS_5, 0x01, AUTH_ANONYMOUS};
            write(stream, auth, cancellationToken);
            byte[] authResponse = read(stream, buffer, 2, cancellationToken);

            if (authResponse[0] != SOCKS_5) {
                throw new ProxyException("Invalid SOCKS version (expected: "
                        + unsigned(SOCKS_5)
                        + ", received: "
                        + unsigned(authResponse[0])
                        + ")");
            }

            switch (authResponse[1]) {
                case AUTH_ANONYMOUS -> {}
                case AUTH_USERNAME -> {
                    if (!usingCredentials) {
                        throw new ProxyException("Server requests authorization but none " + "was provided");
                    }
                    List<Byte> credentials = new ArrayList<>();
                    credentials.add(AUTH_VERSION);
                    credentials.add((byte) username.length());
                    addAll(credentials, username.getBytes(StandardCharsets.US_ASCII));
                    credentials.add((byte) password.length());
                    addAll(credentials, password.getBytes(StandardCharsets.US_ASCII));
                    write(stream, toByteArray(credentials), cancellationToken);

                    byte[] response = read(stream, buffer, 2, cancellationToken);
                    if (response.length != 2) {
                        throw new ProxyException("Abnormal authentication response from server");
                    }
                    if (response[0] != AUTH_VERSION) {
                        throw new ProxyException("Invalid authentication subnegotiation "
                                + "version (expected: "
                                + unsigned(AUTH_VERSION)
                                + ", received: "
                                + unsigned(response[0])
                                + ")");
                    }
                    if (response[1] != EMPTY) {
                        throw new ProxyException("Authentication failed: error code " + unsigned(response[1]));
                    }
                }
                case ERROR ->
                    throw new ProxyException("Server does not support the specified " + "authentication method(s)");
                default ->
                    throw new ProxyException("Unknown auth METHOD response from server: " + unsigned(authResponse[1]));
            }

            List<Byte> connection = new ArrayList<>();
            connection.add(SOCKS_5);
            connection.add(CONNECT);
            connection.add(EMPTY);
            connection.add(IPV4);
            addAll(connection, destinationAddress.getAddress());
            connection.add((byte) (destinationPort >>> 8));
            connection.add((byte) destinationPort);
            write(stream, toByteArray(connection), cancellationToken);

            byte[] connectionResponse = read(stream, buffer, 4, CancellationToken.none());
            if (connectionResponse[0] != SOCKS_5) {
                throw new ProxyException("Invalid SOCKS version (expected: "
                        + unsigned(SOCKS_5)
                        + ", received: "
                        + unsigned(authResponse[0])
                        + ")");
            }
            if (connectionResponse[1] != EMPTY) {
                throw new ProxyException("SOCKS connection failed: " + connectionFailure(connectionResponse[1]));
            }

            String boundAddress;
            try {
                boundAddress = switch (connectionResponse[3]) {
                    case IPV4 ->
                        InetAddress.getByAddress(read(stream, buffer, 4, CancellationToken.none()))
                                .getHostAddress();
                    case DOMAIN -> {
                        byte[] length = read(stream, buffer, 1, CancellationToken.none());
                        if (length[0] == ERROR) {
                            throw new ProxyException("Invalid domain name");
                        }
                        yield new String(
                                read(stream, buffer, unsigned(length[0]), CancellationToken.none()),
                                StandardCharsets.US_ASCII);
                    }
                    case IPV6 ->
                        InetAddress.getByAddress(read(stream, buffer, 16, CancellationToken.none()))
                                .getHostAddress();
                    default ->
                        throw new ProxyException("Unknown SOCKS Address type (expected: one of "
                                + unsigned(IPV4)
                                + ", "
                                + unsigned(DOMAIN)
                                + ", "
                                + unsigned(IPV6)
                                + ", received: "
                                + unsigned(connectionResponse[3])
                                + ")");
                };
            } catch (Exception exception) {
                throw new ProxyException("Invalid address response from server: " + exception.getMessage());
            }

            byte[] port = read(stream, buffer, 2, CancellationToken.none());
            int boundPort = (unsigned(port[0]) << 8) | unsigned(port[1]);
            return new ProxyEndpoint(boundAddress, boundPort);
        } catch (ProxyException exception) {
            throw exception;
        } catch (Exception exception) {
            Throwable cause = unwrap(exception);
            throw new ProxyException("Failed to connect to proxy: " + cause.getMessage(), cause);
        }
    }

    private static byte[] read(NetworkStream stream, byte[] buffer, int length, CancellationToken cancellationToken) {
        int bytesRead = await(stream.readAsync(buffer, 0, length, cancellationToken));
        byte[] result = new byte[bytesRead];
        System.arraycopy(buffer, 0, result, 0, bytesRead);
        return result;
    }

    private static void write(NetworkStream stream, byte[] data, CancellationToken cancellationToken) {
        await(stream.writeAsync(data, 0, data.length, cancellationToken));
    }

    private static <T> T await(CompletableFuture<T> future) {
        return future.join();
    }

    private static void addAll(List<Byte> list, byte[] bytes) {
        for (byte value : bytes) {
            list.add(value);
        }
    }

    private static byte[] toByteArray(List<Byte> bytes) {
        byte[] result = new byte[bytes.size()];
        for (int index = 0; index < bytes.size(); index++) {
            result[index] = bytes.get(index);
        }
        return result;
    }

    private static String connectionFailure(byte code) {
        return switch (unsigned(code)) {
            case 0x01 -> "General SOCKS server failure";
            case 0x02 -> "SocketConnection not allowed by ruleset";
            case 0x03 -> "Network unreachable";
            case 0x04 -> "Host unreachable";
            case 0x05 -> "SocketConnection refused";
            case 0x06 -> "TTL expired";
            case 0x07 -> "Command not supported";
            case 0x08 -> "Address type not supported";
            default -> "Unknown SOCKS error " + unsigned(code);
        };
    }

    private static int unsigned(byte value) {
        return Byte.toUnsignedInt(value);
    }

    private static Throwable unwrap(Throwable exception) {
        Throwable current = exception;
        while ((current instanceof CompletionException) && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static void validatePort(int port, String name) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException(name + " must be within 0 and 65535, inclusive");
        }
    }
}
