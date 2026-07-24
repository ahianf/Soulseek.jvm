// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network;

import dev.slsk.CacheLookupResult;
import dev.slsk.SearchResponseCacheRecord;
import dev.slsk.common.Constants;
import dev.slsk.common.WaitKey;
import dev.slsk.diagnostics.DiagnosticEventArgs;
import dev.slsk.diagnostics.DiagnosticEventListener;
import dev.slsk.diagnostics.DiagnosticFactory;
import dev.slsk.diagnostics.IDiagnosticFactory;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.messaging.messages.PeerInit;
import dev.slsk.messaging.messages.PierceFirewall;
import dev.slsk.network.tcp.IConnection;
import dev.slsk.network.tcp.IListener;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;

/** Handles incoming connections established by the TCP listener. */
public final class ListenerHandler implements IListenerHandler {
    private final ListenerHandlerClient client;
    private final IDiagnosticFactory diagnostic;
    private final CopyOnWriteArrayList<DiagnosticEventListener> diagnosticListeners = new CopyOnWriteArrayList<>();

    /** Creates a handler with its default diagnostic factory. */
    public ListenerHandler(ListenerHandlerClient client) {
        this(client, null);
    }

    /** Creates a handler. */
    public ListenerHandler(ListenerHandlerClient client, IDiagnosticFactory diagnosticFactory) {
        this.client = Objects.requireNonNull(client, "client");
        diagnostic = diagnosticFactory == null
                ? new DiagnosticFactory(client.getOptions().getMinimumDiagnosticLevel(), this::raiseDiagnostic)
                : diagnosticFactory;
    }

    @Override
    public void addDiagnosticGeneratedListener(DiagnosticEventListener listener) {
        diagnosticListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeDiagnosticGeneratedListener(DiagnosticEventListener listener) {
        diagnosticListeners.remove(listener);
    }

    @Override
    public void handleConnection(IListener sender, IConnection connection) {
        handleConnectionAsync(connection);
    }

    CompletableFuture<Void> handleConnectionAsync(IConnection connection) {
        diagnostic.debug("Accepted incoming connection from "
                + connection.getIpEndPoint().getAddress().getHostAddress()
                + " on " + client.getListener().getIpAddress()
                + ":" + client.getListener().getPort()
                + " (id: " + connection.getId() + ")");

        CompletableFuture<Void> operation;
        try {
            operation = connection
                    .readAsync(4)
                    .thenCompose(lengthBytes -> {
                        int length = ByteBuffer.wrap(lengthBytes)
                                .order(ByteOrder.LITTLE_ENDIAN)
                                .getInt();
                        return connection.readAsync(length).thenApply(body -> {
                            byte[] message = Arrays.copyOf(lengthBytes, lengthBytes.length + body.length);
                            System.arraycopy(body, 0, message, lengthBytes.length, body.length);
                            return message;
                        });
                    })
                    .thenCompose(message -> routeInitialization(connection, message));
        } catch (Throwable failure) {
            operation = CompletableFuture.failedFuture(failure);
        }

        return operation.handle((ignored, failure) -> {
            if (failure != null) {
                Throwable cause = unwrap(failure);
                diagnostic.debug("Failed to initialize direct connection from "
                        + connection.getIpEndPoint().getAddress().getHostAddress()
                        + ":" + connection.getIpEndPoint().getPort()
                        + ": " + message(cause));
                connection.disconnect(null, asException(cause));
                connection.close();
            }
            return null;
        });
    }

    IDiagnosticFactory getDiagnostic() {
        return diagnostic;
    }

    private CompletableFuture<Void> routeInitialization(IConnection connection, byte[] message) {
        Optional<PeerInit> peerInit = PeerInit.tryFromByteArray(message);
        if (peerInit.isPresent()) {
            return handlePeerInit(connection, peerInit.get());
        }

        Optional<PierceFirewall> pierce = PierceFirewall.tryFromByteArray(message);
        if (pierce.isPresent()) {
            return handlePierceFirewall(connection, pierce.get());
        }

        return CompletableFuture.failedFuture(new ConnectionException("Unrecognized initialization message: "
                + toHex(message) + " (" + message.length
                + " bytes, id: " + connection.getId() + ")"));
    }

    private CompletableFuture<Void> handlePeerInit(IConnection connection, PeerInit peerInit) {
        diagnostic.debug("PeerInit for connection type " + peerInit.getConnectionType()
                + " received from " + peerInit.getUsername() + " ("
                + connection.getIpEndPoint().getAddress().getHostAddress()
                + ":" + client.getListener().getPort()
                + ") (id: " + connection.getId() + ")");

        if (Constants.ConnectionType.PEER.equals(peerInit.getConnectionType())) {
            return client.getPeerConnectionManager()
                    .addOrUpdateMessageConnectionAsync(peerInit.getUsername(), connection);
        }
        if (Constants.ConnectionType.TRANSFER.equals(peerInit.getConnectionType())) {
            return client.getPeerConnectionManager()
                    .getTransferConnectionAsync(peerInit.getUsername(), peerInit.getToken(), connection)
                    .thenAccept(result -> {
                        WaitKey waitKey = new WaitKey(
                                Constants.WaitKey.DIRECT_TRANSFER, peerInit.getUsername(), result.remoteToken());
                        if (client.getWaiter().hasWait(waitKey)) {
                            client.getWaiter().complete(waitKey, result.connection());
                        } else {
                            diagnostic.debug("Unexpected transfer connection for token "
                                    + peerInit.getToken() + " from "
                                    + peerInit.getUsername() + " ("
                                    + connection.getIpEndPoint().getAddress().getHostAddress()
                                    + ":" + client.getListener().getPort()
                                    + ") (id: " + connection.getId() + ")");
                            result.connection().disconnect("Transfer connection rejected: unknown token");
                        }
                    });
        }
        if (Constants.ConnectionType.DISTRIBUTED.equals(peerInit.getConnectionType())) {
            return client.getDistributedConnectionManager()
                    .addOrUpdateChildConnectionAsync(peerInit.getUsername(), connection);
        }
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> handlePierceFirewall(IConnection connection, PierceFirewall pierce) {
        int token = pierce.getToken();
        String username =
                client.getPeerConnectionManager().getPendingSolicitations().get(token);
        if (username != null) {
            diagnostic.debug("Peer PierceFirewall with token " + token
                    + " received from " + username + " ("
                    + connection.getIpEndPoint().getAddress().getHostAddress()
                    + ":" + client.getListener().getPort()
                    + ") (id: " + connection.getId() + ")");
            client.getWaiter()
                    .complete(new WaitKey(Constants.WaitKey.SOLICITED_PEER_CONNECTION, username, token), connection);
            return CompletableFuture.completedFuture(null);
        }

        username = client.getDistributedConnectionManager()
                .getPendingSolicitations()
                .get(token);
        if (username != null) {
            diagnostic.debug("Distributed PierceFirewall with token " + token
                    + " received from " + username + " ("
                    + connection.getIpEndPoint().getAddress().getHostAddress()
                    + ":" + client.getListener().getPort()
                    + ") (id: " + connection.getId() + ")");
            client.getWaiter()
                    .complete(
                            new WaitKey(Constants.WaitKey.SOLICITED_DISTRIBUTED_CONNECTION, username, token),
                            connection);
            return CompletableFuture.completedFuture(null);
        }

        if (client.getOptions().getSearchResponseCache() != null) {
            CacheLookupResult<SearchResponseCacheRecord> lookup =
                    client.getOptions().getSearchResponseCache().tryGet(token);
            if (lookup.found()) {
                SearchResponseCacheRecord record = lookup.value();
                diagnostic.debug("PierceFirewall matching pending search response "
                        + "received from " + record.username() + " ("
                        + connection.getIpEndPoint().getAddress().getHostAddress()
                        + ":" + client.getListener().getPort()
                        + ") (id: " + connection.getId() + ")");
                return client.getPeerConnectionManager()
                        .addOrUpdateMessageConnectionAsync(record.username(), connection)
                        .thenCompose(ignored -> client.getSearchResponder().tryRespondAsync(token))
                        .thenApply(ignored -> null);
            }
        }

        return CompletableFuture.failedFuture(
                new ConnectionException("Unknown PierceFirewall attempt with token " + token
                        + " from "
                        + connection.getIpEndPoint().getAddress().getHostAddress()
                        + ":" + connection.getIpEndPoint().getPort()
                        + " (id: " + connection.getId() + ")"));
    }

    private void raiseDiagnostic(DiagnosticEventArgs args) {
        diagnosticListeners.forEach(listener -> listener.handle(this, args));
    }

    private static String toHex(byte[] bytes) {
        StringBuilder builder = new StringBuilder();
        for (byte value : bytes) {
            if (!builder.isEmpty()) {
                builder.append('-');
            }
            builder.append(String.format("%02X", value & 0xff));
        }
        return builder.toString();
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static String message(Throwable failure) {
        return failure.getMessage() == null ? "" : failure.getMessage();
    }

    private static Exception asException(Throwable failure) {
        return failure instanceof Exception exception ? exception : new RuntimeException(failure);
    }
}
