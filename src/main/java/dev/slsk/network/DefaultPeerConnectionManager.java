// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network;

import dev.slsk.CancellationRegistration;
import dev.slsk.CancellationToken;
import dev.slsk.CancellationTokenSource;
import dev.slsk.common.Constants;
import dev.slsk.common.WaitKey;
import dev.slsk.diagnostics.DiagnosticEvent;
import dev.slsk.diagnostics.DiagnosticEventListener;
import dev.slsk.diagnostics.DiagnosticSink;
import dev.slsk.diagnostics.FilteringDiagnosticSink;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.messaging.handlers.PeerMessageHandler;
import dev.slsk.messaging.messages.ConnectToPeerRequest;
import dev.slsk.messaging.messages.ConnectToPeerResponse;
import dev.slsk.messaging.messages.PeerInit;
import dev.slsk.messaging.messages.PierceFirewall;
import dev.slsk.network.tcp.Connection;
import dev.slsk.network.tcp.ConnectionDisconnectedEventArgs;
import dev.slsk.network.tcp.ConnectionEventListener;
import dev.slsk.network.tcp.ConnectionTypes;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/** Manages peer message and transfer connections. */
public final class DefaultPeerConnectionManager implements PeerConnectionManager {
    private final PeerConnectionManagerClient client;
    private final ConnectionFactory connectionFactory;
    private final DiagnosticSink diagnostic;
    private final CopyOnWriteArrayList<DiagnosticEventListener> diagnosticListeners = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<String, CompletableFuture<MessageConnection>> messageConnections =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CancellationTokenSource> pendingInboundIndirectConnections =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, String> pendingSolicitations = new ConcurrentHashMap<>();
    private final ConnectionEventListener<ConnectionDisconnectedEventArgs> disconnectedListener =
            this::messageConnectionDisconnected;
    private final ConnectionEventListener<ConnectionDisconnectedEventArgs> provisionalDisconnectedListener =
            this::messageConnectionProvisionalDisconnected;
    private final AtomicBoolean disposed = new AtomicBoolean();

    /** Creates a manager with default collaborators. */
    public DefaultPeerConnectionManager(PeerConnectionManagerClient client) {
        this(client, null, null);
    }

    /** Creates a manager. */
    public DefaultPeerConnectionManager(
            PeerConnectionManagerClient client, ConnectionFactory connectionFactory, DiagnosticSink diagnosticFactory) {
        this.client = Objects.requireNonNull(client, "client");
        this.connectionFactory = connectionFactory == null ? new DefaultConnectionFactory() : connectionFactory;
        diagnostic = diagnosticFactory == null
                ? new FilteringDiagnosticSink(client.getOptions().getMinimumDiagnosticLevel(), this::raiseDiagnostic)
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
    public List<PeerEndpoint> getMessageConnections() {
        List<PeerEndpoint> snapshot = new ArrayList<>();
        for (CompletableFuture<MessageConnection> future : messageConnections.values()) {
            MessageConnection connection = future.join();
            snapshot.add(new PeerEndpoint(connection.getUsername(), connection.getIpEndpoint()));
        }
        return List.copyOf(snapshot);
    }

    @Override
    public Map<Integer, String> getPendingSolicitations() {
        return Map.copyOf(pendingSolicitations);
    }

    @Override
    public CompletableFuture<Void> addOrUpdateMessageConnectionAsync(String username, Connection incomingConnection) {
        Objects.requireNonNull(incomingConnection, "incomingConnection");
        AtomicReference<CompletableFuture<MessageConnection>> cached = new AtomicReference<>();
        CompletableFuture<MessageConnection> replacement = messageConnections.compute(username, (key, old) -> {
            cached.set(old);
            return invoke(() -> establishIncomingMessageConnection(username, incomingConnection, old));
        });

        return replacement.handle((connection, failure) -> {
            if (failure != null) {
                Throwable cause = unwrap(failure);
                String message = "Failed to establish an inbound message connection to "
                        + username + " (" + incomingConnection.getIpEndpoint()
                        + "): " + message(cause);
                diagnostic.debug(message + " (type: "
                        + incomingConnection.getType() + ", id: "
                        + incomingConnection.getId() + ")");
                diagnostic.debug("Purging message connection cache of failed connection "
                        + "to " + username + " ("
                        + incomingConnection.getIpEndpoint() + ").");
                messageConnections.remove(username);
                throw new CompletionException(new ConnectionException(message, cause));
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<Connection> awaitTransferConnectionAsync(
            String username, String filename, int remoteToken, CancellationToken cancellationToken) {
        LinkedCancellation directCancellation = new LinkedCancellation(cancellationToken);
        LinkedCancellation indirectCancellation = new LinkedCancellation(cancellationToken);
        diagnostic.debug("Waiting for a direct or indirect transfer connection from "
                + username + " with remote token " + remoteToken + " for "
                + filename);

        CompletableFuture<Connection> indirect = client.getWaiter()
                .waitAsync(
                        new WaitKey(Constants.WaitKey.INDIRECT_TRANSFER, username, filename, remoteToken),
                        Connection.class,
                        client.getOptions().getTransferConnectionOptions().getConnectTimeout(),
                        indirectCancellation.token());
        CompletableFuture<Connection> direct = client.getWaiter()
                .waitAsync(
                        new WaitKey(Constants.WaitKey.DIRECT_TRANSFER, username, remoteToken),
                        Connection.class,
                        client.getOptions().getTransferConnectionOptions().getConnectTimeout(),
                        directCancellation.token());

        return firstSuccessful(direct, indirect).handle((winner, failure) -> {
            if (failure != null) {
                directCancellation.close();
                indirectCancellation.close();
                String message = "Failed to establish a direct or indirect transfer "
                        + "connection to " + username
                        + " with remote token " + remoteToken + " for "
                        + filename;
                diagnostic.debug(message);
                throw new CompletionException(new ConnectionException(message));
            }
            boolean directWon = winner.source() == direct;
            diagnostic.debug((directWon ? "Direct" : "Indirect")
                    + " transfer connection to " + username + " ("
                    + winner.value().getIpEndpoint()
                    + ") with remote token " + remoteToken + " for " + filename
                    + " established first, attempting to cancel "
                    + (directWon ? "indirect" : "direct") + " connection.");
            (directWon ? indirectCancellation : directCancellation).cancel();
            directCancellation.close();
            indirectCancellation.close();
            diagnostic.debug("Transfer connection to " + username + " ("
                    + winner.value().getIpEndpoint()
                    + ") with remote token " + remoteToken + " for "
                    + filename + " established. (type: "
                    + winner.value().getType() + ", id: "
                    + winner.value().getId() + ")");
            return winner.value();
        });
    }

    @Override
    public CompletableFuture<MessageConnection> getCachedMessageConnectionAsync(String username) {
        CompletableFuture<MessageConnection> cached = messageConnections.get(username);
        if (cached == null) {
            return CompletableFuture.completedFuture(null);
        }
        return cached.handle((connection, failure) -> {
            if (failure != null) {
                diagnostic.debug("Failed to retrieve cached message connection to " + username + ": "
                        + message(unwrap(failure)));
                return null;
            }
            diagnostic.debug("Retrieved cached message connection to "
                    + connection.getUsername() + " ("
                    + connection.getIpEndpoint() + ") (type: "
                    + connection.getType() + ", id: "
                    + connection.getId() + ")");
            return connection;
        });
    }

    @Override
    public CompletableFuture<MessageConnection> getOrAddMessageConnectionAsync(ConnectToPeerResponse response) {
        AtomicBoolean cached = new AtomicBoolean(true);
        CompletableFuture<MessageConnection> future =
                messageConnections.computeIfAbsent(response.getUsername(), key -> {
                    cached.set(false);
                    return invoke(() -> establishInboundIndirectMessageConnection(response));
                });

        return future.handle((connection, failure) -> {
            if (failure == null) {
                if (cached.get()) {
                    diagnostic.debug("Retrieved cached message connection to "
                            + response.getUsername() + " ("
                            + response.getIpEndpoint() + ") (type: "
                            + connection.getType() + ", id: "
                            + connection.getId() + ")");
                }
                return connection;
            }

            Throwable cause = unwrap(failure);
            String message = "Failed to establish an inbound indirect message "
                    + "connection to " + response.getUsername() + " ("
                    + response.getIpEndpoint() + "): " + message(cause);
            diagnostic.debug(message);
            if (!(cause instanceof CancellationException)) {
                diagnostic.debug("Purging message connection cache of failed connection "
                        + "to " + response.getUsername() + " ("
                        + response.getIpEndpoint() + ").");
                CompletableFuture<MessageConnection> removedRecord = messageConnections.remove(response.getUsername());
                if (removedRecord != null) {
                    removedRecord.handle((removed, ignored) -> {
                        if (removed != null && removed.getType().hasFlag(ConnectionTypes.DIRECT)) {
                            diagnostic.warning("Erroneously purged direct message "
                                    + "connection to "
                                    + response.getUsername()
                                    + " upon indirect failure");
                            messageConnections.putIfAbsent(response.getUsername(), removedRecord);
                        }
                        return null;
                    });
                }
            }
            throw new CompletionException(new ConnectionException(message, cause));
        });
    }

    @Override
    public CompletableFuture<MessageConnection> getOrAddMessageConnectionAsync(
            String username, InetSocketAddress ipEndpoint, CancellationToken cancellationToken) {
        return getOrAddMessageConnectionAsync(username, ipEndpoint, client.getNextToken(), cancellationToken);
    }

    @Override
    public CompletableFuture<MessageConnection> getOrAddMessageConnectionAsync(
            String username, InetSocketAddress ipEndpoint, int solicitationToken, CancellationToken cancellationToken) {
        AtomicBoolean cached = new AtomicBoolean(true);
        CompletableFuture<MessageConnection> future = messageConnections.computeIfAbsent(username, key -> {
            cached.set(false);
            return establishRacingMessageConnection(username, ipEndpoint, solicitationToken, cancellationToken);
        });
        return future.handle((connection, failure) -> {
            if (failure != null) {
                diagnostic.debug("Purging message connection cache of failed connection " + "to " + username + " ("
                        + ipEndpoint + ").");
                messageConnections.remove(username);
                throw new CompletionException(unwrap(failure));
            }
            if (cached.get()) {
                diagnostic.debug("Retrieved cached message connection to " + username
                        + " (" + ipEndpoint + ") (type: "
                        + connection.getType() + ", id: "
                        + connection.getId() + ")");
            }
            return connection;
        });
    }

    @Override
    public CompletableFuture<TransferConnectionResult> getTransferConnectionAsync(
            String username, int token, Connection incomingConnection) {
        diagnostic.debug("Inbound transfer connection to " + username + " ("
                + incomingConnection.getIpEndpoint() + ") for token " + token
                + " accepted. (type: " + incomingConnection.getType()
                + ", id: " + incomingConnection.getId());
        Connection connection = connectionFactory.getTransferConnection(
                incomingConnection.getIpEndpoint(),
                client.getOptions().getTransferConnectionOptions(),
                incomingConnection.handoffTcpClient());
        connection.setType(ConnectionTypes.INBOUND.or(ConnectionTypes.DIRECT));
        connection.addDisconnectedListener(
                (sender, eventArgs) -> diagnostic.debug("Transfer connection to " + username + " ("
                        + connection.getIpEndpoint() + ") for token " + token
                        + " disconnected: " + disconnectMessage(eventArgs)
                        + ". (type: " + connection.getType() + ", id: "
                        + connection.getId() + ")"));
        diagnostic.debug("Inbound transfer connection to " + username + " ("
                + connection.getIpEndpoint() + ") for token " + token
                + " handed off. (old: " + incomingConnection.getId()
                + ", new: " + connection.getId() + ")");

        return connection.readAsync(4).handle((bytes, failure) -> {
            if (failure != null) {
                Throwable cause = unwrap(failure);
                String message = "Failed to establish an inbound transfer connection to "
                        + username + " ("
                        + incomingConnection.getIpEndpoint()
                        + ") for token " + token + ": " + message(cause);
                diagnostic.debug(message + " (type: " + connection.getType() + ", id: " + connection.getId() + ")");
                connection.close();
                throw new CompletionException(new ConnectionException(message, cause));
            }
            int remoteToken = littleEndianInteger(bytes);
            diagnostic.debug("Transfer connection to " + username + " ("
                    + connection.getIpEndpoint() + ") for token "
                    + remoteToken + " established. (type: "
                    + connection.getType() + ", id: "
                    + connection.getId() + ")");
            return new TransferConnectionResult(connection, remoteToken);
        });
    }

    @Override
    public CompletableFuture<TransferConnectionResult> getTransferConnectionAsync(ConnectToPeerResponse response) {
        diagnostic.debug("Attempting inbound indirect transfer connection to "
                + response.getUsername() + " (" + response.getIpEndpoint()
                + ") for token " + response.getToken());
        Connection connection = connectionFactory.getTransferConnection(
                response.getIpEndpoint(), client.getOptions().getTransferConnectionOptions());
        connection.setType(ConnectionTypes.INBOUND.or(ConnectionTypes.INDIRECT));
        connection.addDisconnectedListener(
                (sender, eventArgs) -> diagnostic.debug("Transfer connection to " + response.getUsername() + " ("
                        + response.getIpEndpoint() + ") for token "
                        + response.getToken() + " disconnected: "
                        + disconnectMessage(eventArgs) + ". (type: "
                        + connection.getType() + ", id: "
                        + connection.getId() + ")"));

        return connection
                .connectAsync()
                .thenCompose(ignored -> connection.writeAsync(new PierceFirewall(response.getToken()).toByteArray()))
                .thenCompose(ignored -> connection.readAsync(4))
                .handle((bytes, failure) -> {
                    if (failure != null) {
                        Throwable cause = unwrap(failure);
                        String message = "Failed to establish an inbound indirect transfer "
                                + "connection to " + response.getUsername() + " ("
                                + response.getIpEndpoint() + "): "
                                + message(cause);
                        diagnostic.debug(message);
                        connection.close();
                        throw new CompletionException(new ConnectionException(message, cause));
                    }
                    int remoteToken = littleEndianInteger(bytes);
                    diagnostic.debug("Transfer connection to " + response.getUsername() + " ("
                            + response.getIpEndpoint() + ") for token "
                            + response.getToken() + " established. (type: "
                            + connection.getType() + ", id: "
                            + connection.getId() + ")");
                    return new TransferConnectionResult(connection, remoteToken);
                });
    }

    @Override
    public CompletableFuture<Connection> getTransferConnectionAsync(
            String username, InetSocketAddress ipEndpoint, int token, CancellationToken cancellationToken) {
        LinkedCancellation directCancellation = new LinkedCancellation(cancellationToken);
        LinkedCancellation indirectCancellation = new LinkedCancellation(cancellationToken);
        diagnostic.debug("Attempting simultaneous direct and indirect transfer " + "connections to " + username + " ("
                + ipEndpoint + ")");
        CompletableFuture<Connection> direct =
                getTransferConnectionOutboundDirectAsync(ipEndpoint, token, directCancellation.token());
        CompletableFuture<Connection> indirect =
                getTransferConnectionOutboundIndirectAsync(username, token, indirectCancellation.token());

        return firstSuccessful(direct, indirect)
                .thenCompose(winner -> {
                    boolean directWon = winner.source() == direct;
                    diagnostic.debug((directWon ? "Direct" : "Indirect")
                            + " transfer connection to " + username + " (" + ipEndpoint
                            + ") established first, attempting to cancel "
                            + (directWon ? "indirect" : "direct") + " connection.");
                    (directWon ? indirectCancellation : directCancellation).cancel();
                    CompletableFuture<Void> negotiation = CompletableFuture.completedFuture(null);
                    if (directWon) {
                        byte[] request = new PeerInit(client.getUsername(), Constants.ConnectionType.TRANSFER, token)
                                .toByteArray();
                        negotiation = winner.value().writeAsync(request, token(cancellationToken));
                    }
                    return negotiation
                            .thenCompose(ignored ->
                                    winner.value().writeAsync(littleEndianBytes(token), token(cancellationToken)))
                            .handle((ignored, failure) -> {
                                directCancellation.close();
                                indirectCancellation.close();
                                if (failure != null) {
                                    Throwable cause = unwrap(failure);
                                    String message = "Failed to negotiate transfer connection to "
                                            + username + " (" + ipEndpoint + "): "
                                            + message(cause);
                                    diagnostic.debug(message + " (type: "
                                            + winner.value().getType() + ", id: "
                                            + winner.value().getId() + ")");
                                    winner.value().close();
                                    throw new CompletionException(new ConnectionException(message, cause));
                                }
                                diagnostic.debug("Transfer connection to " + username + " ("
                                        + ipEndpoint + ") established. (type: "
                                        + winner.value().getType() + ", id: "
                                        + winner.value().getId() + ")");
                                return winner.value();
                            });
                })
                .handle((connection, failure) -> {
                    if (failure != null) {
                        directCancellation.close();
                        indirectCancellation.close();
                        Throwable cause = unwrap(failure);
                        if (cause instanceof ConnectionException
                                && cause.getMessage() != null
                                && cause.getMessage().startsWith("Failed to negotiate")) {
                            throw new CompletionException(cause);
                        }
                        String message = "Failed to establish a direct or indirect transfer "
                                + "connection to " + username + " (" + ipEndpoint
                                + ")";
                        diagnostic.debug(message);
                        throw new CompletionException(new ConnectionException(message));
                    }
                    return connection;
                });
    }

    @Override
    public void removeAndDisposeAll() {
        pendingSolicitations.clear();
        pendingInboundIndirectConnections.clear();
        messageConnections.forEach((username, future) -> {
            if (messageConnections.remove(username, future)) {
                future.thenAccept(connection -> {
                    if (connection != null) {
                        connection.close();
                    }
                });
            }
        });
    }

    @Override
    public boolean tryInvalidateMessageConnectionCache(String username) {
        return messageConnections.remove(username) != null;
    }

    @Override
    public void close() {
        if (disposed.compareAndSet(false, true)) {
            removeAndDisposeAll();
        }
    }

    private CompletableFuture<MessageConnection> establishIncomingMessageConnection(
            String username, Connection incomingConnection, CompletableFuture<MessageConnection> cached) {
        diagnostic.debug("Inbound message connection to " + username + " ("
                + incomingConnection.getIpEndpoint()
                + ") accepted. (type: " + incomingConnection.getType()
                + ", id: " + incomingConnection.getId() + ")");
        MessageConnection connection = connectionFactory.getMessageConnection(
                username,
                incomingConnection.getIpEndpoint(),
                client.getOptions().getPeerConnectionOptions(),
                incomingConnection.handoffTcpClient());
        diagnostic.debug("Inbound message connection to " + username + " ("
                + connection.getIpEndpoint() + ") handed off. (old: "
                + incomingConnection.getId() + ", new: "
                + connection.getId() + ")");
        incomingConnection.close();
        connection.setType(ConnectionTypes.INBOUND.or(ConnectionTypes.DIRECT));
        attachPeerMessageListeners(connection);
        connection.addDisconnectedListener(disconnectedListener);

        CompletableFuture<Void> supersede = CompletableFuture.completedFuture(null);
        if (cached != null) {
            CancellationTokenSource pending = pendingInboundIndirectConnections.get(username);
            if (pending != null) {
                diagnostic.debug("Cancelling pending inbound indirect message connection " + "to " + username);
                pending.cancel();
            }
            supersede = cached.handle((old, failure) -> {
                if (old != null) {
                    old.removeDisconnectedListener(disconnectedListener);
                    diagnostic.debug("Superseding cached message connection to "
                            + username + " (" + old.getIpEndpoint()
                            + ") (old: " + old.getId() + ", new: "
                            + connection.getId());
                }
                return null;
            });
        }

        return supersede.thenApply(ignored -> {
            try {
                connection.startReadingContinuously();
            } catch (Throwable failure) {
                connection.close();
                throw failure;
            }
            diagnostic.debug("Message connection to " + username + " ("
                    + connection.getIpEndpoint() + ") established. (type: "
                    + connection.getType() + ", id: "
                    + connection.getId() + ")");
            return connection;
        });
    }

    private CompletableFuture<MessageConnection> establishInboundIndirectMessageConnection(
            ConnectToPeerResponse response) {
        diagnostic.debug("Attempting inbound indirect message connection to "
                + response.getUsername() + " (" + response.getIpEndpoint()
                + ") for token " + response.getToken());
        MessageConnection connection = connectionFactory.getMessageConnection(
                response.getUsername(),
                response.getIpEndpoint(),
                client.getOptions().getPeerConnectionOptions());
        connection.setType(ConnectionTypes.INBOUND.or(ConnectionTypes.INDIRECT));
        attachPeerMessageListeners(connection);
        CancellationTokenSource cancellation = new CancellationTokenSource();
        pendingInboundIndirectConnections.put(response.getUsername(), cancellation);

        return connection
                .connectAsync(cancellation.getToken())
                .thenCompose(ignored -> connection.writeAsync(
                        new PierceFirewall(response.getToken()).toByteArray(), cancellation.getToken()))
                .handle((ignored, failure) -> {
                    pendingInboundIndirectConnections.remove(response.getUsername(), cancellation);
                    cancellation.close();
                    if (failure != null) {
                        connection.close();
                        throw new CompletionException(unwrap(failure));
                    }
                    connection.addDisconnectedListener(disconnectedListener);
                    diagnostic.debug("Message connection to " + response.getUsername() + " ("
                            + response.getIpEndpoint()
                            + ") established. (type: " + connection.getType()
                            + ", id: " + connection.getId() + ")");
                    return connection;
                });
    }

    private CompletableFuture<MessageConnection> establishRacingMessageConnection(
            String username, InetSocketAddress ipEndpoint, int solicitationToken, CancellationToken cancellationToken) {
        LinkedCancellation directCancellation = new LinkedCancellation(cancellationToken);
        LinkedCancellation indirectCancellation = new LinkedCancellation(cancellationToken);
        diagnostic.debug("Attempting simultaneous direct and indirect message " + "connections to " + username + " ("
                + ipEndpoint + ")");
        CompletableFuture<MessageConnection> direct =
                getMessageConnectionOutboundDirectAsync(username, ipEndpoint, directCancellation.token());
        CompletableFuture<MessageConnection> indirect =
                getMessageConnectionOutboundIndirectAsync(username, solicitationToken, indirectCancellation.token());

        return firstSuccessful(direct, indirect)
                .thenCompose(winner -> {
                    MessageConnection connection = winner.value();
                    connection.addDisconnectedListener(disconnectedListener);
                    connection.removeDisconnectedListener(provisionalDisconnectedListener);
                    boolean directWon = winner.source() == direct;
                    diagnostic.debug((directWon ? "Direct" : "Indirect")
                            + " message connection to " + username + " (" + ipEndpoint
                            + ") established first, attempting to cancel "
                            + (directWon ? "indirect" : "direct") + " connection.");
                    (directWon ? indirectCancellation : directCancellation).cancel();

                    CompletableFuture<Void> negotiation;
                    try {
                        if (directWon) {
                            negotiation = connection.writeAsync(
                                    new PeerInit(
                                                    client.getUsername(),
                                                    Constants.ConnectionType.PEER,
                                                    client.getNextToken())
                                            .toByteArray(),
                                    token(cancellationToken));
                        } else {
                            connection.startReadingContinuously();
                            negotiation = CompletableFuture.completedFuture(null);
                        }
                    } catch (Throwable failure) {
                        negotiation = CompletableFuture.failedFuture(failure);
                    }
                    return negotiation.handle((ignored, failure) -> {
                        directCancellation.close();
                        indirectCancellation.close();
                        if (failure != null) {
                            Throwable cause = unwrap(failure);
                            String message = "Failed to negotiate message connection to "
                                    + username + " (" + ipEndpoint + "): "
                                    + message(cause);
                            diagnostic.debug(
                                    message + " (type: " + connection.getType() + ", id: " + connection.getId() + ")");
                            connection.close();
                            throw new CompletionException(new ConnectionException(message, cause));
                        }
                        diagnostic.debug("Message connection to " + username + " (" + ipEndpoint
                                + ") established. (type: " + connection.getType()
                                + ", id: " + connection.getId() + ")");
                        return connection;
                    });
                })
                .handle((connection, failure) -> {
                    if (failure != null) {
                        directCancellation.close();
                        indirectCancellation.close();
                        Throwable cause = unwrap(failure);
                        if (cause instanceof ConnectionException) {
                            throw new CompletionException(cause);
                        }
                        String message = "Failed to establish a direct or indirect message "
                                + "connection to " + username + " (" + ipEndpoint
                                + ")";
                        diagnostic.debug(message);
                        throw new CompletionException(new ConnectionException(message));
                    }
                    return connection;
                });
    }

    private CompletableFuture<MessageConnection> getMessageConnectionOutboundDirectAsync(
            String username, InetSocketAddress ipEndpoint, CancellationToken cancellationToken) {
        diagnostic.debug("Attempting direct message connection to " + username + " (" + ipEndpoint + ")");
        MessageConnection connection = connectionFactory.getMessageConnection(
                username, ipEndpoint, client.getOptions().getPeerConnectionOptions());
        connection.setType(ConnectionTypes.OUTBOUND.or(ConnectionTypes.DIRECT));
        attachPeerMessageListeners(connection);
        connection.addDisconnectedListener(provisionalDisconnectedListener);
        return connection.connectAsync(cancellationToken).handle((ignored, failure) -> {
            if (failure != null) {
                diagnostic.debug("Failed to establish a direct message connection to "
                        + username + " (" + ipEndpoint + "): "
                        + message(unwrap(failure)));
                connection.close();
                throw new CompletionException(unwrap(failure));
            }
            diagnostic.debug("Direct message connection to " + username + " ("
                    + ipEndpoint + ") established. (type: "
                    + connection.getType() + ", id: "
                    + connection.getId() + ")");
            return connection;
        });
    }

    private CompletableFuture<MessageConnection> getMessageConnectionOutboundIndirectAsync(
            String username, int solicitationToken, CancellationToken cancellationToken) {
        diagnostic.debug("Soliciting indirect message connection to " + username + " with token " + solicitationToken);
        pendingSolicitations.putIfAbsent(solicitationToken, username);
        CompletableFuture<Connection> incoming = client.getServerConnection()
                .writeAsync(
                        new ConnectToPeerRequest(solicitationToken, username, Constants.ConnectionType.PEER),
                        cancellationToken)
                .thenCompose(ignored -> client.getWaiter()
                        .waitAsync(
                                new WaitKey(Constants.WaitKey.SOLICITED_PEER_CONNECTION, username, solicitationToken),
                                Connection.class,
                                client.getOptions().getPeerConnectionOptions().getConnectTimeout(),
                                cancellationToken));

        return incoming.thenApply(accepted -> {
                    try {
                        MessageConnection connection = connectionFactory.getMessageConnection(
                                username,
                                accepted.getIpEndpoint(),
                                client.getOptions().getPeerConnectionOptions(),
                                accepted.handoffTcpClient());
                        diagnostic.debug("Indirect message connection to " + username + " ("
                                + accepted.getIpEndpoint()
                                + ") handed off. (old: " + accepted.getId()
                                + ", new: " + connection.getId() + ")");
                        connection.setType(ConnectionTypes.OUTBOUND.or(ConnectionTypes.INDIRECT));
                        attachPeerMessageListeners(connection);
                        connection.addDisconnectedListener(provisionalDisconnectedListener);
                        diagnostic.debug("Indirect message connection to " + username + " ("
                                + connection.getIpEndpoint()
                                + ") established. (type: " + connection.getType()
                                + ", id: " + connection.getId() + ")");
                        return connection;
                    } finally {
                        accepted.close();
                    }
                })
                .handle((connection, failure) -> {
                    pendingSolicitations.remove(solicitationToken, username);
                    if (failure != null) {
                        diagnostic.debug("Failed to establish an indirect message connection to "
                                + username + " with token " + solicitationToken
                                + ": " + message(unwrap(failure)));
                        throw new CompletionException(unwrap(failure));
                    }
                    return connection;
                });
    }

    private CompletableFuture<Connection> getTransferConnectionOutboundDirectAsync(
            InetSocketAddress ipEndpoint, int token, CancellationToken cancellationToken) {
        diagnostic.debug("Attempting direct transfer connection for token " + token + " to " + ipEndpoint);
        Connection connection = connectionFactory.getTransferConnection(
                ipEndpoint, client.getOptions().getTransferConnectionOptions());
        connection.setType(ConnectionTypes.OUTBOUND.or(ConnectionTypes.DIRECT));
        connection.addDisconnectedListener(
                (sender, eventArgs) -> diagnostic.debug("Transfer connection for token " + token + " to "
                        + ipEndpoint + " disconnected: "
                        + disconnectMessage(eventArgs) + ". (type: "
                        + connection.getType() + ", id: "
                        + connection.getId() + ")"));
        return connection.connectAsync(cancellationToken).handle((ignored, failure) -> {
            if (failure != null) {
                diagnostic.debug("Failed to establish a direct transfer connection "
                        + "for token " + token + " to (" + ipEndpoint
                        + "): " + message(unwrap(failure)));
                connection.close();
                throw new CompletionException(unwrap(failure));
            }
            diagnostic.debug("Direct transfer connection for " + token + " to "
                    + connection.getIpEndpoint()
                    + " established. (type: " + connection.getType()
                    + ", id: " + connection.getId() + ")");
            return connection;
        });
    }

    private CompletableFuture<Connection> getTransferConnectionOutboundIndirectAsync(
            String username, int token, CancellationToken cancellationToken) {
        diagnostic.debug("Soliciting indirect transfer connection to " + username + " with token " + token);
        int solicitationToken = client.getNextToken();
        pendingSolicitations.putIfAbsent(solicitationToken, username);
        return client.getServerConnection()
                .writeAsync(
                        new ConnectToPeerRequest(solicitationToken, username, Constants.ConnectionType.TRANSFER),
                        cancellationToken)
                .thenCompose(ignored -> client.getWaiter()
                        .waitAsync(
                                new WaitKey(Constants.WaitKey.SOLICITED_PEER_CONNECTION, username, solicitationToken),
                                Connection.class,
                                client.getOptions()
                                        .getTransferConnectionOptions()
                                        .getConnectTimeout(),
                                cancellationToken))
                .thenApply(accepted -> {
                    try {
                        Connection connection = connectionFactory.getTransferConnection(
                                accepted.getIpEndpoint(),
                                client.getOptions().getTransferConnectionOptions(),
                                accepted.handoffTcpClient());
                        diagnostic.debug("Indirect transfer connection to " + username + " ("
                                + accepted.getIpEndpoint()
                                + ") handed off. (old: " + accepted.getId()
                                + ", new: " + connection.getId() + ")");
                        connection.setType(ConnectionTypes.OUTBOUND.or(ConnectionTypes.INDIRECT));
                        connection.addDisconnectedListener(
                                (sender, eventArgs) -> diagnostic.debug("Transfer connection for token " + token + " ("
                                        + accepted.getIpEndpoint()
                                        + ") disconnected: "
                                        + disconnectMessage(eventArgs) + ". (type: "
                                        + connection.getType() + ", id: "
                                        + connection.getId() + ")"));
                        diagnostic.debug("Indirect transfer connection for " + token + " ("
                                + connection.getIpEndpoint()
                                + ") established. (type: "
                                + connection.getType() + ", id: "
                                + connection.getId() + ")");
                        return connection;
                    } finally {
                        accepted.close();
                    }
                })
                .handle((connection, failure) -> {
                    pendingSolicitations.remove(solicitationToken, username);
                    if (failure != null) {
                        diagnostic.debug("Failed to establish an indirect transfer "
                                + "connection to " + username + " with token "
                                + token + ": " + message(unwrap(failure)));
                        throw new CompletionException(unwrap(failure));
                    }
                    return connection;
                });
    }

    private void attachPeerMessageListeners(MessageConnection connection) {
        PeerMessageHandler handler = client.getPeerMessageHandler();
        connection.addMessageReadListener(handler::handleMessageRead);
        connection.addMessageReceivedListener(handler::handleMessageReceived);
        connection.addMessageWrittenListener(handler::handleMessageWritten);
    }

    private void messageConnectionDisconnected(Connection sender, ConnectionDisconnectedEventArgs eventArgs) {
        MessageConnection connection = (MessageConnection) sender;
        diagnostic.debug("Message connection to " + connection.getUsername() + " ("
                + connection.getIpEndpoint() + ") disconnected. (type: "
                + connection.getType() + ", id: " + connection.getId()
                + ")");
        if (messageConnections.remove(connection.getUsername(), CompletableFuture.completedFuture(connection))) {
            diagnostic.debug("Removed message connection record for "
                    + connection.getKey().getUsername() + " ("
                    + connection.getIpEndpoint() + ") (type: "
                    + connection.getType() + ", id: "
                    + connection.getId() + ")");
        } else {
            CompletableFuture<MessageConnection> cached = messageConnections.get(connection.getUsername());
            if (cached != null
                    && cached.getNow(null) == connection
                    && messageConnections.remove(connection.getUsername(), cached)) {
                diagnostic.debug("Removed message connection record for "
                        + connection.getKey().getUsername() + " ("
                        + connection.getIpEndpoint() + ") (type: "
                        + connection.getType() + ", id: "
                        + connection.getId() + ")");
            }
        }
        connection.close();
        diagnostic.debug("Message connection cache now contains " + messageConnections.size() + " connections.");
    }

    private void messageConnectionProvisionalDisconnected(
            Connection sender, ConnectionDisconnectedEventArgs eventArgs) {
        sender.close();
    }

    private void raiseDiagnostic(DiagnosticEvent eventArgs) {
        diagnosticListeners.forEach(listener -> listener.handle(this, eventArgs));
    }

    private static <T> CompletableFuture<T> invoke(Supplier<CompletableFuture<T>> supplier) {
        try {
            return supplier.get();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static <T> CompletableFuture<Winner<T>> firstSuccessful(
            CompletableFuture<T> first, CompletableFuture<T> second) {
        CompletableFuture<Winner<T>> result = new CompletableFuture<>();
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        first.whenComplete((value, failure) -> {
            if (failure == null) {
                result.complete(new Winner<>(value, first));
            } else if (firstFailure.getAndSet(unwrap(failure)) != null) {
                result.completeExceptionally(unwrap(failure));
            }
        });
        second.whenComplete((value, failure) -> {
            if (failure == null) {
                result.complete(new Winner<>(value, second));
            } else if (firstFailure.getAndSet(unwrap(failure)) != null) {
                result.completeExceptionally(unwrap(failure));
            }
        });
        return result;
    }

    private static int littleEndianInteger(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    private static byte[] littleEndianBytes(int value) {
        return ByteBuffer.allocate(4)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(value)
                .array();
    }

    private static CancellationToken token(CancellationToken token) {
        return token == null ? CancellationToken.none() : token;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static String message(Throwable failure) {
        return failure.getMessage() == null ? "" : failure.getMessage();
    }

    private static String disconnectMessage(ConnectionDisconnectedEventArgs eventArgs) {
        if (eventArgs.getException() != null) {
            return message(eventArgs.getException());
        }
        return eventArgs.getMessage();
    }

    private record Winner<T>(T value, CompletableFuture<T> source) {}

    private static final class LinkedCancellation implements AutoCloseable {
        private final CancellationTokenSource source = new CancellationTokenSource();
        private final CancellationRegistration registration;

        private LinkedCancellation(CancellationToken parent) {
            registration = DefaultPeerConnectionManager.token(parent).register(source::cancel);
        }

        private CancellationToken token() {
            return source.getToken();
        }

        private void cancel() {
            source.cancel();
        }

        @Override
        public void close() {
            registration.close();
            source.close();
        }
    }
}
