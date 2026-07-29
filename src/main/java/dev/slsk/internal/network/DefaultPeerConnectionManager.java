// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import dev.slsk.CancellationController;
import dev.slsk.CancellationSignal;
import dev.slsk.CancellationSubscription;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.internal.common.Constants;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.NetworkExecutor;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.diagnostics.DiagnosticEvent;
import dev.slsk.internal.diagnostics.DiagnosticEventListener;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.diagnostics.FilteringDiagnosticSink;
import dev.slsk.internal.messaging.handlers.PeerMessageHandler;
import dev.slsk.internal.messaging.messages.ConnectToPeerRequest;
import dev.slsk.internal.messaging.messages.ConnectToPeerResponse;
import dev.slsk.internal.messaging.messages.PeerInit;
import dev.slsk.internal.messaging.messages.PierceFirewall;
import dev.slsk.internal.network.tcp.Connection;
import dev.slsk.internal.network.tcp.ConnectionDisconnectedEvent;
import dev.slsk.internal.network.tcp.ConnectionEventListener;
import dev.slsk.internal.network.tcp.ConnectionTypes;
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
    private final ConcurrentHashMap<String, CancellationController> pendingInboundIndirectConnections =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, String> pendingSolicitations = new ConcurrentHashMap<>();
    private final ConnectionEventListener<ConnectionDisconnectedEvent> disconnectedListener =
            this::messageConnectionDisconnected;
    private final ConnectionEventListener<ConnectionDisconnectedEvent> provisionalDisconnectedListener =
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

    /**
     * Returns the peers there is currently a message connection to.
     *
     * <p>Established ones only. The map holds attempts as well as connections,
     * and an attempt is a future that does not complete until the peer answers
     * or the timeout expires — so joining them, as this used to, made reading
     * the list cost as much as making a connection, and throw when one failed.
     *
     * <p>That is the wrong shape for something whose only callers are a metrics
     * gauge and a diagnostic. A connection that has not been established is not
     * a connection, and asking how many there are must never be the thing that
     * waits for one. It is also why this deviates from the C# property it was
     * ported from, which blocks on {@code .Result} the same way.
     *
     * @return the established peer connections; never blocks, never throws
     */
    @Override
    public List<PeerEndpoint> getMessageConnections() {
        List<PeerEndpoint> snapshot = new ArrayList<>();
        for (CompletableFuture<MessageConnection> future : messageConnections.values()) {
            if (!future.isDone() || future.isCompletedExceptionally()) {
                continue;
            }
            MessageConnection connection = future.getNow(null);
            if (connection != null) {
                snapshot.add(new PeerEndpoint(connection.getUsername(), connection.getIpEndpoint()));
            }
        }
        return List.copyOf(snapshot);
    }

    @Override
    public Map<Integer, String> getPendingSolicitations() {
        return Map.copyOf(pendingSolicitations);
    }

    @Override
    public void addOrUpdateMessageConnection(String username, Connection incomingConnection) {
        await(addOrUpdateMessageConnectionAsync(username, incomingConnection));
    }

    @Override
    public Connection awaitTransferConnection(
            String username, String filename, int remoteToken, CancellationSignal cancellationSignal) {
        LinkedCancellation directCancellation = new LinkedCancellation(cancellationSignal);
        LinkedCancellation indirectCancellation = new LinkedCancellation(cancellationSignal);
        diagnostic.debug("Waiting for a direct or indirect transfer connection from "
                + username + " with remote token " + remoteToken + " for "
                + filename);

        // Both waits are registered before either is awaited, so a peer that
        // arrives on one path while the other is still being set up is caught.
        Wait<Connection> indirectWait = client.getWaiter()
                .register(
                        new WaitKey(Constants.WaitKey.INDIRECT_TRANSFER, username, filename, remoteToken),
                        Connection.class,
                        client.getOptions().getTransferConnectionOptions().getConnectTimeout(),
                        indirectCancellation.token());
        Wait<Connection> directWait = client.getWaiter()
                .register(
                        new WaitKey(Constants.WaitKey.DIRECT_TRANSFER, username, remoteToken),
                        Connection.class,
                        client.getOptions().getTransferConnectionOptions().getConnectTimeout(),
                        directCancellation.token());

        // Awaiting two waits at once is the whole reason this dispatches
        // threads; the peer answers on exactly one of them and we cannot know
        // which.
        FirstSuccess.Winner<Connection> winner;
        try {
            winner = FirstSuccess.race(directWait::await, indirectWait::await);
        } catch (Throwable failure) {
            directCancellation.close();
            indirectCancellation.close();
            String message = "Failed to establish a direct or indirect transfer "
                    + "connection to " + username
                    + " with remote token " + remoteToken + " for "
                    + filename;
            diagnostic.debug(message);
            throw new CompletionException(new ConnectionException(message));
        }

        boolean directWon = winner.first();
        Connection connection = winner.value();
        diagnostic.debug((directWon ? "Direct" : "Indirect")
                + " transfer connection to " + username + " ("
                + connection.getIpEndpoint()
                + ") with remote token " + remoteToken + " for " + filename
                + " established first, attempting to cancel "
                + (directWon ? "indirect" : "direct") + " connection.");
        (directWon ? indirectCancellation : directCancellation).cancel();
        directCancellation.close();
        indirectCancellation.close();
        diagnostic.debug("Transfer connection to " + username + " ("
                + connection.getIpEndpoint()
                + ") with remote token " + remoteToken + " for "
                + filename + " established. (type: "
                + connection.getType() + ", id: "
                + connection.getId() + ")");
        return connection;
    }

    @Override
    public MessageConnection getCachedMessageConnection(String username) {
        return await(getCachedMessageConnectionAsync(username));
    }

    @Override
    public MessageConnection getOrAddMessageConnection(ConnectToPeerResponse connectToPeerResponse) {
        return await(getOrAddMessageConnectionAsync(connectToPeerResponse));
    }

    @Override
    public MessageConnection getOrAddMessageConnection(
            String username, InetSocketAddress ipEndpoint, CancellationSignal cancellationSignal) {
        return await(getOrAddMessageConnectionAsync(username, ipEndpoint, cancellationSignal));
    }

    @Override
    public MessageConnection getOrAddMessageConnection(
            String username,
            InetSocketAddress ipEndpoint,
            int solicitationToken,
            CancellationSignal cancellationSignal) {
        return await(getOrAddMessageConnectionAsync(username, ipEndpoint, solicitationToken, cancellationSignal));
    }

    @Override
    public TransferConnectionResult getTransferConnection(String username, int token, Connection incomingConnection) {
        return await(getTransferConnectionAsync(username, token, incomingConnection));
    }

    @Override
    public TransferConnectionResult getTransferConnection(ConnectToPeerResponse connectToPeerResponse) {
        return await(getTransferConnectionAsync(connectToPeerResponse));
    }

    @Override
    public Connection getTransferConnection(
            String username, InetSocketAddress ipEndpoint, int token, CancellationSignal cancellationSignal) {
        LinkedCancellation directCancellation = new LinkedCancellation(cancellationSignal);
        LinkedCancellation indirectCancellation = new LinkedCancellation(cancellationSignal);
        diagnostic.debug("Attempting simultaneous direct and indirect transfer " + "connections to " + username + " ("
                + ipEndpoint + ")");

        FirstSuccess.Winner<Connection> winner;
        try {
            winner = FirstSuccess.race(
                    () -> establishOutboundDirectTransferConnection(ipEndpoint, token, directCancellation.token()),
                    () -> establishOutboundIndirectTransferConnection(username, token, indirectCancellation.token()));
        } catch (Throwable failure) {
            directCancellation.close();
            indirectCancellation.close();
            String message = "Failed to establish a direct or indirect transfer "
                    + "connection to " + username + " (" + ipEndpoint
                    + ")";
            diagnostic.debug(message);
            throw new CompletionException(new ConnectionException(message));
        }

        boolean directWon = winner.first();
        Connection connection = winner.value();
        diagnostic.debug((directWon ? "Direct" : "Indirect")
                + " transfer connection to " + username + " (" + ipEndpoint
                + ") established first, attempting to cancel "
                + (directWon ? "indirect" : "direct") + " connection.");
        (directWon ? indirectCancellation : directCancellation).cancel();
        try {
            if (directWon) {
                connection.write(
                        new PeerInit(client.getUsername(), Constants.ConnectionType.TRANSFER, token).toByteArray(),
                        token(cancellationSignal));
            }
            connection.write(littleEndianBytes(token), token(cancellationSignal));
        } catch (Throwable failure) {
            Throwable cause = unwrap(failure);
            String message = "Failed to negotiate transfer connection to "
                    + username + " (" + ipEndpoint + "): "
                    + message(cause);
            diagnostic.debug(message + " (type: " + connection.getType() + ", id: " + connection.getId() + ")");
            connection.close();
            throw new CompletionException(new ConnectionException(message, cause));
        } finally {
            directCancellation.close();
            indirectCancellation.close();
        }
        diagnostic.debug("Transfer connection to " + username + " ("
                + ipEndpoint + ") established. (type: "
                + connection.getType() + ", id: "
                + connection.getId() + ")");
        return connection;
    }

    /**
     * Waits for one of the cached or raced operations below.
     *
     * <p>The last future in this class that a caller can see, and it stops
     * here. Two things underneath genuinely need one — the per-user cache that
     * deduplicates concurrent establishment, and the direct/indirect race — and
     * D11 and D12 replace both with a connection cell and a first-success
     * helper. Until they do, this is where the shape converts.
     */
    private static <T> T await(CompletableFuture<T> operation) {
        try {
            return operation.join();
        } catch (Throwable failure) {
            throw Failures.propagate(unwrap(failure));
        }
    }

    private CompletableFuture<Void> addOrUpdateMessageConnectionAsync(String username, Connection incomingConnection) {
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

    private CompletableFuture<MessageConnection> getCachedMessageConnectionAsync(String username) {
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

    private CompletableFuture<MessageConnection> getOrAddMessageConnectionAsync(ConnectToPeerResponse response) {
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

    private CompletableFuture<MessageConnection> getOrAddMessageConnectionAsync(
            String username, InetSocketAddress ipEndpoint, CancellationSignal cancellationSignal) {
        return getOrAddMessageConnectionAsync(username, ipEndpoint, client.getNextToken(), cancellationSignal);
    }

    private CompletableFuture<MessageConnection> getOrAddMessageConnectionAsync(
            String username,
            InetSocketAddress ipEndpoint,
            int solicitationToken,
            CancellationSignal cancellationSignal) {
        AtomicBoolean cached = new AtomicBoolean(true);
        CompletableFuture<MessageConnection> future = messageConnections.computeIfAbsent(username, key -> {
            cached.set(false);
            // The establishment is blocking now, and the cache entry is still a
            // future, so it needs a thread to run on. D11 replaces the entry
            // with a cell and this call runs on the caller's own thread.
            return NetworkExecutor.supplyAsync(() ->
                    establishRacingMessageConnection(username, ipEndpoint, solicitationToken, cancellationSignal));
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

    private CompletableFuture<TransferConnectionResult> getTransferConnectionAsync(
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
                (sender, eventData) -> diagnostic.debug("Transfer connection to " + username + " ("
                        + connection.getIpEndpoint() + ") for token " + token
                        + " disconnected: " + disconnectMessage(eventData)
                        + ". (type: " + connection.getType() + ", id: "
                        + connection.getId() + ")"));
        diagnostic.debug("Inbound transfer connection to " + username + " ("
                + connection.getIpEndpoint() + ") for token " + token
                + " handed off. (old: " + incomingConnection.getId()
                + ", new: " + connection.getId() + ")");

        return NetworkExecutor.supplyAsync(() -> {
            byte[] bytes;
            try {
                bytes = connection.read(4);
            } catch (Throwable failure) {
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

    private CompletableFuture<TransferConnectionResult> getTransferConnectionAsync(ConnectToPeerResponse response) {
        diagnostic.debug("Attempting inbound indirect transfer connection to "
                + response.getUsername() + " (" + response.getIpEndpoint()
                + ") for token " + response.getToken());
        Connection connection = connectionFactory.getTransferConnection(
                response.getIpEndpoint(), client.getOptions().getTransferConnectionOptions());
        connection.setType(ConnectionTypes.INBOUND.or(ConnectionTypes.INDIRECT));
        connection.addDisconnectedListener(
                (sender, eventData) -> diagnostic.debug("Transfer connection to " + response.getUsername() + " ("
                        + response.getIpEndpoint() + ") for token "
                        + response.getToken() + " disconnected: "
                        + disconnectMessage(eventData) + ". (type: "
                        + connection.getType() + ", id: "
                        + connection.getId() + ")"));

        return NetworkExecutor.supplyAsync(() -> {
            byte[] bytes;
            try {
                connection.connect();
                connection.write(new PierceFirewall(response.getToken()).toByteArray());
                bytes = connection.read(4);
            } catch (Throwable failure) {
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
            CancellationController pending = pendingInboundIndirectConnections.get(username);
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
        CancellationController cancellation = new CancellationController();
        pendingInboundIndirectConnections.put(response.getUsername(), cancellation);

        return NetworkExecutor.supplyAsync(() -> {
            try {
                connection.connect(cancellation.getSignal());
                connection.write(new PierceFirewall(response.getToken()).toByteArray(), cancellation.getSignal());
            } catch (Throwable failure) {
                connection.close();
                throw new CompletionException(unwrap(failure));
            } finally {
                pendingInboundIndirectConnections.remove(response.getUsername(), cancellation);
                cancellation.close();
            }
            connection.addDisconnectedListener(disconnectedListener);
            diagnostic.debug("Message connection to " + response.getUsername() + " ("
                    + response.getIpEndpoint()
                    + ") established. (type: " + connection.getType()
                    + ", id: " + connection.getId() + ")");
            return connection;
        });
    }

    private MessageConnection establishRacingMessageConnection(
            String username,
            InetSocketAddress ipEndpoint,
            int solicitationToken,
            CancellationSignal cancellationSignal) {
        LinkedCancellation directCancellation = new LinkedCancellation(cancellationSignal);
        LinkedCancellation indirectCancellation = new LinkedCancellation(cancellationSignal);
        diagnostic.debug("Attempting simultaneous direct and indirect message " + "connections to " + username + " ("
                + ipEndpoint + ")");

        FirstSuccess.Winner<MessageConnection> winner;
        try {
            winner = FirstSuccess.race(
                    () -> establishOutboundDirectMessageConnection(username, ipEndpoint, directCancellation.token()),
                    () -> establishOutboundIndirectMessageConnection(
                            username, solicitationToken, indirectCancellation.token()));
        } catch (Throwable failure) {
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

        MessageConnection connection = winner.value();
        connection.addDisconnectedListener(disconnectedListener);
        connection.removeDisconnectedListener(provisionalDisconnectedListener);
        boolean directWon = winner.first();
        diagnostic.debug((directWon ? "Direct" : "Indirect")
                + " message connection to " + username + " (" + ipEndpoint
                + ") established first, attempting to cancel "
                + (directWon ? "indirect" : "direct") + " connection.");
        (directWon ? indirectCancellation : directCancellation).cancel();

        try {
            if (directWon) {
                connection.write(
                        new PeerInit(client.getUsername(), Constants.ConnectionType.PEER, client.getNextToken())
                                .toByteArray(),
                        token(cancellationSignal));
            } else {
                connection.startReadingContinuously();
            }
        } catch (Throwable failure) {
            Throwable cause = unwrap(failure);
            String message = "Failed to negotiate message connection to "
                    + username + " (" + ipEndpoint + "): "
                    + message(cause);
            diagnostic.debug(message + " (type: " + connection.getType() + ", id: " + connection.getId() + ")");
            connection.close();
            throw new CompletionException(new ConnectionException(message, cause));
        } finally {
            directCancellation.close();
            indirectCancellation.close();
        }
        diagnostic.debug("Message connection to " + username + " (" + ipEndpoint
                + ") established. (type: " + connection.getType()
                + ", id: " + connection.getId() + ")");
        return connection;
    }

    private MessageConnection establishOutboundDirectMessageConnection(
            String username, InetSocketAddress ipEndpoint, CancellationSignal cancellationSignal) {
        diagnostic.debug("Attempting direct message connection to " + username + " (" + ipEndpoint + ")");
        MessageConnection connection = connectionFactory.getMessageConnection(
                username, ipEndpoint, client.getOptions().getPeerConnectionOptions());
        connection.setType(ConnectionTypes.OUTBOUND.or(ConnectionTypes.DIRECT));
        attachPeerMessageListeners(connection);
        connection.addDisconnectedListener(provisionalDisconnectedListener);
        try {
            connection.connect(cancellationSignal);
        } catch (Throwable failure) {
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
    }

    private MessageConnection establishOutboundIndirectMessageConnection(
            String username, int solicitationToken, CancellationSignal cancellationSignal) {
        diagnostic.debug("Soliciting indirect message connection to " + username + " with token " + solicitationToken);
        pendingSolicitations.putIfAbsent(solicitationToken, username);
        try {
            // Registered before the request that provokes it: the peer can be
            // knocking on the listener before this write returns.
            Wait<Connection> wait = client.getWaiter()
                    .register(
                            new WaitKey(Constants.WaitKey.SOLICITED_PEER_CONNECTION, username, solicitationToken),
                            Connection.class,
                            client.getOptions().getPeerConnectionOptions().getConnectTimeout(),
                            cancellationSignal);
            client.getServerConnection()
                    .write(
                            new ConnectToPeerRequest(solicitationToken, username, Constants.ConnectionType.PEER),
                            cancellationSignal);
            Connection accepted = wait.await();
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
        } catch (Throwable failure) {
            diagnostic.debug("Failed to establish an indirect message connection to "
                    + username + " with token " + solicitationToken
                    + ": " + message(unwrap(failure)));
            throw new CompletionException(unwrap(failure));
        } finally {
            pendingSolicitations.remove(solicitationToken, username);
        }
    }

    private Connection establishOutboundDirectTransferConnection(
            InetSocketAddress ipEndpoint, int token, CancellationSignal cancellationSignal) {
        diagnostic.debug("Attempting direct transfer connection for token " + token + " to " + ipEndpoint);
        Connection connection = connectionFactory.getTransferConnection(
                ipEndpoint, client.getOptions().getTransferConnectionOptions());
        connection.setType(ConnectionTypes.OUTBOUND.or(ConnectionTypes.DIRECT));
        connection.addDisconnectedListener(
                (sender, eventData) -> diagnostic.debug("Transfer connection for token " + token + " to "
                        + ipEndpoint + " disconnected: "
                        + disconnectMessage(eventData) + ". (type: "
                        + connection.getType() + ", id: "
                        + connection.getId() + ")"));
        try {
            connection.connect(cancellationSignal);
        } catch (Throwable failure) {
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
    }

    private Connection establishOutboundIndirectTransferConnection(
            String username, int token, CancellationSignal cancellationSignal) {
        diagnostic.debug("Soliciting indirect transfer connection to " + username + " with token " + token);
        int solicitationToken = client.getNextToken();
        pendingSolicitations.putIfAbsent(solicitationToken, username);
        try {
            Wait<Connection> wait = client.getWaiter()
                    .register(
                            new WaitKey(Constants.WaitKey.SOLICITED_PEER_CONNECTION, username, solicitationToken),
                            Connection.class,
                            client.getOptions().getTransferConnectionOptions().getConnectTimeout(),
                            cancellationSignal);
            client.getServerConnection()
                    .write(
                            new ConnectToPeerRequest(solicitationToken, username, Constants.ConnectionType.TRANSFER),
                            cancellationSignal);
            Connection accepted = wait.await();
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
                        (sender, eventData) -> diagnostic.debug("Transfer connection for token " + token + " ("
                                + accepted.getIpEndpoint()
                                + ") disconnected: "
                                + disconnectMessage(eventData) + ". (type: "
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
        } catch (Throwable failure) {
            diagnostic.debug("Failed to establish an indirect transfer "
                    + "connection to " + username + " with token "
                    + token + ": " + message(unwrap(failure)));
            throw new CompletionException(unwrap(failure));
        } finally {
            pendingSolicitations.remove(solicitationToken, username);
        }
    }

    private void attachPeerMessageListeners(MessageConnection connection) {
        PeerMessageHandler handler = client.getPeerMessageHandler();
        connection.addMessageReadListener(handler::handleMessageRead);
        connection.addMessageReceivedListener(handler::handleMessageReceived);
        connection.addMessageWrittenListener(handler::handleMessageWritten);
    }

    private void messageConnectionDisconnected(Connection sender, ConnectionDisconnectedEvent eventData) {
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

    private void messageConnectionProvisionalDisconnected(Connection sender, ConnectionDisconnectedEvent eventData) {
        sender.close();
    }

    private void raiseDiagnostic(DiagnosticEvent eventData) {
        diagnosticListeners.forEach(listener -> listener.handle(this, eventData));
    }

    private static <T> CompletableFuture<T> invoke(Supplier<CompletableFuture<T>> supplier) {
        try {
            return supplier.get();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
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

    private static CancellationSignal token(CancellationSignal token) {
        return token == null ? CancellationSignal.none() : token;
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

    private static String disconnectMessage(ConnectionDisconnectedEvent eventData) {
        if (eventData.getException() != null) {
            return message(eventData.getException());
        }
        return eventData.getMessage();
    }

    private static final class LinkedCancellation implements AutoCloseable {
        private final CancellationController source = new CancellationController();
        private final CancellationSubscription registration;

        private LinkedCancellation(CancellationSignal parent) {
            registration = DefaultPeerConnectionManager.token(parent).register(source::cancel);
        }

        private CancellationSignal token() {
            return source.getSignal();
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
