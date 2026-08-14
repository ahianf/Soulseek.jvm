// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import dev.slsk.CancellationController;
import dev.slsk.CancellationSignal;
import dev.slsk.CancellationSubscription;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.internal.ServerLink;
import dev.slsk.internal.common.Constants;
import dev.slsk.internal.common.NetworkExecutor;
import dev.slsk.internal.common.TokenFactory;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
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
import dev.slsk.internal.options.SoulseekClientOptions;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Everything this client's connections to peers are.
 *
 * <p>The message connection to each peer and the cell that dedupes it, the
 * transfer connections, and the direct-versus-indirect race that establishes
 * either — a peer we can reach and a peer who can only reach us are the same
 * peer, and which of the two arms wins is nobody else's business.
 *
 * <p>This was {@code DefaultPeerConnectionManager}, reaching back through a
 * six-member interface onto the engine for the options, our username, a token,
 * the correlator, the server connection and the peer message handler. Every one
 * of those was a one-line accessor, which is a service locator with an
 * interface's name on it. They are constructor arguments now, and the interface
 * is gone.
 */
public final class PeerNetwork implements PeerConnectionManager {

    /**
     * How long a lapsed solicitation stays resolvable, in milliseconds.
     *
     * <p>Long enough to cover a peer that answered just past the deadline,
     * short enough that the map does not accumulate. See {@link
     * #retainSolicitationForLateAnswer}.
     */
    private static final long LATE_SOLICITATION_GRACE_MILLIS = 20_000;

    /**
     * The live options, not a snapshot of them.
     *
     * <p>Reconfiguring a running client replaces the options object, and a
     * connection established afterwards must be established under the new
     * timeouts.
     */
    private final Supplier<SoulseekClientOptions> options;

    private final ServerLink server;
    private final Waiter waiter;
    private final TokenFactory tokens;
    private final PeerMessageHandler peerMessages;
    private final ConnectionFactory connectionFactory;
    private final DiagnosticSink diagnostic;
    private final CopyOnWriteArrayList<DiagnosticEventListener> diagnosticListeners = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<String, ConnectionCell> messageConnections = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CancellationController> pendingInboundIndirectConnections =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, String> pendingSolicitations = new ConcurrentHashMap<>();
    private final ConnectionEventListener<ConnectionDisconnectedEvent> disconnectedListener =
            this::messageConnectionDisconnected;
    private final ConnectionEventListener<ConnectionDisconnectedEvent> provisionalDisconnectedListener =
            this::messageConnectionProvisionalDisconnected;
    private final AtomicBoolean disposed = new AtomicBoolean();

    /** Creates a peer network with default collaborators. */
    public PeerNetwork(
            Supplier<SoulseekClientOptions> options,
            ServerLink server,
            Waiter waiter,
            TokenFactory tokens,
            PeerMessageHandler peerMessages,
            ConnectionFactory connectionFactory) {
        this(options, server, waiter, tokens, peerMessages, connectionFactory, null);
    }

    /** Creates a peer network. */
    public PeerNetwork(
            Supplier<SoulseekClientOptions> options,
            ServerLink server,
            Waiter waiter,
            TokenFactory tokens,
            PeerMessageHandler peerMessages,
            ConnectionFactory connectionFactory,
            DiagnosticSink diagnosticFactory) {
        this.options = Objects.requireNonNull(options, "options");
        this.server = Objects.requireNonNull(server, "server");
        this.waiter = Objects.requireNonNull(waiter, "waiter");
        this.tokens = Objects.requireNonNull(tokens, "tokens");
        this.peerMessages = Objects.requireNonNull(peerMessages, "peerMessages");
        this.connectionFactory = Objects.requireNonNull(connectionFactory, "connectionFactory");
        diagnostic = diagnosticFactory == null
                ? new FilteringDiagnosticSink(options.get().getMinimumDiagnosticLevel(), this::raiseDiagnostic)
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
     * and an attempt does not settle until the peer answers or the timeout
     * expires — so waiting on them, as this used to, made reading the list cost
     * as much as making a connection, and throw when one failed.
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
        for (ConnectionCell cell : messageConnections.values()) {
            MessageConnection connection = cell.peek();
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
        Objects.requireNonNull(incomingConnection, "incomingConnection");
        // A peer that connects to us wins over whatever we had for it, so this
        // claims the entry outright rather than waiting to see what is there.
        ConnectionCell cell = new ConnectionCell();
        ConnectionCell superseded = messageConnections.put(username, cell);

        try {
            guardOpen(username, cell);
            MessageConnection established =
                    establishIncomingMessageConnection(username, incomingConnection, superseded);
            cell.settle(established);
            guardOpen(username, cell);
            evictIfDeadOnArrival(username, cell, established);
        } catch (Throwable failure) {
            Throwable cause = unwrap(failure);
            cell.fail(cause);
            String message = "Failed to establish an inbound message connection to "
                    + username + " (" + incomingConnection.getIpEndpoint()
                    + "): " + message(cause);
            diagnostic.debug(
                    message + " (type: " + incomingConnection.getType() + ", id: " + incomingConnection.getId() + ")");
            diagnostic.debug("Purging message connection cache of failed connection "
                    + "to " + username + " ("
                    + incomingConnection.getIpEndpoint() + ").");
            messageConnections.remove(username, cell);
            throw new CompletionException(new ConnectionException(message, cause));
        }
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
        Wait<Connection> indirectWait = waiter.register(
                new WaitKey(Constants.WaitKey.INDIRECT_TRANSFER, username, filename, remoteToken),
                Connection.class,
                options.get().getTransferConnectionOptions().getConnectTimeout(),
                indirectCancellation.token());
        Wait<Connection> directWait = waiter.register(
                new WaitKey(Constants.WaitKey.DIRECT_TRANSFER, username, remoteToken),
                Connection.class,
                options.get().getTransferConnectionOptions().getConnectTimeout(),
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
            // A cancelled wait is the caller's own doing, not a failed
            // connection. The C# source lets cancellation propagate and runs
            // its second-chance connection only on ConnectionException;
            // collapsing the two here made cancelling a waiting download dial
            // a pointless second-chance with an already-cancelled signal and
            // end ERRORED instead of CANCELLED.
            if (unwrap(failure) instanceof CancellationException cancelled) {
                throw cancelled;
            }
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
        ConnectionCell cell = messageConnections.get(username);
        if (cell == null) {
            return null;
        }

        MessageConnection connection;
        try {
            connection = cell.await();
        } catch (Throwable failure) {
            diagnostic.debug(
                    "Failed to retrieve cached message connection to " + username + ": " + message(unwrap(failure)));
            return null;
        }
        diagnostic.debug("Retrieved cached message connection to "
                + connection.getUsername() + " ("
                + connection.getIpEndpoint() + ") (type: "
                + connection.getType() + ", id: "
                + connection.getId() + ")");
        return connection;
    }

    @Override
    public MessageConnection getOrAddMessageConnection(ConnectToPeerResponse connectToPeerResponse) {
        String username = connectToPeerResponse.getUsername();
        ConnectionCell claim = new ConnectionCell();
        ConnectionCell cached = messageConnections.putIfAbsent(username, claim);
        ConnectionCell entry = cached == null ? claim : cached;

        try {
            guardOpen(username, entry);
            if (cached != null) {
                MessageConnection connection = entry.await();
                diagnostic.debug("Retrieved cached message connection to "
                        + username + " ("
                        + connectToPeerResponse.getIpEndpoint() + ") (type: "
                        + connection.getType() + ", id: "
                        + connection.getId() + ")");
                return connection;
            }
            MessageConnection connection = establishInboundIndirectMessageConnection(connectToPeerResponse);
            entry.settle(connection);
            guardOpen(username, entry);
            evictIfDeadOnArrival(username, entry, connection);
            return connection;
        } catch (Throwable failure) {
            Throwable cause = unwrap(failure);
            entry.fail(cause);
            String message = "Failed to establish an inbound indirect message "
                    + "connection to " + username + " ("
                    + connectToPeerResponse.getIpEndpoint() + "): " + message(cause);
            diagnostic.debug(message);
            if (!(cause instanceof CancellationException)) {
                diagnostic.debug("Purging message connection cache of failed connection "
                        + "to " + username + " ("
                        + connectToPeerResponse.getIpEndpoint() + ").");
                // Only ever the entry this attempt was waiting on. A direct
                // connection that superseded it has already replaced the entry,
                // and purging that one is what the cache used to have to detect
                // afterwards and undo.
                messageConnections.remove(username, entry);
            }
            throw new CompletionException(new ConnectionException(message, cause));
        }
    }

    @Override
    public MessageConnection getOrAddMessageConnection(
            String username, InetSocketAddress ipEndpoint, CancellationSignal cancellationSignal) {
        return getOrAddMessageConnection(username, ipEndpoint, tokens.nextToken(), cancellationSignal);
    }

    /**
     * Returns the message connection to a peer, establishing one if there is
     * none.
     *
     * <p>The cell is claimed before the connection exists, which is what makes
     * this deduplicate: the caller that claims it establishes the connection,
     * and every caller that arrives while that is still in flight waits on the
     * same cell instead of opening a second socket to the same peer.
     */
    @Override
    public MessageConnection getOrAddMessageConnection(
            String username,
            InetSocketAddress ipEndpoint,
            int solicitationToken,
            CancellationSignal cancellationSignal) {
        ConnectionCell claim = new ConnectionCell();
        ConnectionCell cached = messageConnections.putIfAbsent(username, claim);
        ConnectionCell entry = cached == null ? claim : cached;

        try {
            guardOpen(username, entry);
            if (cached != null) {
                MessageConnection connection = entry.await();
                diagnostic.debug("Retrieved cached message connection to " + username
                        + " (" + ipEndpoint + ") (type: "
                        + connection.getType() + ", id: "
                        + connection.getId() + ")");
                return connection;
            }
            MessageConnection connection =
                    establishRacingMessageConnection(username, ipEndpoint, solicitationToken, cancellationSignal);
            entry.settle(connection);
            guardOpen(username, entry);
            evictIfDeadOnArrival(username, entry, connection);
            return connection;
        } catch (Throwable failure) {
            Throwable cause = unwrap(failure);
            entry.fail(cause);
            diagnostic.debug("Purging message connection cache of failed connection " + "to " + username + " ("
                    + ipEndpoint + ").");
            messageConnections.remove(username, entry);
            throw new CompletionException(cause);
        }
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
            // As above: cancellation is the caller's, not the connection's,
            // and must classify as itself.
            if (unwrap(failure) instanceof CancellationException cancelled) {
                throw cancelled;
            }
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
                        new PeerInit(server.username(), Constants.ConnectionType.TRANSFER, token).toByteArray(),
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

    @Override
    public TransferConnectionResult getTransferConnection(String username, int token, Connection incomingConnection) {
        diagnostic.debug("Inbound transfer connection to " + username + " ("
                + incomingConnection.getIpEndpoint() + ") for token " + token
                + " accepted. (type: " + incomingConnection.getType()
                + ", id: " + incomingConnection.getId());
        Connection connection = connectionFactory.getTransferConnection(
                incomingConnection.getIpEndpoint(),
                options.get().getTransferConnectionOptions(),
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
    }

    @Override
    public TransferConnectionResult getTransferConnection(ConnectToPeerResponse response) {
        diagnostic.debug("Attempting inbound indirect transfer connection to "
                + response.getUsername() + " (" + response.getIpEndpoint()
                + ") for token " + response.getToken());
        Connection connection = connectionFactory.getTransferConnection(
                response.getIpEndpoint(), options.get().getTransferConnectionOptions());
        connection.setType(ConnectionTypes.INBOUND.or(ConnectionTypes.INDIRECT));
        connection.addDisconnectedListener(
                (sender, eventData) -> diagnostic.debug("Transfer connection to " + response.getUsername() + " ("
                        + response.getIpEndpoint() + ") for token "
                        + response.getToken() + " disconnected: "
                        + disconnectMessage(eventData) + ". (type: "
                        + connection.getType() + ", id: "
                        + connection.getId() + ")"));

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
    }

    @Override
    public void removeAndDisposeAll() {
        pendingSolicitations.clear();
        pendingInboundIndirectConnections.clear();
        messageConnections.forEach((username, cell) -> {
            if (messageConnections.remove(username, cell)) {
                cell.closeWhenSettled();
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

    private MessageConnection establishIncomingMessageConnection(
            String username, Connection incomingConnection, ConnectionCell superseded) {
        diagnostic.debug("Inbound message connection to " + username + " ("
                + incomingConnection.getIpEndpoint()
                + ") accepted. (type: " + incomingConnection.getType()
                + ", id: " + incomingConnection.getId() + ")");
        MessageConnection connection = connectionFactory.getMessageConnection(
                username,
                incomingConnection.getIpEndpoint(),
                options.get().getPeerConnectionOptions(),
                incomingConnection.handoffTcpClient());
        diagnostic.debug("Inbound message connection to " + username + " ("
                + connection.getIpEndpoint() + ") handed off. (old: "
                + incomingConnection.getId() + ", new: "
                + connection.getId() + ")");
        incomingConnection.close();
        connection.setType(ConnectionTypes.INBOUND.or(ConnectionTypes.DIRECT));
        attachPeerMessageListeners(connection);
        connection.addDisconnectedListener(disconnectedListener);

        if (superseded != null) {
            CancellationController pending = pendingInboundIndirectConnections.get(username);
            if (pending != null) {
                diagnostic.debug("Cancelling pending inbound indirect message connection " + "to " + username);
                pending.cancel();
            }
            // An attempt still in flight owns the connection it is about to
            // produce, so it has to finish before that connection can be let
            // go of. Cancelling the pending indirect above is what makes it
            // finish promptly; every other attempt is bounded by its own
            // connect timeout.
            MessageConnection old = superseded.awaitQuietly();
            if (old != null) {
                old.removeDisconnectedListener(disconnectedListener);
                diagnostic.debug("Superseding cached message connection to "
                        + username + " (" + old.getIpEndpoint()
                        + ") (old: " + old.getId() + ", new: "
                        + connection.getId());
            }
        }

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
    }

    private MessageConnection establishInboundIndirectMessageConnection(ConnectToPeerResponse response) {
        diagnostic.debug("Attempting inbound indirect message connection to "
                + response.getUsername() + " (" + response.getIpEndpoint()
                + ") for token " + response.getToken());
        MessageConnection connection = connectionFactory.getMessageConnection(
                response.getUsername(), response.getIpEndpoint(), options.get().getPeerConnectionOptions());
        connection.setType(ConnectionTypes.INBOUND.or(ConnectionTypes.INDIRECT));
        attachPeerMessageListeners(connection);
        CancellationController cancellation = new CancellationController();
        pendingInboundIndirectConnections.put(response.getUsername(), cancellation);

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
                        new PeerInit(server.username(), Constants.ConnectionType.PEER, tokens.nextToken())
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
                username, ipEndpoint, options.get().getPeerConnectionOptions());
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
        boolean answered = false;
        try {
            // Registered before the request that provokes it: the peer can be
            // knocking on the listener before this write returns.
            Wait<Connection> wait = waiter.register(
                    new WaitKey(Constants.WaitKey.SOLICITED_PEER_CONNECTION, username, solicitationToken),
                    Connection.class,
                    options.get().getPeerConnectionOptions().getIndirectSolicitationTimeout(),
                    cancellationSignal);
            server.write(
                    new ConnectToPeerRequest(solicitationToken, username, Constants.ConnectionType.PEER),
                    cancellationSignal);
            Connection accepted = wait.await();
            answered = true;
            try {
                MessageConnection connection = connectionFactory.getMessageConnection(
                        username,
                        accepted.getIpEndpoint(),
                        options.get().getPeerConnectionOptions(),
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
            if (answered) {
                pendingSolicitations.remove(solicitationToken, username);
            } else {
                retainSolicitationForLateAnswer(solicitationToken, username);
            }
        }
    }

    /**
     * Keeps a lapsed solicitation resolvable for a while longer.
     *
     * <p>The token is the only thing that names the peer behind an inbound
     * PierceFirewall, so dropping it the instant the wait expired turned every
     * late answer into an unknown one: the listener could not say who had
     * connected, and closed a connection the peer had just successfully made.
     * One recorded session did that 2,598 times.
     *
     * <p>The wait is gone either way — whatever provoked the solicitation has
     * already failed — but the connection need not be. {@code
     * DefaultListenerHandler} adopts it into the cache instead, so the next
     * attempt at this peer starts from a connection that is already open.
     * Transfer solicitations get no such grace: a transfer connection with no
     * transfer waiting on it has nothing to carry.
     *
     * <p>The removal parks a virtual thread rather than taking a scheduler
     * dependency, which is how the rest of this class waits.
     */
    private void retainSolicitationForLateAnswer(int solicitationToken, String username) {
        try {
            NetworkExecutor.executor().execute(() -> {
                try {
                    Thread.sleep(LATE_SOLICITATION_GRACE_MILLIS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                } finally {
                    pendingSolicitations.remove(solicitationToken, username);
                }
            });
        } catch (Throwable dispatchFailed) {
            // This is called from a finally. Throwing here would replace the
            // failure that finally is unwinding, and a grace window is not
            // worth that: drop it and expire the token now.
            pendingSolicitations.remove(solicitationToken, username);
        }
    }

    private Connection establishOutboundDirectTransferConnection(
            InetSocketAddress ipEndpoint, int token, CancellationSignal cancellationSignal) {
        diagnostic.debug("Attempting direct transfer connection for token " + token + " to " + ipEndpoint);
        Connection connection = connectionFactory.getTransferConnection(
                ipEndpoint, options.get().getTransferConnectionOptions());
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
        int solicitationToken = tokens.nextToken();
        pendingSolicitations.putIfAbsent(solicitationToken, username);
        try {
            Wait<Connection> wait = waiter.register(
                    new WaitKey(Constants.WaitKey.SOLICITED_PEER_CONNECTION, username, solicitationToken),
                    Connection.class,
                    options.get().getTransferConnectionOptions().getIndirectSolicitationTimeout(),
                    cancellationSignal);
            server.write(
                    new ConnectToPeerRequest(solicitationToken, username, Constants.ConnectionType.TRANSFER),
                    cancellationSignal);
            Connection accepted = wait.await();
            try {
                Connection connection = connectionFactory.getTransferConnection(
                        accepted.getIpEndpoint(),
                        options.get().getTransferConnectionOptions(),
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
        connection.addMessageReadListener(peerMessages::handleMessageRead);
        connection.addMessageReceivedListener(peerMessages::handleMessageReceived);
        connection.addMessageWrittenListener(peerMessages::handleMessageWritten);
    }

    /**
     * Refuses a cache insertion once the network is closed, undoing the claim
     * it made.
     *
     * <p>{@code removeAndDisposeAll} iterates the map weakly, so a cell put in
     * racing the sweep — or after it — was never disposed and never removed: a
     * shutdown-time place-in-queue poll or upload-failure notification could
     * repopulate the cache of a closed network, which is how a live run's last
     * cache census read 1 rather than 0.
     */
    private void guardOpen(String username, ConnectionCell entry) {
        if (disposed.get()) {
            messageConnections.remove(username, entry);
            entry.closeWhenSettled();
            throw new CompletionException(new ConnectionException("The peer network is closed"));
        }
    }

    /**
     * Evicts a connection that died before its cell settled.
     *
     * <p>The disconnected listener is attached before the cell settles, and it
     * evicts only when {@code peek()} answers with this connection — which an
     * unsettled cell cannot. A peer that drops in that window would otherwise
     * leave a closed connection cached forever, its disconnect event already
     * spent, handed to every later caller until an inbound reconnect replaces
     * it.
     */
    private void evictIfDeadOnArrival(String username, ConnectionCell entry, MessageConnection connection) {
        if (connection.getState() != dev.slsk.internal.network.tcp.ConnectionState.CONNECTED
                && messageConnections.remove(username, entry)) {
            diagnostic.debug("Removed message connection record for " + username
                    + "; the connection dropped before its cache entry settled (id: "
                    + connection.getId() + ")");
        }
    }

    private void messageConnectionDisconnected(Connection sender, ConnectionDisconnectedEvent eventData) {
        MessageConnection connection = (MessageConnection) sender;
        diagnostic.debug("Message connection to " + connection.getUsername() + " ("
                + connection.getIpEndpoint() + ") disconnected. (type: "
                + connection.getType() + ", id: " + connection.getId()
                + ")");
        // Only if this is still the connection on record. A peer that
        // reconnects has already replaced the entry, and the disconnect of the
        // connection it replaced must not evict its successor.
        ConnectionCell cell = messageConnections.get(connection.getUsername());
        if (cell != null && cell.peek() == connection && messageConnections.remove(connection.getUsername(), cell)) {
            diagnostic.debug("Removed message connection record for "
                    + connection.getKey().getUsername() + " ("
                    + connection.getIpEndpoint() + ") (type: "
                    + connection.getType() + ", id: "
                    + connection.getId() + ")");
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
            registration = PeerNetwork.token(parent).register(source::cancel);
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
