// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import dev.slsk.Subscription;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.internal.ServerLink;
import dev.slsk.internal.common.Constants;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.Scheduler;
import dev.slsk.internal.common.TokenFactory;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.concurrent.CancellationController;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.concurrent.CancellationSubscription;
import dev.slsk.internal.diagnostics.DiagnosticMessage;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.diagnostics.FilteringDiagnosticSink;
import dev.slsk.internal.events.Subscriptions;
import dev.slsk.internal.messaging.handlers.PeerMessageHandler;
import dev.slsk.internal.messaging.messages.ConnectToPeerRequest;
import dev.slsk.internal.messaging.messages.ConnectToPeerResponse;
import dev.slsk.internal.messaging.messages.PeerInit;
import dev.slsk.internal.messaging.messages.PierceFirewall;
import dev.slsk.internal.network.tcp.Connection;
import dev.slsk.internal.network.tcp.ConnectionDisconnectedEvent;
import dev.slsk.internal.network.tcp.ConnectionType;
import dev.slsk.internal.options.SoulseekClientOptions;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
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
    private final Scheduler scheduler;
    private final boolean ownsScheduler;
    private final CopyOnWriteArrayList<Consumer<? super DiagnosticMessage>> diagnosticListeners =
            new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<String, ConnectionCell> messageConnections = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CancellationController> pendingInboundIndirectConnections =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, String> pendingSolicitations = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<MessageConnection, Subscription> disconnectSubscriptions =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<MessageConnection, Subscription> provisionalDisconnectSubscriptions =
            new ConcurrentHashMap<>();
    private final Consumer<ConnectionDisconnectedEvent> disconnectedListener = this::messageConnectionDisconnected;
    private final Consumer<ConnectionDisconnectedEvent> provisionalDisconnectedListener =
            this::messageConnectionProvisionalDisconnected;
    private final AtomicBoolean closed = new AtomicBoolean();

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
        this(options, server, waiter, tokens, peerMessages, connectionFactory, diagnosticFactory, null);
    }

    /** Creates a peer network sharing a caller-owned scheduler. */
    public PeerNetwork(
            Supplier<SoulseekClientOptions> options,
            ServerLink server,
            Waiter waiter,
            TokenFactory tokens,
            PeerMessageHandler peerMessages,
            ConnectionFactory connectionFactory,
            DiagnosticSink diagnosticFactory,
            Scheduler scheduler) {
        this.options = Objects.requireNonNull(options, "options");
        this.server = Objects.requireNonNull(server, "server");
        this.waiter = Objects.requireNonNull(waiter, "waiter");
        this.tokens = Objects.requireNonNull(tokens, "tokens");
        this.peerMessages = Objects.requireNonNull(peerMessages, "peerMessages");
        this.connectionFactory = Objects.requireNonNull(connectionFactory, "connectionFactory");
        ownsScheduler = scheduler == null;
        this.scheduler = scheduler == null ? new Scheduler("soulseek-peer-network") : scheduler;
        diagnostic = diagnosticFactory == null
                ? new FilteringDiagnosticSink(options.get().minimumDiagnosticLevel(), this::publishDiagnostic)
                : DiagnosticSink.forSource(diagnosticFactory, PeerNetwork.class);
    }

    @Override
    public Subscription subscribe(Consumer<? super DiagnosticMessage> listener) {
        return Subscriptions.add(diagnosticListeners, listener);
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
        } catch (Throwable cause) {
            cell.fail(cause);
            String message = "Failed to establish an inbound message connection to "
                    + username + " (" + incomingConnection.getIpEndpoint()
                    + "): " + Failures.message(cause);
            diagnostic.debug(
                    message + " (type: " + incomingConnection.getType() + ", id: " + incomingConnection.getId() + ")");
            diagnostic.debug("Purging message connection cache of failed connection "
                    + "to " + username + " ("
                    + incomingConnection.getIpEndpoint() + ").");
            messageConnections.remove(username, cell);
            throw new ConnectionException(message, cause);
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
                options.get().transferConnectionOptions().connectTimeout(),
                indirectCancellation.token());
        Wait<Connection> directWait = waiter.register(
                new WaitKey(Constants.WaitKey.DIRECT_TRANSFER, username, remoteToken),
                Connection.class,
                options.get().transferConnectionOptions().connectTimeout(),
                directCancellation.token());

        // Awaiting two waits at once is the whole reason this dispatches
        // threads; the peer answers on exactly one of them and we cannot know
        // which.
        FirstSuccess.Winner<Connection> winner;
        try {
            winner = FirstSuccess.race(scheduler.executor(), directWait::await, indirectWait::await);
        } catch (Throwable failure) {
            directCancellation.close();
            indirectCancellation.close();
            // A cancelled wait is the caller's own doing, not a failed
            // connection. The C# source lets cancellation propagate and runs
            // its second-chance connection only on ConnectionException;
            // collapsing the two here made cancelling a waiting download dial
            // a pointless second-chance with an already-cancelled signal and
            // end ERRORED instead of CANCELLED.
            if (failure instanceof CancellationException cancelled) {
                throw cancelled;
            }
            String message = "Failed to establish a direct or indirect transfer "
                    + "connection to " + username
                    + " with remote token " + remoteToken + " for "
                    + filename;
            diagnostic.debug(message);
            throw new ConnectionException(message);
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
                    "Failed to retrieve cached message connection to " + username + ": " + Failures.message(failure));
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
        } catch (Throwable cause) {
            entry.fail(cause);
            String message = "Failed to establish an inbound indirect message "
                    + "connection to " + username + " ("
                    + connectToPeerResponse.getIpEndpoint() + "): " + Failures.message(cause);
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
            throw new ConnectionException(message, cause);
        }
    }

    @Override
    public MessageConnection getOrAddMessageConnection(
            String username, InetSocketAddress ipEndpoint, CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
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
            String username, InetSocketAddress ipEndpoint, int solicitationToken, CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
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
            entry.fail(failure);
            diagnostic.debug("Purging message connection cache of failed connection " + "to " + username + " ("
                    + ipEndpoint + ").");
            messageConnections.remove(username, entry);
            throw Failures.rethrow(failure);
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
                    scheduler.executor(),
                    () -> establishOutboundDirectTransferConnection(ipEndpoint, token, directCancellation.token()),
                    () -> establishOutboundIndirectTransferConnection(username, token, indirectCancellation.token()));
        } catch (Throwable failure) {
            directCancellation.close();
            indirectCancellation.close();
            // As above: cancellation is the caller's, not the connection's,
            // and must classify as itself.
            if (failure instanceof CancellationException cancelled) {
                throw cancelled;
            }
            String message = "Failed to establish a direct or indirect transfer "
                    + "connection to " + username + " (" + ipEndpoint
                    + ")";
            diagnostic.debug(message);
            throw new ConnectionException(message);
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
        } catch (Throwable cause) {
            String message = "Failed to negotiate transfer connection to "
                    + username + " (" + ipEndpoint + "): "
                    + Failures.message(cause);
            diagnostic.debug(message + " (type: " + connection.getType() + ", id: " + connection.getId() + ")");
            connection.close();
            throw new ConnectionException(message, cause);
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
                options.get().transferConnectionOptions(),
                incomingConnection.handoffTcpClient());
        connection.setType(ConnectionType.INBOUND_DIRECT);
        connection.<ConnectionDisconnectedEvent>subscribe(
                Connection.Kind.DISCONNECTED,
                eventData -> diagnostic.debug("Transfer connection to " + username + " ("
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
        } catch (Throwable cause) {
            String message = "Failed to establish an inbound transfer connection to "
                    + username + " ("
                    + incomingConnection.getIpEndpoint()
                    + ") for token " + token + ": " + Failures.message(cause);
            diagnostic.debug(message + " (type: " + connection.getType() + ", id: " + connection.getId() + ")");
            connection.close();
            throw new ConnectionException(message, cause);
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
                response.getIpEndpoint(), options.get().transferConnectionOptions());
        connection.setType(ConnectionType.INBOUND_INDIRECT);
        connection.<ConnectionDisconnectedEvent>subscribe(
                Connection.Kind.DISCONNECTED,
                eventData -> diagnostic.debug("Transfer connection to " + response.getUsername() + " ("
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
        } catch (Throwable cause) {
            String message = "Failed to establish an inbound indirect transfer "
                    + "connection to " + response.getUsername() + " ("
                    + response.getIpEndpoint() + "): "
                    + Failures.message(cause);
            diagnostic.debug(message);
            connection.close();
            throw new ConnectionException(message, cause);
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
    public void removeAndCloseAll() {
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
        if (closed.compareAndSet(false, true)) {
            removeAndCloseAll();
            if (ownsScheduler) {
                scheduler.close();
            }
        }
    }

    private MessageConnection establishIncomingMessageConnection(
            String username, Connection incomingConnection, ConnectionCell superseded) throws InterruptedException {
        diagnostic.debug("Inbound message connection to " + username + " ("
                + incomingConnection.getIpEndpoint()
                + ") accepted. (type: " + incomingConnection.getType()
                + ", id: " + incomingConnection.getId() + ")");
        MessageConnection connection = connectionFactory.getMessageConnection(
                username,
                incomingConnection.getIpEndpoint(),
                options.get().peerConnectionOptions(),
                incomingConnection.handoffTcpClient());
        diagnostic.debug("Inbound message connection to " + username + " ("
                + connection.getIpEndpoint() + ") handed off. (old: "
                + incomingConnection.getId() + ", new: "
                + connection.getId() + ")");
        incomingConnection.close();
        connection.setType(ConnectionType.INBOUND_DIRECT);
        attachPeerMessageListeners(connection);
        subscribeToDisconnect(connection);

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
                Subscription oldSubscription = disconnectSubscriptions.remove(old);
                if (oldSubscription != null) {
                    oldSubscription.close();
                }
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

    private MessageConnection establishInboundIndirectMessageConnection(ConnectToPeerResponse response)
            throws InterruptedException, TimeoutException {
        diagnostic.debug("Attempting inbound indirect message connection to "
                + response.getUsername() + " (" + response.getIpEndpoint()
                + ") for token " + response.getToken());
        MessageConnection connection = connectionFactory.getMessageConnection(
                response.getUsername(), response.getIpEndpoint(), options.get().peerConnectionOptions());
        connection.setType(ConnectionType.INBOUND_INDIRECT);
        attachPeerMessageListeners(connection);
        CancellationController cancellation = new CancellationController();
        pendingInboundIndirectConnections.put(response.getUsername(), cancellation);

        try {
            connection.connect(cancellation.getSignal());
            connection.write(new PierceFirewall(response.getToken()).toByteArray(), cancellation.getSignal());
        } catch (Throwable failure) {
            connection.close();
            throw Failures.rethrow(failure);
        } finally {
            pendingInboundIndirectConnections.remove(response.getUsername(), cancellation);
            cancellation.close();
        }
        subscribeToDisconnect(connection);
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
                    scheduler.executor(),
                    () -> establishOutboundDirectMessageConnection(username, ipEndpoint, directCancellation.token()),
                    () -> establishOutboundIndirectMessageConnection(
                            username, solicitationToken, indirectCancellation.token()));
        } catch (Throwable failure) {
            directCancellation.close();
            indirectCancellation.close();
            if (failure instanceof ConnectionException connectionFailure) {
                throw connectionFailure;
            }
            String message = "Failed to establish a direct or indirect message "
                    + "connection to " + username + " (" + ipEndpoint
                    + ")";
            diagnostic.debug(message);
            throw new ConnectionException(message);
        }

        MessageConnection connection = winner.value();
        subscribeToDisconnect(connection);
        Subscription provisionalSubscription = provisionalDisconnectSubscriptions.remove(connection);
        if (provisionalSubscription != null) {
            provisionalSubscription.close();
        }
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
        } catch (Throwable cause) {
            String message = "Failed to negotiate message connection to "
                    + username + " (" + ipEndpoint + "): "
                    + Failures.message(cause);
            diagnostic.debug(message + " (type: " + connection.getType() + ", id: " + connection.getId() + ")");
            connection.close();
            throw new ConnectionException(message, cause);
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
            String username, InetSocketAddress ipEndpoint, CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
        diagnostic.debug("Attempting direct message connection to " + username + " (" + ipEndpoint + ")");
        MessageConnection connection = connectionFactory.getMessageConnection(
                username, ipEndpoint, options.get().peerConnectionOptions());
        connection.setType(ConnectionType.OUTBOUND_DIRECT);
        attachPeerMessageListeners(connection);
        subscribeToProvisionalDisconnect(connection);
        try {
            connection.connect(cancellationSignal);
        } catch (Throwable failure) {
            diagnostic.debug("Failed to establish a direct message connection to "
                    + username + " (" + ipEndpoint + "): "
                    + Failures.message(failure));
            connection.close();
            throw Failures.rethrow(failure);
        }
        diagnostic.debug("Direct message connection to " + username + " ("
                + ipEndpoint + ") established. (type: "
                + connection.getType() + ", id: "
                + connection.getId() + ")");
        return connection;
    }

    private MessageConnection establishOutboundIndirectMessageConnection(
            String username, int solicitationToken, CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
        diagnostic.debug("Soliciting indirect message connection to " + username + " with token " + solicitationToken);
        pendingSolicitations.putIfAbsent(solicitationToken, username);
        boolean answered = false;
        try {
            // Registered before the request that provokes it: the peer can be
            // knocking on the listener before this write returns.
            Wait<Connection> wait = waiter.register(
                    new WaitKey(Constants.WaitKey.SOLICITED_PEER_CONNECTION, username, solicitationToken),
                    Connection.class,
                    options.get().peerConnectionOptions().indirectSolicitationTimeout(),
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
                        options.get().peerConnectionOptions(),
                        accepted.handoffTcpClient());
                diagnostic.debug("Indirect message connection to " + username + " ("
                        + accepted.getIpEndpoint()
                        + ") handed off. (old: " + accepted.getId()
                        + ", new: " + connection.getId() + ")");
                connection.setType(ConnectionType.OUTBOUND_INDIRECT);
                attachPeerMessageListeners(connection);
                subscribeToProvisionalDisconnect(connection);
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
                    + ": " + Failures.message(failure));
            throw Failures.rethrow(failure);
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
            scheduler.schedule(
                    () -> pendingSolicitations.remove(solicitationToken, username),
                    LATE_SOLICITATION_GRACE_MILLIS,
                    java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (Throwable dispatchFailed) {
            // This is called from a finally. Throwing here would replace the
            // failure that finally is unwinding, and a grace window is not
            // worth that: drop it and expire the token now.
            pendingSolicitations.remove(solicitationToken, username);
        }
    }

    private Connection establishOutboundDirectTransferConnection(
            InetSocketAddress ipEndpoint, int token, CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
        diagnostic.debug("Attempting direct transfer connection for token " + token + " to " + ipEndpoint);
        Connection connection = connectionFactory.getTransferConnection(
                ipEndpoint, options.get().transferConnectionOptions());
        connection.setType(ConnectionType.OUTBOUND_DIRECT);
        connection.<ConnectionDisconnectedEvent>subscribe(
                Connection.Kind.DISCONNECTED,
                eventData -> diagnostic.debug("Transfer connection for token " + token + " to "
                        + ipEndpoint + " disconnected: "
                        + disconnectMessage(eventData) + ". (type: "
                        + connection.getType() + ", id: "
                        + connection.getId() + ")"));
        try {
            connection.connect(cancellationSignal);
        } catch (Throwable failure) {
            diagnostic.debug("Failed to establish a direct transfer connection "
                    + "for token " + token + " to (" + ipEndpoint
                    + "): " + Failures.message(failure));
            connection.close();
            throw Failures.rethrow(failure);
        }
        diagnostic.debug("Direct transfer connection for " + token + " to "
                + connection.getIpEndpoint()
                + " established. (type: " + connection.getType()
                + ", id: " + connection.getId() + ")");
        return connection;
    }

    private Connection establishOutboundIndirectTransferConnection(
            String username, int token, CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
        diagnostic.debug("Soliciting indirect transfer connection to " + username + " with token " + token);
        int solicitationToken = tokens.nextToken();
        pendingSolicitations.putIfAbsent(solicitationToken, username);
        try {
            Wait<Connection> wait = waiter.register(
                    new WaitKey(Constants.WaitKey.SOLICITED_PEER_CONNECTION, username, solicitationToken),
                    Connection.class,
                    options.get().transferConnectionOptions().indirectSolicitationTimeout(),
                    cancellationSignal);
            server.write(
                    new ConnectToPeerRequest(solicitationToken, username, Constants.ConnectionType.TRANSFER),
                    cancellationSignal);
            Connection accepted = wait.await();
            try {
                Connection connection = connectionFactory.getTransferConnection(
                        accepted.getIpEndpoint(),
                        options.get().transferConnectionOptions(),
                        accepted.handoffTcpClient());
                diagnostic.debug("Indirect transfer connection to " + username + " ("
                        + accepted.getIpEndpoint()
                        + ") handed off. (old: " + accepted.getId()
                        + ", new: " + connection.getId() + ")");
                connection.setType(ConnectionType.OUTBOUND_INDIRECT);
                connection.<ConnectionDisconnectedEvent>subscribe(
                        Connection.Kind.DISCONNECTED,
                        eventData -> diagnostic.debug("Transfer connection for token " + token + " ("
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
                    + token + ": " + Failures.message(failure));
            throw Failures.rethrow(failure);
        } finally {
            pendingSolicitations.remove(solicitationToken, username);
        }
    }

    private void attachPeerMessageListeners(MessageConnection connection) {
        connection.<MessageEvent>subscribe(MessageConnection.MessageKind.READ, peerMessages::handleMessageRead);
        connection.<MessageReceivedEvent>subscribe(
                MessageConnection.MessageKind.RECEIVED, peerMessages::handleMessageReceived);
        connection.<MessageEvent>subscribe(MessageConnection.MessageKind.WRITTEN, peerMessages::handleMessageWritten);
    }

    /**
     * Refuses a cache insertion once the network is closed, undoing the claim
     * it made.
     *
     * <p>{@code removeAndCloseAll} iterates the map weakly, so a cell put in
     * racing the sweep — or after it — was never closed and never removed: a
     * shutdown-time place-in-queue poll or upload-failure notification could
     * repopulate the cache of a closed network, which is how a live run's last
     * cache census read 1 rather than 0.
     */
    private void guardOpen(String username, ConnectionCell entry) {
        if (closed.get()) {
            messageConnections.remove(username, entry);
            entry.closeWhenSettled();
            throw new ConnectionException("The peer network is closed");
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

    private void messageConnectionDisconnected(ConnectionDisconnectedEvent eventData) {
        MessageConnection connection = (MessageConnection) eventData.connection();
        Subscription subscription = disconnectSubscriptions.remove(connection);
        if (subscription != null) {
            subscription.close();
        }
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

    private void messageConnectionProvisionalDisconnected(ConnectionDisconnectedEvent eventData) {
        MessageConnection connection = (MessageConnection) eventData.connection();
        Subscription subscription = provisionalDisconnectSubscriptions.remove(connection);
        if (subscription != null) {
            subscription.close();
        }
        connection.close();
    }

    private void publishDiagnostic(DiagnosticMessage eventData) {
        diagnosticListeners.forEach(listener -> listener.accept(eventData));
    }

    private void subscribeToDisconnect(MessageConnection connection) {
        Subscription subscription = connection.subscribe(Connection.Kind.DISCONNECTED, disconnectedListener);
        Subscription previous = disconnectSubscriptions.put(connection, subscription);
        if (previous != null) {
            previous.close();
        }
    }

    private void subscribeToProvisionalDisconnect(MessageConnection connection) {
        Subscription subscription = connection.subscribe(Connection.Kind.DISCONNECTED, provisionalDisconnectedListener);
        Subscription previous = provisionalDisconnectSubscriptions.put(connection, subscription);
        if (previous != null) {
            previous.close();
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

    private static String disconnectMessage(ConnectionDisconnectedEvent eventData) {
        if (eventData.exception() != null) {
            return Failures.message(eventData.exception());
        }
        return eventData.message();
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
