// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import dev.slsk.exceptions.ConnectionException;
import dev.slsk.internal.common.CacheLookupResult;
import dev.slsk.internal.common.Constants;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.diagnostics.DiagnosticEvent;
import dev.slsk.internal.diagnostics.DiagnosticEventListener;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.diagnostics.FilteringDiagnosticSink;
import dev.slsk.internal.messaging.messages.PeerInit;
import dev.slsk.internal.messaging.messages.PierceFirewall;
import dev.slsk.internal.network.tcp.Connection;
import dev.slsk.internal.network.tcp.Listener;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.internal.search.SearchResponder;
import dev.slsk.internal.search.SearchResponseCacheRecord;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

/**
 * Handles incoming connections established by the TCP listener.
 *
 * <p>Everything here is supplied rather than fetched: the listener is replaced
 * when the client is reconfigured, and the two connection managers and the
 * responder are built after this is, because they are what an accepted
 * connection is handed to.
 */
public final class DefaultListenerHandler implements ListenerHandler {
    private final Supplier<SoulseekClientOptions> options;
    private final Supplier<Listener> listener;
    private final Supplier<PeerConnectionManager> peers;
    private final Supplier<DistributedConnectionManager> distributed;
    private final Waiter waiter;
    private final Supplier<SearchResponder> searchResponses;
    private final DiagnosticSink diagnostic;
    private final CopyOnWriteArrayList<DiagnosticEventListener> diagnosticListeners = new CopyOnWriteArrayList<>();

    /** Creates a handler with its default diagnostic factory. */
    public DefaultListenerHandler(
            Supplier<SoulseekClientOptions> options,
            Supplier<Listener> listener,
            Supplier<PeerConnectionManager> peers,
            Supplier<DistributedConnectionManager> distributed,
            Waiter waiter,
            Supplier<SearchResponder> searchResponses) {
        this(options, listener, peers, distributed, waiter, searchResponses, null);
    }

    /** Creates a handler. */
    public DefaultListenerHandler(
            Supplier<SoulseekClientOptions> options,
            Supplier<Listener> listener,
            Supplier<PeerConnectionManager> peers,
            Supplier<DistributedConnectionManager> distributed,
            Waiter waiter,
            Supplier<SearchResponder> searchResponses,
            DiagnosticSink diagnosticFactory) {
        this.options = Objects.requireNonNull(options, "options");
        this.listener = Objects.requireNonNull(listener, "listener");
        this.peers = Objects.requireNonNull(peers, "peers");
        this.distributed = Objects.requireNonNull(distributed, "distributed");
        this.waiter = Objects.requireNonNull(waiter, "waiter");
        this.searchResponses = Objects.requireNonNull(searchResponses, "searchResponses");
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

    @Override
    public void handleConnection(Listener sender, Connection connection) {
        diagnostic.debug("Accepted incoming connection from "
                + connection.getIpEndpoint().getAddress().getHostAddress()
                + " on " + listener.get().getIpAddress()
                + ":" + listener.get().getPort()
                + " (id: " + connection.getId() + ")");

        try {
            // Everything here runs on this thread: the listener hands each
            // accepted connection one of its own, and its whole job is this
            // handshake and whatever the handshake turns out to be for.
            byte[] lengthBytes = connection.read(4);
            int length =
                    ByteBuffer.wrap(lengthBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
            byte[] body = connection.read(length);
            byte[] message = Arrays.copyOf(lengthBytes, lengthBytes.length + body.length);
            System.arraycopy(body, 0, message, lengthBytes.length, body.length);
            routeInitialization(connection, message);
        } catch (Throwable failure) {
            Throwable cause = unwrap(failure);
            diagnostic.debug("Failed to initialize direct connection from "
                    + connection.getIpEndpoint().getAddress().getHostAddress()
                    + ":" + connection.getIpEndpoint().getPort()
                    + ": " + message(cause));
            connection.disconnect(null, asException(cause));
            connection.close();
        }
    }

    DiagnosticSink getDiagnostic() {
        return diagnostic;
    }

    private void routeInitialization(Connection connection, byte[] message) {
        Optional<PeerInit> peerInit = PeerInit.tryFromByteArray(message);
        if (peerInit.isPresent()) {
            handlePeerInit(connection, peerInit.get());
            return;
        }

        Optional<PierceFirewall> pierce = PierceFirewall.tryFromByteArray(message);
        if (pierce.isPresent()) {
            handlePierceFirewall(connection, pierce.get());
            return;
        }

        throw new ConnectionException("Unrecognized initialization message: "
                + toHex(message) + " (" + message.length
                + " bytes, id: " + connection.getId() + ")");
    }

    private void handlePeerInit(Connection connection, PeerInit peerInit) {
        diagnostic.debug("PeerInit for connection type " + peerInit.getConnectionType()
                + " received from " + peerInit.getUsername() + " ("
                + connection.getIpEndpoint().getAddress().getHostAddress()
                + ":" + listener.get().getPort()
                + ") (id: " + connection.getId() + ")");

        if (Constants.ConnectionType.PEER.equals(peerInit.getConnectionType())) {
            peers.get().addOrUpdateMessageConnection(peerInit.getUsername(), connection);
            return;
        }
        if (Constants.ConnectionType.TRANSFER.equals(peerInit.getConnectionType())) {
            TransferConnectionResult result =
                    peers.get().getTransferConnection(peerInit.getUsername(), peerInit.getToken(), connection);
            WaitKey waitKey =
                    new WaitKey(Constants.WaitKey.DIRECT_TRANSFER, peerInit.getUsername(), result.remoteToken());
            if (waiter.hasWait(waitKey)) {
                waiter.complete(waitKey, result.connection());
            } else {
                diagnostic.debug("Unexpected transfer connection for token "
                        + peerInit.getToken() + " from "
                        + peerInit.getUsername() + " ("
                        + connection.getIpEndpoint().getAddress().getHostAddress()
                        + ":" + listener.get().getPort()
                        + ") (id: " + connection.getId() + ")");
                result.connection().disconnect("Transfer connection rejected: unknown token");
            }
            return;
        }
        if (Constants.ConnectionType.DISTRIBUTED.equals(peerInit.getConnectionType())) {
            distributed.get().addOrUpdateChildConnection(peerInit.getUsername(), connection);
        }
    }

    private void handlePierceFirewall(Connection connection, PierceFirewall pierce) {
        int token = pierce.getToken();
        String username = peers.get().getPendingSolicitations().get(token);
        if (username != null) {
            diagnostic.debug("Peer PierceFirewall with token " + token
                    + " received from " + username + " ("
                    + connection.getIpEndpoint().getAddress().getHostAddress()
                    + ":" + listener.get().getPort()
                    + ") (id: " + connection.getId() + ")");
            WaitKey waitKey = new WaitKey(Constants.WaitKey.SOLICITED_PEER_CONNECTION, username, token);
            if (waiter.hasWait(waitKey)) {
                waiter.complete(waitKey, connection);
            } else {
                // Answered, just not in time. Whatever solicited this has
                // already given up, but the peer did the work and the socket is
                // open — and for a peer we cannot dial, one they opened is the
                // only kind there is. Caching it is what makes the next attempt
                // cost nothing; closing it, as this used to, made the next
                // attempt solicit all over again.
                diagnostic.debug("Peer PierceFirewall with token " + token
                        + " from " + username + " arrived after its solicitation "
                        + "lapsed; caching the connection (id: "
                        + connection.getId() + ")");
                peers.get().addOrUpdateMessageConnection(username, connection);
            }
            return;
        }

        username = distributed.get().getPendingSolicitations().get(token);
        if (username != null) {
            diagnostic.debug("Distributed PierceFirewall with token " + token
                    + " received from " + username + " ("
                    + connection.getIpEndpoint().getAddress().getHostAddress()
                    + ":" + listener.get().getPort()
                    + ") (id: " + connection.getId() + ")");
            waiter.complete(
                    new WaitKey(Constants.WaitKey.SOLICITED_DISTRIBUTED_CONNECTION, username, token), connection);
            return;
        }

        if (options.get().getSearchResponseCache() != null) {
            CacheLookupResult<SearchResponseCacheRecord> lookup =
                    options.get().getSearchResponseCache().lookup(token);
            if (lookup.found()) {
                SearchResponseCacheRecord record = lookup.value();
                diagnostic.debug("PierceFirewall matching pending search response "
                        + "received from " + record.username() + " ("
                        + connection.getIpEndpoint().getAddress().getHostAddress()
                        + ":" + listener.get().getPort()
                        + ") (id: " + connection.getId() + ")");
                peers.get().addOrUpdateMessageConnection(record.username(), connection);
                searchResponses.get().tryRespond(token);
                return;
            }
        }

        throw new ConnectionException("Unknown PierceFirewall attempt with token " + token
                + " from "
                + connection.getIpEndpoint().getAddress().getHostAddress()
                + ":" + connection.getIpEndpoint().getPort()
                + " (id: " + connection.getId() + ")");
    }

    private void raiseDiagnostic(DiagnosticEvent args) {
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
