// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import dev.slsk.exceptions.ConnectionException;
import dev.slsk.internal.common.Constants;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.messaging.messages.PeerInit;
import dev.slsk.internal.messaging.messages.PierceFirewall;
import dev.slsk.internal.network.tcp.Listener;
import dev.slsk.internal.network.tcp.TransportConnection;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.internal.search.SearchResponder;
import dev.slsk.internal.search.SearchResponseCacheRecord;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles incoming connections established by the TCP listener.
 *
 * <p>Everything here is supplied rather than fetched because the listener, the
 * two connection managers, and the responder are wired around one another and
 * are not all available when this handler is constructed.
 */
public final class ListenerHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ListenerHandler.class);
    // Nicotine+ limits direct initialization frames to 16 KiB.  A direct
    // connection starts with either an 8-byte PierceFirewall message or a
    // small PeerInit message, so this is still deliberately generous.
    private static final int MAX_INITIALIZATION_MESSAGE_BYTES = 16 * 1024;
    private final Supplier<SoulseekClientOptions> options;
    private final Supplier<Listener> listener;
    private final Supplier<PeerConnectionManager> peers;
    private final Supplier<DistributedConnectionManager> distributed;
    private final Waiter waiter;
    private final Supplier<SearchResponder> searchResponses;

    /** Creates a handler. */
    public ListenerHandler(
            Supplier<SoulseekClientOptions> options,
            Supplier<Listener> listener,
            Supplier<PeerConnectionManager> peers,
            Supplier<DistributedConnectionManager> distributed,
            Waiter waiter,
            Supplier<SearchResponder> searchResponses) {
        this.options = Objects.requireNonNull(options, "options");
        this.listener = Objects.requireNonNull(listener, "listener");
        this.peers = Objects.requireNonNull(peers, "peers");
        this.distributed = Objects.requireNonNull(distributed, "distributed");
        this.waiter = Objects.requireNonNull(waiter, "waiter");
        this.searchResponses = Objects.requireNonNull(searchResponses, "searchResponses");
    }

    public void handleConnection(TransportConnection connection) {
        LOG.debug(
                "Accepted incoming connection from {} on {}:{} (id: {})",
                connection.getIpEndpoint().getAddress().getHostAddress(),
                listener.get().getIpAddress(),
                listener.get().getPort(),
                connection.getId());

        try {
            // Everything here runs on this thread: the listener hands each
            // accepted connection one of its own, and its whole job is this
            // handshake and whatever the handshake turns out to be for.
            byte[] lengthBytes = connection.read(4);
            int length =
                    ByteBuffer.wrap(lengthBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
            if (length < 0 || length > MAX_INITIALIZATION_MESSAGE_BYTES) {
                throw new ConnectionException("Invalid initialization message length: " + length);
            }
            byte[] body = connection.read(length);
            byte[] message = Arrays.copyOf(lengthBytes, lengthBytes.length + body.length);
            System.arraycopy(body, 0, message, lengthBytes.length, body.length);
            routeInitialization(connection, message);
        } catch (Throwable failure) {
            Throwable cause = failure;
            LOG.debug(
                    "Failed to initialize direct connection from {}:{}: {}",
                    connection.getIpEndpoint().getAddress().getHostAddress(),
                    connection.getIpEndpoint().getPort(),
                    Failures.message(cause));
            connection.disconnect(null, asException(cause));
            connection.close();
        }
    }

    private void routeInitialization(TransportConnection connection, byte[] message) {
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

    private void handlePeerInit(TransportConnection connection, PeerInit peerInit) {
        LOG.debug(
                "PeerInit for connection type {} received from {} ({}:{}) (id: {})",
                peerInit.getConnectionType(),
                peerInit.getUsername(),
                connection.getIpEndpoint().getAddress().getHostAddress(),
                listener.get().getPort(),
                connection.getId());

        if (Constants.ConnectionType.PEER.equals(peerInit.getConnectionType())) {
            peers.get().addOrUpdateMessageConnection(peerInit.getUsername(), connection);
            return;
        }
        if (Constants.ConnectionType.TRANSFER.equals(peerInit.getConnectionType())) {
            TransferConnectionResult result =
                    peers.get().getTransferConnection(peerInit.getUsername(), peerInit.getToken(), connection);
            WaitKey waitKey = new WaitKey.DirectTransfer(peerInit.getUsername(), result.remoteToken());
            if (waiter.hasWait(waitKey)) {
                waiter.complete(waitKey, result.connection());
            } else {
                LOG.debug(
                        "Unexpected transfer connection for token {} from {} ({}:{}) (id: {})",
                        peerInit.getToken(),
                        peerInit.getUsername(),
                        connection.getIpEndpoint().getAddress().getHostAddress(),
                        listener.get().getPort(),
                        connection.getId());
                result.connection().disconnect("Transfer connection rejected: unknown token");
            }
            return;
        }
        if (Constants.ConnectionType.DISTRIBUTED.equals(peerInit.getConnectionType())) {
            distributed.get().addOrUpdateChildConnection(peerInit.getUsername(), connection);
        }
    }

    private void handlePierceFirewall(TransportConnection connection, PierceFirewall pierce) {
        int token = pierce.getToken();
        String username = peers.get().getPendingSolicitations().get(token);
        if (username != null) {
            LOG.debug(
                    "Peer PierceFirewall with token {} received from {} ({}:{}) (id: {})",
                    token,
                    username,
                    connection.getIpEndpoint().getAddress().getHostAddress(),
                    listener.get().getPort(),
                    connection.getId());
            WaitKey waitKey = new WaitKey.SolicitedPeer(username, token);
            if (waiter.hasWait(waitKey)) {
                waiter.complete(waitKey, connection);
            } else {
                // Answered, just not in time. Whatever solicited this has
                // already given up, but the peer did the work and the socket is
                // open — and for a peer we cannot dial, one they opened is the
                // only kind there is. Caching it is what makes the next attempt
                // cost nothing; closing it, as this used to, made the next
                // attempt solicit all over again.
                LOG.debug(
                        "Peer PierceFirewall with token {} from {} arrived after its solicitation lapsed; caching "
                                + "the connection (id: {})",
                        token,
                        username,
                        connection.getId());
                peers.get().addOrUpdateMessageConnection(username, connection);
            }
            return;
        }

        username = distributed.get().getPendingSolicitations().get(token);
        if (username != null) {
            LOG.debug(
                    "Distributed PierceFirewall with token {} received from {} ({}:{}) (id: {})",
                    token,
                    username,
                    connection.getIpEndpoint().getAddress().getHostAddress(),
                    listener.get().getPort(),
                    connection.getId());
            waiter.complete(new WaitKey.SolicitedDistributed(username, token), connection);
            return;
        }

        if (options.get().searchResponseCache() != null) {
            Optional<SearchResponseCacheRecord> lookup =
                    options.get().searchResponseCache().lookup(token);
            if (lookup.isPresent()) {
                SearchResponseCacheRecord record = lookup.get();
                LOG.debug(
                        "PierceFirewall matching pending search response received from {} ({}:{}) (id: {})",
                        record.username(),
                        connection.getIpEndpoint().getAddress().getHostAddress(),
                        listener.get().getPort(),
                        connection.getId());
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

    private static Exception asException(Throwable failure) {
        return failure instanceof Exception exception ? exception : new RuntimeException(failure);
    }
}
