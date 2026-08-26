// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.handlers;

import dev.slsk.internal.ServerLink;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.NetworkExecutor;
import dev.slsk.internal.common.TokenFactory;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import dev.slsk.internal.messaging.messages.DistributedBranchLevel;
import dev.slsk.internal.messaging.messages.DistributedBranchRoot;
import dev.slsk.internal.messaging.messages.DistributedChildDepth;
import dev.slsk.internal.messaging.messages.DistributedPingResponse;
import dev.slsk.internal.messaging.messages.DistributedSearchRequest;
import dev.slsk.internal.messaging.messages.EmbeddedMessage;
import dev.slsk.internal.network.DistributedConnectionManager;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.MessageEvent;
import dev.slsk.internal.network.PeerEndpoint;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.internal.search.SearchResponder;
import java.util.Base64;
import java.util.Objects;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles incoming messages from distributed connections.
 *
 * <p>The mesh and the responder are supplied rather than held: this is built
 * before the mesh, because the mesh attaches this to every connection it makes.
 */
public final class DefaultDistributedMessageHandler implements DistributedMessageHandler {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultDistributedMessageHandler.class);
    private static final Logger PROTOCOL = LoggerFactory.getLogger("dev.slsk.protocol");
    private final Supplier<SoulseekClientOptions> options;
    private final ServerLink server;
    private final TokenFactory tokens;
    private final Waiter waiter;
    private final Supplier<DistributedConnectionManager> mesh;
    private final Supplier<SearchResponder> searchResponses;
    private final NetworkExecutor networkExecutor;
    private volatile String deduplicationHash;

    /** Creates a handler. */
    public DefaultDistributedMessageHandler(
            Supplier<SoulseekClientOptions> options,
            ServerLink server,
            TokenFactory tokens,
            Waiter waiter,
            Supplier<DistributedConnectionManager> mesh,
            Supplier<SearchResponder> searchResponses) {
        this(options, server, tokens, waiter, mesh, searchResponses, new NetworkExecutor());
    }

    /** Creates a handler sharing its client's network executor. */
    public DefaultDistributedMessageHandler(
            Supplier<SoulseekClientOptions> options,
            ServerLink server,
            TokenFactory tokens,
            Waiter waiter,
            Supplier<DistributedConnectionManager> mesh,
            Supplier<SearchResponder> searchResponses,
            NetworkExecutor networkExecutor) {
        this.options = Objects.requireNonNull(options, "options");
        this.server = Objects.requireNonNull(server, "server");
        this.tokens = Objects.requireNonNull(tokens, "tokens");
        this.waiter = Objects.requireNonNull(waiter, "waiter");
        this.mesh = Objects.requireNonNull(mesh, "mesh");
        this.searchResponses = Objects.requireNonNull(searchResponses, "searchResponses");
        this.networkExecutor = Objects.requireNonNull(networkExecutor, "networkExecutor");
    }

    @Override
    public void handleChildMessageRead(MessageEvent eventData) {
        handleChildMessageRead(eventData.connection(), eventData.message());
    }

    @Override
    public void handleChildMessageRead(MessageConnection connection, byte[] message) {
        MessageCode.Distributed code;
        try {
            code = new MessageReader<>(message, MessageCode.Distributed.class).readCode();
        } catch (IllegalArgumentException unknown) {
            // A newer client's message, not a broken connection.
            LOG.debug(
                    "Ignored an unknown distributed child message from {}: {}",
                    connection.getUsername(),
                    unknown.getMessage());
            return;
        }
        if (code != MessageCode.Distributed.PING) {
            PROTOCOL.trace(
                    "Distributed child message received: {} from {} ({}) (id: {})",
                    code,
                    connection.getUsername(),
                    connection.getIpEndpoint(),
                    connection.getId());
        }

        try {
            switch (code) {
                case CHILD_DEPTH -> {
                    // Source ignores a child's depth.
                }
                // Off the child's read loop, as the dispatched write it
                // replaces was: a ping answered in front of the next search
                // request delays every child behind this one.
                case PING ->
                    networkExecutor.dispatch(
                            () -> connection.write(new DistributedPingResponse(tokens.nextToken())),
                            failure -> warnChild(code, connection, failure));
                default ->
                    LOG.debug(
                            "Unhandled distributed child message: {} from {} ({}); {} bytes",
                            code,
                            connection.getUsername(),
                            connection.getIpEndpoint(),
                            message.length);
            }
        } catch (Throwable failure) {
            warnChild(code, connection, failure);
        }
    }

    @Override
    public void handleChildMessageWritten(MessageEvent eventData) {
        MessageConnection connection = eventData.connection();
        MessageCode.Distributed code =
                new MessageReader<>(eventData.message(), MessageCode.Distributed.class).readCode();
        if (code != MessageCode.Distributed.PING) {
            PROTOCOL.trace(
                    "Distributed child message sent: {} to {} ({}) (id: {})",
                    code,
                    connection.getUsername(),
                    connection.getIpEndpoint(),
                    connection.getId());
        }
    }

    @Override
    public void handleMessageRead(MessageEvent eventData) {
        handleMessageRead(eventData.connection(), eventData.message());
    }

    @Override
    public void handleMessageRead(MessageConnection connection, byte[] message) {
        MessageCode.Distributed code;
        try {
            code = new MessageReader<>(message, MessageCode.Distributed.class).readCode();
        } catch (IllegalArgumentException unknown) {
            // A newer client's message, not a broken connection. This is the
            // parent's read loop — every inbound search travels on it.
            LOG.debug(
                    "Ignored an unknown distributed message from {}: {}",
                    connection.getUsername(),
                    unknown.getMessage());
            return;
        }
        if (code != MessageCode.Distributed.SEARCH_REQUEST
                && code != MessageCode.Distributed.EMBEDDED_MESSAGE
                && code != MessageCode.Distributed.PING) {
            PROTOCOL.trace(
                    "Distributed message received: {} from {} ({}) (id: {})",
                    code,
                    connection.getUsername(),
                    connection.getIpEndpoint(),
                    connection.getId());
        } else if (options.get().deduplicateSearchRequests()) {
            String current = Base64.getEncoder().encodeToString(message);
            if (Objects.equals(deduplicationHash, current)) {
                return;
            }
            deduplicationHash = current;
        }

        try {
            switch (code) {
                case EMBEDDED_MESSAGE -> handleParentEmbeddedMessage(connection, message);
                case SEARCH_REQUEST -> handleSearchRequest(message);
                case PING -> {
                    DistributedPingResponse ping = DistributedPingResponse.fromByteArray(message);
                    waiter.complete(
                            new WaitKey.DistributedUser(MessageCode.Distributed.PING, connection.getUsername()), ping);
                }
                case BRANCH_LEVEL -> {
                    DistributedBranchLevel branchLevel = DistributedBranchLevel.fromByteArray(message);
                    if (isParent(connection)) {
                        mesh.get().setParentBranchLevel(branchLevel.getLevel());
                    }
                }
                case BRANCH_ROOT -> {
                    DistributedBranchRoot branchRoot = DistributedBranchRoot.fromByteArray(message);
                    if (isParent(connection)) {
                        mesh.get().setParentBranchRoot(branchRoot.getUsername());
                    }
                }
                case CHILD_DEPTH -> {
                    DistributedChildDepth depth = DistributedChildDepth.fromByteArray(message);
                    waiter.complete(new WaitKey.ChildDepth(connection.getKey()), depth.getDepth());
                }
                default ->
                    LOG.debug(
                            "Unhandled distributed message: {} from {} ({}); {} bytes",
                            code,
                            connection.getUsername(),
                            connection.getIpEndpoint(),
                            message.length);
            }
        } catch (Throwable failure) {
            Throwable cause = failure;
            LOG.warn(
                    "Error handling distributed message: {} from {} ({}); {}",
                    code,
                    connection.getUsername(),
                    connection.getIpEndpoint(),
                    Failures.message(cause),
                    cause);
        }
    }

    @Override
    public void handleMessageWritten(MessageEvent eventData) {
        MessageConnection connection = eventData.connection();
        MessageCode.Distributed code =
                new MessageReader<>(eventData.message(), MessageCode.Distributed.class).readCode();
        PROTOCOL.trace("Distributed message sent: {}", code);
    }

    @Override
    public void handleEmbeddedMessage(byte[] message) {
        MessageCode.Distributed code = MessageCode.Distributed.UNKNOWN;
        try {
            EmbeddedMessage embedded = EmbeddedMessage.fromByteArray(message);
            code = embedded.getDistributedCode();
            byte[] distributed = embedded.getDistributedMessage();
            if (code != MessageCode.Distributed.SEARCH_REQUEST) {
                LOG.debug("Unhandled embedded message: {}; {} bytes", code, message.length);
                return;
            }
            mesh.get().promoteToBranchRoot();
            DistributedSearchRequest search = DistributedSearchRequest.fromByteArray(distributed);
            broadcastAndRespond(distributed, search);
        } catch (Throwable failure) {
            Throwable cause = failure;
            LOG.warn("Error handling embedded message: {}; {}", code, Failures.message(cause), cause);
        }
    }

    private void handleParentEmbeddedMessage(MessageConnection connection, byte[] message) {
        EmbeddedMessage embedded = EmbeddedMessage.fromByteArray(message);
        if (embedded.getDistributedCode() != MessageCode.Distributed.SEARCH_REQUEST) {
            LOG.debug(
                    "Unhandled embedded message: {} from {} ({}); {} bytes",
                    MessageCode.Distributed.EMBEDDED_MESSAGE,
                    connection.getUsername(),
                    connection.getIpEndpoint(),
                    message.length);
            return;
        }
        byte[] distributed = embedded.getDistributedMessage();
        broadcastAndRespond(distributed, DistributedSearchRequest.fromByteArray(distributed));
    }

    private void handleSearchRequest(byte[] message) {
        broadcastAndRespond(message, DistributedSearchRequest.fromByteArray(message));
    }

    /**
     * Forwards a search down the branch and answers it.
     *
     * <p>Both go to threads of their own. This runs on the parent's read loop,
     * and neither fanning a search out to every child nor asking the share
     * catalog and connecting to the searcher is something the next search
     * request should wait behind. Each dispatch reports its own failure — the
     * discarded future that used to carry it back is gone.
     */
    private void broadcastAndRespond(byte[] distributed, DistributedSearchRequest search) {
        networkExecutor.dispatch(
                () -> mesh.get().broadcastMessage(distributed),
                failure -> LOG.warn(
                        "Error broadcasting search request from {} with token {}: {}",
                        search.getUsername(),
                        search.getToken(),
                        Failures.message(failure),
                        failure));
        if (Objects.equals(search.getUsername(), server.username())) {
            return;
        }
        networkExecutor.dispatch(
                () -> searchResponses.get().tryRespond(search.getUsername(), search.getToken(), search.getQuery()),
                failure -> LOG.warn(
                        "Error responding to search request from {} with token {}: {}",
                        search.getUsername(),
                        search.getToken(),
                        Failures.message(failure),
                        failure));
    }

    private void warnChild(MessageCode.Distributed code, MessageConnection connection, Throwable failure) {
        Throwable cause = failure;
        LOG.warn(
                "Error handling distributed child message: {} from {} ({}); {}",
                code,
                connection.getUsername(),
                connection.getIpEndpoint(),
                Failures.message(cause),
                cause);
    }

    private boolean isParent(MessageConnection connection) {
        return Objects.equals(
                new PeerEndpoint(connection.getUsername(), connection.getIpEndpoint()),
                mesh.get().getParent());
    }

}
