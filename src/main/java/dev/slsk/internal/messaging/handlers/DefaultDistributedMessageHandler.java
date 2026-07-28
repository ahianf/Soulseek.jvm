// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.handlers;

import dev.slsk.internal.common.Constants;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.diagnostics.DiagnosticEvent;
import dev.slsk.internal.diagnostics.DiagnosticEventListener;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.diagnostics.FilteringDiagnosticSink;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import dev.slsk.internal.messaging.messages.DistributedBranchLevel;
import dev.slsk.internal.messaging.messages.DistributedBranchRoot;
import dev.slsk.internal.messaging.messages.DistributedChildDepth;
import dev.slsk.internal.messaging.messages.DistributedPingResponse;
import dev.slsk.internal.messaging.messages.DistributedSearchRequest;
import dev.slsk.internal.messaging.messages.EmbeddedMessage;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.MessageEvent;
import dev.slsk.internal.network.PeerEndpoint;
import java.util.Base64;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;

/** Handles incoming messages from distributed connections. */
public final class DefaultDistributedMessageHandler implements DistributedMessageHandler {
    private final DistributedMessageHandlerClient client;
    private final DiagnosticSink diagnostic;
    private final CopyOnWriteArrayList<DiagnosticEventListener> diagnosticListeners = new CopyOnWriteArrayList<>();
    private volatile String deduplicationHash;

    /** Creates a handler with its default diagnostic factory. */
    public DefaultDistributedMessageHandler(DistributedMessageHandlerClient client) {
        this(client, null);
    }

    /** Creates a handler. */
    public DefaultDistributedMessageHandler(DistributedMessageHandlerClient client, DiagnosticSink diagnosticFactory) {
        this.client = Objects.requireNonNull(client, "client");
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
    public void handleChildMessageRead(MessageConnection sender, MessageEvent eventData) {
        handleChildMessageRead(sender, eventData.getMessage());
    }

    @Override
    public void handleChildMessageRead(MessageConnection sender, byte[] message) {
        handleChildMessageReadAsync(sender, message);
    }

    CompletableFuture<Void> handleChildMessageReadAsync(MessageConnection connection, byte[] message) {
        MessageCode.Distributed code = new MessageReader<>(message, MessageCode.Distributed.class).readCode();
        if (code != MessageCode.Distributed.PING) {
            diagnostic.debug("Distributed child message received: " + code + " from "
                    + connection.getUsername() + " ("
                    + connection.getIpEndpoint() + ") (id: "
                    + connection.getId() + ")");
        }

        CompletableFuture<Void> operation;
        try {
            operation = switch (code) {
                case CHILD_DEPTH -> CompletableFuture.completedFuture(null);
                case PING -> connection.writeAsync(new DistributedPingResponse(client.getNextToken()));
                default -> {
                    diagnostic.debug("Unhandled distributed child message: " + code
                            + " from " + connection.getUsername() + " ("
                            + connection.getIpEndpoint() + "); "
                            + message.length + " bytes");
                    yield CompletableFuture.completedFuture(null);
                }
            };
        } catch (Throwable failure) {
            operation = CompletableFuture.failedFuture(failure);
        }
        return operation.handle((ignored, failure) -> {
            if (failure != null) {
                Throwable cause = unwrap(failure);
                diagnostic.warning(
                        "Error handling distributed child message: " + code
                                + " from " + connection.getUsername() + " ("
                                + connection.getIpEndpoint() + "); "
                                + message(cause),
                        cause);
            }
            return null;
        });
    }

    @Override
    public void handleChildMessageWritten(MessageConnection connection, MessageEvent eventData) {
        MessageCode.Distributed code =
                new MessageReader<>(eventData.getMessage(), MessageCode.Distributed.class).readCode();
        if (code != MessageCode.Distributed.PING) {
            diagnostic.debug("Distributed child message sent: " + code + " to "
                    + connection.getUsername() + " ("
                    + connection.getIpEndpoint() + ") (id: "
                    + connection.getId() + ")");
        }
    }

    @Override
    public void handleMessageRead(MessageConnection sender, MessageEvent eventData) {
        handleMessageRead(sender, eventData.getMessage());
    }

    @Override
    public void handleMessageRead(MessageConnection sender, byte[] message) {
        handleMessageReadAsync(sender, message);
    }

    CompletableFuture<Void> handleMessageReadAsync(MessageConnection connection, byte[] message) {
        MessageCode.Distributed code = new MessageReader<>(message, MessageCode.Distributed.class).readCode();
        if (code != MessageCode.Distributed.SEARCH_REQUEST
                && code != MessageCode.Distributed.EMBEDDED_MESSAGE
                && code != MessageCode.Distributed.PING) {
            diagnostic.debug("Distributed message received: " + code + " from "
                    + connection.getUsername() + " ("
                    + connection.getIpEndpoint() + ") (id: "
                    + connection.getId() + ")");
        } else if (client.getOptions().isDeduplicateSearchRequests()) {
            String current = Base64.getEncoder().encodeToString(message);
            if (Objects.equals(deduplicationHash, current)) {
                return CompletableFuture.completedFuture(null);
            }
            deduplicationHash = current;
        }

        CompletableFuture<Void> operation;
        try {
            operation = switch (code) {
                case EMBEDDED_MESSAGE -> handleParentEmbeddedMessage(connection, message);
                case SEARCH_REQUEST -> handleSearchRequest(message);
                case PING -> {
                    DistributedPingResponse ping = DistributedPingResponse.fromByteArray(message);
                    client.getWaiter()
                            .complete(new WaitKey(MessageCode.Distributed.PING, connection.getUsername()), ping);
                    yield CompletableFuture.completedFuture(null);
                }
                case BRANCH_LEVEL -> {
                    DistributedBranchLevel branchLevel = DistributedBranchLevel.fromByteArray(message);
                    if (isParent(connection)) {
                        client.getDistributedConnectionManager().setParentBranchLevel(branchLevel.getLevel());
                    }
                    yield CompletableFuture.completedFuture(null);
                }
                case BRANCH_ROOT -> {
                    DistributedBranchRoot branchRoot = DistributedBranchRoot.fromByteArray(message);
                    if (isParent(connection)) {
                        client.getDistributedConnectionManager().setParentBranchRoot(branchRoot.getUsername());
                    }
                    yield CompletableFuture.completedFuture(null);
                }
                case CHILD_DEPTH -> {
                    DistributedChildDepth depth = DistributedChildDepth.fromByteArray(message);
                    client.getWaiter()
                            .complete(
                                    new WaitKey(Constants.WaitKey.CHILD_DEPTH_MESSAGE, connection.getKey()),
                                    depth.getDepth());
                    yield CompletableFuture.completedFuture(null);
                }
                default -> {
                    diagnostic.debug("Unhandled distributed message: " + code + " from "
                            + connection.getUsername() + " ("
                            + connection.getIpEndpoint() + "); "
                            + message.length + " bytes");
                    yield CompletableFuture.completedFuture(null);
                }
            };
        } catch (Throwable failure) {
            operation = CompletableFuture.failedFuture(failure);
        }
        return operation.handle((ignored, failure) -> {
            if (failure != null) {
                Throwable cause = unwrap(failure);
                diagnostic.warning(
                        "Error handling distributed message: " + code + " from "
                                + connection.getUsername() + " ("
                                + connection.getIpEndpoint() + "); "
                                + message(cause),
                        cause);
            }
            return null;
        });
    }

    @Override
    public void handleMessageWritten(MessageConnection sender, MessageEvent eventData) {
        MessageCode.Distributed code =
                new MessageReader<>(eventData.getMessage(), MessageCode.Distributed.class).readCode();
        diagnostic.debug("Distributed message sent: " + code);
    }

    @Override
    public void handleEmbeddedMessage(byte[] message) {
        handleEmbeddedMessageAsync(message);
    }

    CompletableFuture<Void> handleEmbeddedMessageAsync(byte[] message) {
        AtomicCode code = new AtomicCode();
        CompletableFuture<Void> operation;
        try {
            EmbeddedMessage embedded = EmbeddedMessage.fromByteArray(message);
            code.value = embedded.getDistributedCode();
            byte[] distributed = embedded.getDistributedMessage();
            if (code.value == MessageCode.Distributed.SEARCH_REQUEST) {
                client.getDistributedConnectionManager().promoteToBranchRoot();
                DistributedSearchRequest search = DistributedSearchRequest.fromByteArray(distributed);
                client.getDistributedConnectionManager().broadcastMessageAsync(distributed);
                operation = client.getSearchResponder()
                        .tryRespondAsync(search.getUsername(), search.getToken(), search.getQuery())
                        .thenApply(ignored -> null);
            } else {
                diagnostic.debug("Unhandled embedded message: " + code.value + "; " + message.length + " bytes");
                operation = CompletableFuture.completedFuture(null);
            }
        } catch (Throwable failure) {
            operation = CompletableFuture.failedFuture(failure);
        }
        return operation.handle((ignored, failure) -> {
            if (failure != null) {
                Throwable cause = unwrap(failure);
                diagnostic.warning("Error handling embedded message: " + code.value + "; " + message(cause), cause);
            }
            return null;
        });
    }

    private CompletableFuture<Void> handleParentEmbeddedMessage(MessageConnection connection, byte[] message) {
        EmbeddedMessage embedded = EmbeddedMessage.fromByteArray(message);
        if (embedded.getDistributedCode() != MessageCode.Distributed.SEARCH_REQUEST) {
            diagnostic.debug("Unhandled embedded message: "
                    + MessageCode.Distributed.EMBEDDED_MESSAGE + " from "
                    + connection.getUsername() + " ("
                    + connection.getIpEndpoint() + "); "
                    + message.length + " bytes");
            return CompletableFuture.completedFuture(null);
        }
        DistributedSearchRequest search = DistributedSearchRequest.fromByteArray(embedded.getDistributedMessage());
        client.getDistributedConnectionManager().broadcastMessageAsync(embedded.getDistributedMessage());
        if (Objects.equals(search.getUsername(), client.getUsername())) {
            return CompletableFuture.completedFuture(null);
        }
        return client.getSearchResponder()
                .tryRespondAsync(search.getUsername(), search.getToken(), search.getQuery())
                .thenApply(ignored -> null);
    }

    private CompletableFuture<Void> handleSearchRequest(byte[] message) {
        DistributedSearchRequest search = DistributedSearchRequest.fromByteArray(message);
        client.getDistributedConnectionManager().broadcastMessageAsync(message);
        if (Objects.equals(search.getUsername(), client.getUsername())) {
            return CompletableFuture.completedFuture(null);
        }
        return client.getSearchResponder()
                .tryRespondAsync(search.getUsername(), search.getToken(), search.getQuery())
                .thenApply(ignored -> null);
    }

    private boolean isParent(MessageConnection connection) {
        return Objects.equals(
                new PeerEndpoint(connection.getUsername(), connection.getIpEndpoint()),
                client.getDistributedConnectionManager().getParent());
    }

    private void raiseDiagnostic(DiagnosticEvent eventData) {
        diagnosticListeners.forEach(listener -> listener.handle(this, eventData));
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

    private static final class AtomicCode {
        private MessageCode.Distributed value = MessageCode.Distributed.UNKNOWN;
    }
}
