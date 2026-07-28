// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.handlers;

import dev.slsk.exceptions.MessageReadException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.exceptions.TransferReportedFailedException;
import dev.slsk.internal.BrowseResponse;
import dev.slsk.internal.Catalogs;
import dev.slsk.internal.Directory;
import dev.slsk.internal.RawBrowseResponse;
import dev.slsk.internal.RawSearchResponse;
import dev.slsk.internal.SearchResponse;
import dev.slsk.internal.TransferDirection;
import dev.slsk.internal.UserInfo;
import dev.slsk.internal.common.Constants;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.diagnostics.DiagnosticEvent;
import dev.slsk.internal.diagnostics.DiagnosticEventListener;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.diagnostics.FilteringDiagnosticSink;
import dev.slsk.internal.events.DownloadDeniedEvent;
import dev.slsk.internal.events.DownloadFailedEvent;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import dev.slsk.internal.messaging.messages.BrowseResponseFactory;
import dev.slsk.internal.messaging.messages.FolderContentsRequest;
import dev.slsk.internal.messaging.messages.FolderContentsResponse;
import dev.slsk.internal.messaging.messages.PeerSearchRequest;
import dev.slsk.internal.messaging.messages.PlaceInQueueRequest;
import dev.slsk.internal.messaging.messages.PlaceInQueueResponse;
import dev.slsk.internal.messaging.messages.QueueDownloadRequest;
import dev.slsk.internal.messaging.messages.SearchResponseFactory;
import dev.slsk.internal.messaging.messages.TransferRequest;
import dev.slsk.internal.messaging.messages.TransferResponse;
import dev.slsk.internal.messaging.messages.UploadDenied;
import dev.slsk.internal.messaging.messages.UploadFailed;
import dev.slsk.internal.messaging.messages.UserInfoResponseFactory;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.MessageEvent;
import dev.slsk.internal.network.MessageReceivedEvent;
import dev.slsk.internal.search.SearchInternal;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;

/** Handles incoming messages from peer connections. */
public final class DefaultPeerMessageHandler implements PeerMessageHandler {
    private final PeerMessageHandlerClient client;
    private final DiagnosticSink diagnostic;
    private final CopyOnWriteArrayList<DiagnosticEventListener> diagnosticListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<PeerMessageHandlerEventListener<DownloadDeniedEvent>> downloadDeniedListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<PeerMessageHandlerEventListener<DownloadFailedEvent>> downloadFailedListeners =
            new CopyOnWriteArrayList<>();

    /** Creates a handler with its default diagnostic factory. */
    public DefaultPeerMessageHandler(PeerMessageHandlerClient client) {
        this(client, null);
    }

    /** Creates a handler. */
    public DefaultPeerMessageHandler(PeerMessageHandlerClient client, DiagnosticSink diagnosticFactory) {
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
    public void addDownloadDeniedListener(PeerMessageHandlerEventListener<DownloadDeniedEvent> listener) {
        downloadDeniedListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeDownloadDeniedListener(PeerMessageHandlerEventListener<DownloadDeniedEvent> listener) {
        downloadDeniedListeners.remove(listener);
    }

    @Override
    public void addDownloadFailedListener(PeerMessageHandlerEventListener<DownloadFailedEvent> listener) {
        downloadFailedListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeDownloadFailedListener(PeerMessageHandlerEventListener<DownloadFailedEvent> listener) {
        downloadFailedListeners.remove(listener);
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
        MessageCode.Peer code = new MessageReader<>(message, MessageCode.Peer.class).readCode();
        diagnostic.debug("Peer message received: " + code + " from "
                + connection.getUsername() + " ("
                + connection.getIpEndpoint() + ") (id: "
                + connection.getId() + ")");

        CompletableFuture<Void> operation;
        try {
            operation = switch (code) {
                case SEARCH_RESPONSE -> handleSearchResponse(message);
                case BROWSE_RESPONSE -> handleBrowseResponse(connection, message);
                case INFO_REQUEST -> handleInfoRequest(connection);
                case SEARCH_REQUEST -> handleSearchRequest(connection, message);
                case BROWSE_REQUEST -> handleBrowseRequest(connection);
                case FOLDER_CONTENTS_REQUEST -> handleFolderContentsRequest(connection, message);
                case FOLDER_CONTENTS_RESPONSE -> {
                    FolderContentsResponse response = FolderContentsResponse.fromByteArray(message);
                    client.getWaiter()
                            .complete(
                                    new WaitKey(
                                            MessageCode.Peer.FOLDER_CONTENTS_RESPONSE,
                                            connection.getUsername(),
                                            response.getToken()),
                                    response.getDirectories());
                    yield completed();
                }
                case INFO_RESPONSE -> {
                    UserInfo info = UserInfoResponseFactory.fromByteArray(message);
                    client.getWaiter()
                            .complete(new WaitKey(MessageCode.Peer.INFO_RESPONSE, connection.getUsername()), info);
                    yield completed();
                }
                case TRANSFER_RESPONSE -> {
                    TransferResponse response = TransferResponse.fromByteArray(message);
                    client.getWaiter()
                            .complete(
                                    new WaitKey(
                                            MessageCode.Peer.TRANSFER_RESPONSE,
                                            connection.getUsername(),
                                            response.getToken()),
                                    response);
                    yield completed();
                }
                case QUEUE_DOWNLOAD -> handleQueueDownload(connection, message);
                case TRANSFER_REQUEST -> handleTransferRequest(connection, message);
                case UPLOAD_DENIED -> {
                    handleUploadDenied(connection, message);
                    yield completed();
                }
                case PLACE_IN_QUEUE_RESPONSE -> {
                    PlaceInQueueResponse response = PlaceInQueueResponse.fromByteArray(message);
                    client.getWaiter()
                            .complete(
                                    new WaitKey(
                                            MessageCode.Peer.PLACE_IN_QUEUE_RESPONSE,
                                            connection.getUsername(),
                                            response.getFilename()),
                                    response);
                    yield completed();
                }
                case PLACE_IN_QUEUE_REQUEST -> {
                    PlaceInQueueRequest request = PlaceInQueueRequest.fromByteArray(message);
                    yield trySendPlaceInQueueAsync(connection, request.getFilename());
                }
                case UPLOAD_FAILED -> {
                    handleUploadFailed(connection, message);
                    yield completed();
                }
                default -> {
                    diagnostic.debug("Unhandled peer message: " + code + " from "
                            + connection.getUsername() + " ("
                            + connection.getIpEndpoint() + "); "
                            + message.length + " bytes");
                    yield completed();
                }
            };
        } catch (Throwable failure) {
            operation = CompletableFuture.failedFuture(failure);
        }
        return operation.handle((ignored, failure) -> {
            if (failure != null) {
                Throwable cause = unwrap(failure);
                diagnostic.warning(
                        "Error handling peer message: " + code + " from "
                                + connection.getUsername() + " ("
                                + connection.getIpEndpoint() + "); "
                                + message(cause),
                        cause);
            }
            return null;
        });
    }

    @Override
    public void handleMessageReceived(MessageConnection connection, MessageReceivedEvent eventData) {
        MessageCode.Peer code = MessageCode.Peer.fromValue(ByteBuffer.wrap(eventData.getCode())
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt());
        try {
            if (code == MessageCode.Peer.BROWSE_RESPONSE) {
                client.getWaiter()
                        .complete(
                                new WaitKey(Constants.WaitKey.BROWSE_RESPONSE_CONNECTION, connection.getUsername()),
                                new BrowseResponseConnection(eventData, connection));
            }
        } catch (Throwable failure) {
            diagnostic.warning(
                    "Error handling peer message: " + code + " from "
                            + connection.getUsername() + " ("
                            + connection.getIpEndpoint() + "); "
                            + message(failure),
                    failure);
        }
    }

    @Override
    public void handleMessageWritten(MessageConnection connection, MessageEvent eventData) {
        MessageCode.Peer code = new MessageReader<>(eventData.getMessage(), MessageCode.Peer.class).readCode();
        diagnostic.debug("Peer message sent: " + code + " ("
                + connection.getIpEndpoint() + ") (id: "
                + connection.getId() + ")");
    }

    private CompletableFuture<Void> handleSearchResponse(byte[] message) {
        SearchResponse response = SearchResponseFactory.fromByteArray(message);
        SearchInternal search = client.getSearches().get(response.getToken());
        if (search != null) {
            search.tryAddResponse(response);
        }
        return completed();
    }

    private CompletableFuture<Void> handleBrowseResponse(MessageConnection connection, byte[] message) {
        WaitKey key = new WaitKey(MessageCode.Peer.BROWSE_RESPONSE, connection.getUsername());
        try {
            client.getWaiter().complete(key, BrowseResponseFactory.fromByteArray(message));
        } catch (Throwable failure) {
            client.getWaiter()
                    .fail(key, new MessageReadException("The peer returned an invalid browse response", failure));
            throw failure;
        }
        return completed();
    }

    private CompletableFuture<Void> handleInfoRequest(MessageConnection connection) {
        // Read rather than resolved: the profile is a value this account set,
        // not a question to ask on every request.
        dev.slsk.UserProfile profile = client.getProfile();
        UserInfo info = new UserInfo(
                profile.description(),
                profile.uploadSlots(),
                profile.queueLength(),
                profile.hasFreeUploadSlot(),
                profile.picture().orElse(null));
        return connection
                .writeAsync(info.toByteArray())
                .thenRun(() -> diagnostic.info("User info sent to " + connection.getUsername()));
    }

    private CompletableFuture<Void> handleSearchRequest(MessageConnection connection, byte[] message) {
        PeerSearchRequest request = PeerSearchRequest.fromByteArray(message);
        CompletableFuture<SearchResponse> resolved = Catalogs.ask(() -> Catalogs.searchResponse(
                client.getLoggedInUsername(),
                request.getToken(),
                client.getShareCatalog()
                        .search(
                                dev.slsk.Username.of(connection.getUsername()),
                                request.getQuery(),
                                client.getOptions().getMaximumConcurrentSearches()),
                true,
                0,
                0));
        return resolved.thenCompose(response -> {
                    if (response instanceof RawSearchResponse raw) {
                        return connection
                                .writeAsync(raw.getLength(), raw.getStream())
                                .thenRun(() -> closeQuietly(raw.getStream()));
                    }
                    if (response != null && response.getFileCount() + response.getLockedFileCount() > 0) {
                        return connection.writeAsync(response.toByteArray());
                    }
                    return completed();
                })
                .exceptionally(failure -> {
                    Throwable cause = unwrap(failure);
                    diagnostic.warning(
                            "Error resolving search response for query '"
                                    + request.getQuery() + "' requested by "
                                    + connection.getUsername() + " with token "
                                    + request.getToken() + ": " + message(cause),
                            cause);
                    return null;
                });
    }

    private CompletableFuture<Void> handleBrowseRequest(MessageConnection connection) {
        CompletableFuture<BrowseResponse> resolved = Catalogs.ask(
                () -> Catalogs.browse(client.getShareCatalog().browse(dev.slsk.Username.of(connection.getUsername()))));
        return resolved.handle((response, failure) -> {
                    if (failure == null) {
                        return response;
                    }
                    Throwable cause = unwrap(failure);
                    // A catalog that throws is a bug in the application, not a
                    // reason to leave a peer hanging on a read that never
                    // completes. Answer with nothing, the same as a share we
                    // decline to show them.
                    diagnostic.warning("The share catalog failed to answer a browse: " + message(cause), cause);
                    return new BrowseResponse();
                })
                .thenCompose(response -> {
                    if (response instanceof RawBrowseResponse raw) {
                        return connection
                                .writeAsync(raw.getLength(), raw.getStream())
                                .thenRun(() -> closeQuietly(raw.getStream()));
                    }
                    return connection.writeAsync(response.toByteArray());
                })
                .thenRun(() -> diagnostic.info("Share contents sent to " + connection.getUsername()));
    }

    private CompletableFuture<Void> handleFolderContentsRequest(MessageConnection connection, byte[] message) {
        FolderContentsRequest request = FolderContentsRequest.fromByteArray(message);
        CompletableFuture<? extends Iterable<Directory>> resolved =
                Catalogs.ask(() -> Catalogs.directories(client.getShareCatalog()
                        .directory(dev.slsk.Username.of(connection.getUsername()), request.getDirectoryName())));
        return resolved.handle((directories, failure) -> {
                    if (failure != null) {
                        Throwable cause = unwrap(failure);
                        diagnostic.warning(
                                "The share catalog failed to answer a folder request: " + message(cause), cause);
                        return null;
                    }
                    return directories;
                })
                .thenCompose(directories -> {
                    if (directories == null) {
                        return completed();
                    }
                    FolderContentsResponse response =
                            new FolderContentsResponse(request.getToken(), request.getDirectoryName(), directories);
                    return connection
                            .writeAsync(response)
                            .thenRun(() -> diagnostic.info("Folder contents for " + request.getDirectoryName()
                                    + " sent to " + connection.getUsername()));
                });
    }

    private CompletableFuture<Void> handleQueueDownload(MessageConnection connection, byte[] message) {
        QueueDownloadRequest request = QueueDownloadRequest.fromByteArray(message);
        return tryEnqueueDownloadAsync(connection.getUsername(), connection.getIpEndpoint(), request.getFilename())
                .thenCompose(result -> result.rejected()
                        ? connection.writeAsync(new UploadDenied(request.getFilename(), result.message()))
                        : trySendPlaceInQueueAsync(connection, request.getFilename()));
    }

    private CompletableFuture<Void> handleTransferRequest(MessageConnection connection, byte[] message) {
        TransferRequest request = TransferRequest.fromByteArray(message);
        if (request.getDirection() == TransferDirection.UPLOAD) {
            boolean tracked = !client.getDownloadDictionary().isEmpty()
                    && client.getDownloadDictionary().values().stream()
                            .anyMatch(download -> Objects.equals(download.getUsername(), connection.getUsername())
                                    && Objects.equals(download.getFilename(), request.getFilename()));
            if (tracked) {
                client.getWaiter()
                        .complete(
                                new WaitKey(
                                        MessageCode.Peer.TRANSFER_REQUEST,
                                        connection.getUsername(),
                                        request.getFilename()),
                                request);
                return completed();
            }
            diagnostic.debug("Rejecting unknown upload from " + connection.getUsername()
                    + " for " + request.getFilename() + " with token "
                    + request.getToken());
            return connection.writeAsync(new TransferResponse(request.getToken(), "Cancelled"));
        }

        return tryEnqueueDownloadAsync(connection.getUsername(), connection.getIpEndpoint(), request.getFilename())
                .thenCompose(result -> {
                    if (result.rejected()) {
                        return connection
                                .writeAsync(new TransferResponse(request.getToken(), result.message()))
                                .thenCompose(ignored -> connection.writeAsync(
                                        new UploadDenied(request.getFilename(), result.message())));
                    }
                    return connection
                            .writeAsync(new TransferResponse(request.getToken(), "Queued"))
                            .thenCompose(ignored -> trySendPlaceInQueueAsync(connection, request.getFilename()));
                });
    }

    private void handleUploadDenied(MessageConnection connection, byte[] message) {
        UploadDenied denied = UploadDenied.fromByteArray(message);
        diagnostic.debug("Download of " + denied.getFilename() + " from "
                + connection.getUsername() + " was denied: "
                + denied.getMessage());
        client.getWaiter()
                .fail(
                        new WaitKey(MessageCode.Peer.TRANSFER_REQUEST, connection.getUsername(), denied.getFilename()),
                        new TransferRejectedException(denied.getMessage()));
        DownloadDeniedEvent eventData =
                new DownloadDeniedEvent(connection.getUsername(), denied.getFilename(), denied.getMessage());
        downloadDeniedListeners.forEach(listener -> listener.handle(this, eventData));
    }

    private void handleUploadFailed(MessageConnection connection, byte[] message) {
        UploadFailed failed = UploadFailed.fromByteArray(message);
        diagnostic.debug("Download of " + failed.getFilename() + " reported as failed by " + connection.getUsername());
        client.getWaiter()
                .fail(
                        new WaitKey(MessageCode.Peer.TRANSFER_REQUEST, connection.getUsername(), failed.getFilename()),
                        new TransferReportedFailedException("Download reported as failed by remote client"));
        DownloadFailedEvent eventData = new DownloadFailedEvent(connection.getUsername(), failed.getFilename());
        downloadFailedListeners.forEach(listener -> listener.handle(this, eventData));
    }

    /**
     * Asks the upload policy what to do about a peer's request.
     *
     * <p>Four callbacks used to answer parts of this and none of them could see
     * the others. One decision replaces them, taken off the read loop because a
     * policy is a consumer's code.
     */
    private CompletableFuture<EnqueueResult> tryEnqueueDownloadAsync(
            String username, java.net.InetSocketAddress endpoint, String filename) {
        return dev.slsk.internal.Catalogs.ask(() -> {
            dev.slsk.spi.UploadPolicy.Decision decision =
                    client.getUploadAdmission().decide(dev.slsk.Username.of(username), filename);
            if (decision instanceof dev.slsk.spi.UploadPolicy.Decision.Deny denied) {
                return new EnqueueResult(true, denied.message());
            }
            if (decision instanceof dev.slsk.spi.UploadPolicy.Decision.Allow) {
                client.serveUpload(dev.slsk.Username.of(username), filename);
            }
            return new EnqueueResult(false, "");
        });
    }

    /**
     * Tells a peer where they are in our queue.
     *
     * <p>The queue is the one the policy put them in, so there is nothing to
     * resolve: a peer that is not waiting gets no answer, which is what a peer
     * asking about a file we are not holding for them should get.
     */
    private CompletableFuture<Void> trySendPlaceInQueueAsync(MessageConnection connection, String filename) {
        Integer place = client.getUploadAdmission().place(dev.slsk.Username.of(connection.getUsername()), filename);
        return place == null ? completed() : connection.writeAsync(new PlaceInQueueResponse(filename, place));
    }

    private void raiseDiagnostic(DiagnosticEvent eventData) {
        diagnosticListeners.forEach(listener -> listener.handle(this, eventData));
    }

    private static CompletableFuture<Void> completed() {
        return CompletableFuture.completedFuture(null);
    }

    private static void closeQuietly(java.io.InputStream stream) {
        try {
            stream.close();
        } catch (IOException ignored) {
            // Source ignores stream disposal failures.
        }
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

    private record EnqueueResult(boolean rejected, String message) {}
}
