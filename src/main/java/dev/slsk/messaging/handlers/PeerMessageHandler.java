// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.handlers;

import dev.slsk.BrowseResponse;
import dev.slsk.Directory;
import dev.slsk.RawBrowseResponse;
import dev.slsk.RawSearchResponse;
import dev.slsk.SearchQuery;
import dev.slsk.SearchResponse;
import dev.slsk.TransferDirection;
import dev.slsk.UserInfo;
import dev.slsk.common.Constants;
import dev.slsk.common.WaitKey;
import dev.slsk.diagnostics.DiagnosticEventArgs;
import dev.slsk.diagnostics.DiagnosticEventListener;
import dev.slsk.diagnostics.DiagnosticFactory;
import dev.slsk.diagnostics.IDiagnosticFactory;
import dev.slsk.eventargs.DownloadDeniedEventArgs;
import dev.slsk.eventargs.DownloadFailedEventArgs;
import dev.slsk.exceptions.DownloadEnqueueException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.exceptions.TransferReportedFailedException;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;
import dev.slsk.messaging.messages.BrowseResponseFactory;
import dev.slsk.messaging.messages.FolderContentsRequest;
import dev.slsk.messaging.messages.FolderContentsResponse;
import dev.slsk.messaging.messages.PeerSearchRequest;
import dev.slsk.messaging.messages.PlaceInQueueRequest;
import dev.slsk.messaging.messages.PlaceInQueueResponse;
import dev.slsk.messaging.messages.QueueDownloadRequest;
import dev.slsk.messaging.messages.SearchResponseFactory;
import dev.slsk.messaging.messages.TransferRequest;
import dev.slsk.messaging.messages.TransferResponse;
import dev.slsk.messaging.messages.UploadDenied;
import dev.slsk.messaging.messages.UploadFailed;
import dev.slsk.messaging.messages.UserInfoResponseFactory;
import dev.slsk.network.IMessageConnection;
import dev.slsk.network.MessageEventArgs;
import dev.slsk.network.MessageReceivedEventArgs;
import dev.slsk.options.SoulseekClientOptions;
import dev.slsk.search.SearchInternal;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;

/** Handles incoming messages from peer connections. */
public final class PeerMessageHandler implements IPeerMessageHandler {
    private final PeerMessageHandlerClient client;
    private final IDiagnosticFactory diagnostic;
    private final CopyOnWriteArrayList<DiagnosticEventListener> diagnosticListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<PeerMessageHandlerEventListener<DownloadDeniedEventArgs>>
            downloadDeniedListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<PeerMessageHandlerEventListener<DownloadFailedEventArgs>>
            downloadFailedListeners = new CopyOnWriteArrayList<>();

    /** Creates a handler with its default diagnostic factory. */
    public PeerMessageHandler(PeerMessageHandlerClient client) {
        this(client, null);
    }

    /** Creates a handler. */
    public PeerMessageHandler(PeerMessageHandlerClient client, IDiagnosticFactory diagnosticFactory) {
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
    public void addDownloadDeniedListener(PeerMessageHandlerEventListener<DownloadDeniedEventArgs> listener) {
        downloadDeniedListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeDownloadDeniedListener(PeerMessageHandlerEventListener<DownloadDeniedEventArgs> listener) {
        downloadDeniedListeners.remove(listener);
    }

    @Override
    public void addDownloadFailedListener(PeerMessageHandlerEventListener<DownloadFailedEventArgs> listener) {
        downloadFailedListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public void removeDownloadFailedListener(PeerMessageHandlerEventListener<DownloadFailedEventArgs> listener) {
        downloadFailedListeners.remove(listener);
    }

    @Override
    public void handleMessageRead(IMessageConnection sender, MessageEventArgs eventArgs) {
        handleMessageRead(sender, eventArgs.getMessage());
    }

    @Override
    public void handleMessageRead(IMessageConnection sender, byte[] message) {
        handleMessageReadAsync(sender, message);
    }

    CompletableFuture<Void> handleMessageReadAsync(IMessageConnection connection, byte[] message) {
        MessageCode.Peer code = new MessageReader<>(message, MessageCode.Peer.class).readCode();
        diagnostic.debug("Peer message received: " + code + " from "
                + connection.getUsername() + " ("
                + connection.getIpEndPoint() + ") (id: "
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
                            + connection.getIpEndPoint() + "); "
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
                                + connection.getIpEndPoint() + "); "
                                + message(cause),
                        cause);
            }
            return null;
        });
    }

    @Override
    public void handleMessageReceived(IMessageConnection connection, MessageReceivedEventArgs eventArgs) {
        MessageCode.Peer code = MessageCode.Peer.fromValue(ByteBuffer.wrap(eventArgs.getCode())
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt());
        try {
            if (code == MessageCode.Peer.BROWSE_RESPONSE) {
                client.getWaiter()
                        .complete(
                                new WaitKey(Constants.WaitKey.BROWSE_RESPONSE_CONNECTION, connection.getUsername()),
                                new BrowseResponseConnection(eventArgs, connection));
            }
        } catch (Throwable failure) {
            diagnostic.warning(
                    "Error handling peer message: " + code + " from "
                            + connection.getUsername() + " ("
                            + connection.getIpEndPoint() + "); "
                            + message(failure),
                    failure);
        }
    }

    @Override
    public void handleMessageWritten(IMessageConnection connection, MessageEventArgs eventArgs) {
        MessageCode.Peer code = new MessageReader<>(eventArgs.getMessage(), MessageCode.Peer.class).readCode();
        diagnostic.debug("Peer message sent: " + code + " ("
                + connection.getIpEndPoint() + ") (id: "
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

    private CompletableFuture<Void> handleBrowseResponse(IMessageConnection connection, byte[] message) {
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

    private CompletableFuture<Void> handleInfoRequest(IMessageConnection connection) {
        CompletableFuture<UserInfo> resolved;
        try {
            resolved = client.getOptions()
                    .getUserInfoResolver()
                    .resolve(connection.getUsername(), connection.getIpEndPoint());
        } catch (Throwable failure) {
            resolved = CompletableFuture.failedFuture(failure);
        }
        return resolved.handle((info, failure) -> {
                    if (failure == null) {
                        return CompletableFuture.completedFuture(info);
                    }
                    Throwable cause = unwrap(failure);
                    diagnostic.warning("Failed to resolve user info response: " + message(cause), cause);
                    return new SoulseekClientOptions()
                            .getUserInfoResolver()
                            .resolve(connection.getUsername(), connection.getIpEndPoint());
                })
                .thenCompose(future -> future)
                .thenCompose(info -> connection.writeAsync(info.toByteArray()))
                .thenRun(() -> diagnostic.info("User info sent to " + connection.getUsername()));
    }

    private CompletableFuture<Void> handleSearchRequest(IMessageConnection connection, byte[] message) {
        PeerSearchRequest request = PeerSearchRequest.fromByteArray(message);
        if (client.getOptions().getSearchResponseResolver() == null) {
            return completed();
        }
        CompletableFuture<SearchResponse> resolved;
        try {
            resolved = client.getOptions()
                    .getSearchResponseResolver()
                    .resolve(connection.getUsername(), request.getToken(), SearchQuery.fromText(request.getQuery()));
        } catch (Throwable failure) {
            resolved = CompletableFuture.failedFuture(failure);
        }
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

    private CompletableFuture<Void> handleBrowseRequest(IMessageConnection connection) {
        CompletableFuture<BrowseResponse> resolved;
        try {
            resolved = client.getOptions()
                    .getBrowseResponseResolver()
                    .resolve(connection.getUsername(), connection.getIpEndPoint());
        } catch (Throwable failure) {
            resolved = CompletableFuture.failedFuture(failure);
        }
        return resolved.handle((response, failure) -> {
                    if (failure == null) {
                        return CompletableFuture.completedFuture(response);
                    }
                    Throwable cause = unwrap(failure);
                    diagnostic.warning("Failed to resolve browse response: " + message(cause), cause);
                    return new SoulseekClientOptions()
                            .getBrowseResponseResolver()
                            .resolve(connection.getUsername(), connection.getIpEndPoint());
                })
                .thenCompose(future -> future)
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

    private CompletableFuture<Void> handleFolderContentsRequest(IMessageConnection connection, byte[] message) {
        FolderContentsRequest request = FolderContentsRequest.fromByteArray(message);
        CompletableFuture<? extends Iterable<Directory>> resolved;
        try {
            resolved = client.getOptions()
                    .getDirectoryContentsResolver()
                    .resolve(
                            connection.getUsername(),
                            connection.getIpEndPoint(),
                            request.getToken(),
                            request.getDirectoryName());
        } catch (Throwable failure) {
            resolved = CompletableFuture.failedFuture(failure);
        }
        return resolved.handle((directories, failure) -> {
                    if (failure != null) {
                        Throwable cause = unwrap(failure);
                        diagnostic.warning("Failed to resolve directory contents response: " + message(cause), cause);
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

    private CompletableFuture<Void> handleQueueDownload(IMessageConnection connection, byte[] message) {
        QueueDownloadRequest request = QueueDownloadRequest.fromByteArray(message);
        return tryEnqueueDownloadAsync(connection.getUsername(), connection.getIpEndPoint(), request.getFilename())
                .thenCompose(result -> result.rejected()
                        ? connection.writeAsync(new UploadDenied(request.getFilename(), result.message()))
                        : trySendPlaceInQueueAsync(connection, request.getFilename()));
    }

    private CompletableFuture<Void> handleTransferRequest(IMessageConnection connection, byte[] message) {
        TransferRequest request = TransferRequest.fromByteArray(message);
        if (request.getDirection() == TransferDirection.UPLOAD) {
            boolean tracked = !client.getDownloads().isEmpty()
                    && client.getDownloads().values().stream()
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

        return tryEnqueueDownloadAsync(connection.getUsername(), connection.getIpEndPoint(), request.getFilename())
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

    private void handleUploadDenied(IMessageConnection connection, byte[] message) {
        UploadDenied denied = UploadDenied.fromByteArray(message);
        diagnostic.debug("Download of " + denied.getFilename() + " from "
                + connection.getUsername() + " was denied: "
                + denied.getMessage());
        client.getWaiter()
                .fail(
                        new WaitKey(MessageCode.Peer.TRANSFER_REQUEST, connection.getUsername(), denied.getFilename()),
                        new TransferRejectedException(denied.getMessage()));
        DownloadDeniedEventArgs eventArgs =
                new DownloadDeniedEventArgs(connection.getUsername(), denied.getFilename(), denied.getMessage());
        downloadDeniedListeners.forEach(listener -> listener.handle(this, eventArgs));
    }

    private void handleUploadFailed(IMessageConnection connection, byte[] message) {
        UploadFailed failed = UploadFailed.fromByteArray(message);
        diagnostic.debug("Download of " + failed.getFilename() + " reported as failed by " + connection.getUsername());
        client.getWaiter()
                .fail(
                        new WaitKey(MessageCode.Peer.TRANSFER_REQUEST, connection.getUsername(), failed.getFilename()),
                        new TransferReportedFailedException("Download reported as failed by remote client"));
        DownloadFailedEventArgs eventArgs = new DownloadFailedEventArgs(connection.getUsername(), failed.getFilename());
        downloadFailedListeners.forEach(listener -> listener.handle(this, eventArgs));
    }

    private CompletableFuture<EnqueueResult> tryEnqueueDownloadAsync(
            String username, java.net.InetSocketAddress endpoint, String filename) {
        CompletableFuture<Void> enqueue;
        try {
            enqueue = client.getOptions().getEnqueueDownload().enqueue(username, endpoint, filename);
        } catch (Throwable failure) {
            enqueue = CompletableFuture.failedFuture(failure);
        }
        return enqueue.handle((ignored, failure) -> {
            if (failure == null) {
                return new EnqueueResult(false, "");
            }
            Throwable cause = unwrap(failure);
            if (cause instanceof DownloadEnqueueException) {
                return new EnqueueResult(true, message(cause));
            }
            diagnostic.warning("Failed to invoke QueueDownload action: " + message(cause), cause);
            return new EnqueueResult(true, "Enqueue failed due to internal error");
        });
    }

    private CompletableFuture<Void> trySendPlaceInQueueAsync(IMessageConnection connection, String filename) {
        CompletableFuture<Integer> resolved;
        try {
            resolved = client.getOptions()
                    .getPlaceInQueueResolver()
                    .resolve(connection.getUsername(), connection.getIpEndPoint(), filename);
        } catch (Throwable failure) {
            resolved = CompletableFuture.failedFuture(failure);
        }
        return resolved.handle((place, failure) -> {
                    if (failure != null) {
                        Throwable cause = unwrap(failure);
                        diagnostic.warning(
                                "Failed to resolve place in queue for file " + filename
                                        + " from " + connection.getUsername() + ": "
                                        + message(cause),
                                cause);
                        return null;
                    }
                    return place;
                })
                .thenCompose(place ->
                        place == null ? completed() : connection.writeAsync(new PlaceInQueueResponse(filename, place)));
    }

    private void raiseDiagnostic(DiagnosticEventArgs eventArgs) {
        diagnosticListeners.forEach(listener -> listener.handle(this, eventArgs));
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
