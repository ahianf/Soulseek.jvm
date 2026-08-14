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
import dev.slsk.internal.common.Constants;
import dev.slsk.internal.common.NetworkExecutor;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
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
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.internal.search.SearchInternal;
import dev.slsk.internal.transfer.TransferInternal;
import dev.slsk.internal.user.UserInfo;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/** Handles incoming messages from peer connections. */
public final class DefaultPeerMessageHandler implements PeerMessageHandler {
    private final Supplier<SoulseekClientOptions> options;
    private final Waiter waiter;
    private final Supplier<Map<Integer, SearchInternal>> searches;
    private final Supplier<Map<Integer, TransferInternal>> downloads;
    private final Supplier<String> loggedInUsername;

    /** What a peer's message can ask of us that is not the protocol. */
    private final PeerServices services;

    private final DiagnosticSink diagnostic;
    private final CopyOnWriteArrayList<DiagnosticEventListener> diagnosticListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<PeerMessageHandlerEventListener<DownloadDeniedEvent>> downloadDeniedListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<PeerMessageHandlerEventListener<DownloadFailedEvent>> downloadFailedListeners =
            new CopyOnWriteArrayList<>();

    /**
     * Where an answer to a peer runs.
     *
     * <p>Never the read loop. A share catalog and an upload policy are the
     * consumer's code and a socket write is the peer's latency, and neither
     * belongs in front of the next protocol message. Injectable so a test can
     * run answers on its own thread and assert them without waiting.
     */
    private final Executor responses;

    /** Creates a handler with its default diagnostic factory. */
    public DefaultPeerMessageHandler(
            Supplier<SoulseekClientOptions> options,
            Waiter waiter,
            Supplier<Map<Integer, SearchInternal>> searches,
            Supplier<Map<Integer, TransferInternal>> downloads,
            Supplier<String> loggedInUsername,
            PeerServices services) {
        this(options, waiter, searches, downloads, loggedInUsername, services, null);
    }

    /** Creates a handler. */
    public DefaultPeerMessageHandler(
            Supplier<SoulseekClientOptions> options,
            Waiter waiter,
            Supplier<Map<Integer, SearchInternal>> searches,
            Supplier<Map<Integer, TransferInternal>> downloads,
            Supplier<String> loggedInUsername,
            PeerServices services,
            DiagnosticSink diagnosticFactory) {
        this(
                options,
                waiter,
                searches,
                downloads,
                loggedInUsername,
                services,
                diagnosticFactory,
                NetworkExecutor.executor());
    }

    /** Creates a handler that answers peers on the supplied executor. */
    DefaultPeerMessageHandler(
            Supplier<SoulseekClientOptions> options,
            Waiter waiter,
            Supplier<Map<Integer, SearchInternal>> searches,
            Supplier<Map<Integer, TransferInternal>> downloads,
            Supplier<String> loggedInUsername,
            PeerServices services,
            DiagnosticSink diagnosticFactory,
            Executor responses) {
        this.options = Objects.requireNonNull(options, "options");
        this.waiter = Objects.requireNonNull(waiter, "waiter");
        this.searches = Objects.requireNonNull(searches, "searches");
        this.downloads = Objects.requireNonNull(downloads, "downloads");
        this.loggedInUsername = Objects.requireNonNull(loggedInUsername, "loggedInUsername");
        this.services = Objects.requireNonNull(services, "services");
        this.responses = Objects.requireNonNull(responses, "responses");
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

    /**
     * Decodes a peer message and dispatches it.
     *
     * <p>Runs on the connection's read loop, and everything it does directly is
     * a decode or a registry write. Anything that answers the peer — which means
     * anything that asks a consumer's catalog or policy, or writes to the
     * socket — goes to {@link #answer} instead, because a share lookup and a
     * socket write in front of the next protocol message is how one peer stalls
     * every message behind it.
     *
     * @param connection the connection the message arrived on
     * @param message the raw message
     */
    @Override
    public void handleMessageRead(MessageConnection connection, byte[] message) {
        MessageCode.Peer code;
        try {
            code = new MessageReader<>(message, MessageCode.Peer.class).readCode();
        } catch (IllegalArgumentException unknown) {
            // A newer peer client's message, not a broken connection. C#
            // parses tolerantly and ignores it in the switch default; throwing
            // here killed this peer's read loop.
            diagnostic.debug(
                    "Ignored an unknown peer message from " + connection.getUsername() + ": " + unknown.getMessage());
            return;
        }
        diagnostic.debug("Peer message received: " + code + " from "
                + connection.getUsername() + " ("
                + connection.getIpEndpoint() + ") (id: "
                + connection.getId() + ")");

        try {
            switch (code) {
                case SEARCH_RESPONSE -> handleSearchResponse(message);
                case BROWSE_RESPONSE -> handleBrowseResponse(connection, message);
                case INFO_REQUEST -> handleInfoRequest(connection);
                case SEARCH_REQUEST -> handleSearchRequest(connection, message);
                case BROWSE_REQUEST -> handleBrowseRequest(connection);
                case FOLDER_CONTENTS_REQUEST -> handleFolderContentsRequest(connection, message);
                case FOLDER_CONTENTS_RESPONSE -> {
                    FolderContentsResponse response = FolderContentsResponse.fromByteArray(message);
                    waiter.complete(
                            new WaitKey(
                                    MessageCode.Peer.FOLDER_CONTENTS_RESPONSE,
                                    connection.getUsername(),
                                    response.getToken()),
                            response.getDirectories());
                }
                case INFO_RESPONSE -> {
                    UserInfo info = UserInfoResponseFactory.fromByteArray(message);
                    waiter.complete(new WaitKey(MessageCode.Peer.INFO_RESPONSE, connection.getUsername()), info);
                }
                case TRANSFER_RESPONSE -> {
                    TransferResponse response = TransferResponse.fromByteArray(message);
                    waiter.complete(
                            new WaitKey(
                                    MessageCode.Peer.TRANSFER_RESPONSE, connection.getUsername(), response.getToken()),
                            response);
                }
                case QUEUE_DOWNLOAD -> handleQueueDownload(connection, message);
                case TRANSFER_REQUEST -> handleTransferRequest(connection, message);
                case UPLOAD_DENIED -> handleUploadDenied(connection, message);
                case PLACE_IN_QUEUE_RESPONSE -> {
                    PlaceInQueueResponse response = PlaceInQueueResponse.fromByteArray(message);
                    waiter.complete(
                            new WaitKey(
                                    MessageCode.Peer.PLACE_IN_QUEUE_RESPONSE,
                                    connection.getUsername(),
                                    response.getFilename()),
                            response);
                    // Both, and the second is not redundant. Completing the wait
                    // answers whoever asked; a peer that volunteers a place —
                    // and this library's own uploader volunteers one in reply to
                    // a QueueUpload — has nobody waiting, so without this the
                    // only unprompted news a peer ever sends about its queue was
                    // read and dropped.
                    services.queuePosition(
                            connection.getUsername(), response.getFilename(), response.getPlaceInQueue());
                }
                case PLACE_IN_QUEUE_REQUEST -> {
                    PlaceInQueueRequest request = PlaceInQueueRequest.fromByteArray(message);
                    answer(code, connection, () -> trySendPlaceInQueue(connection, request.getFilename()));
                }
                case UPLOAD_FAILED -> handleUploadFailed(connection, message);
                default ->
                    diagnostic.debug("Unhandled peer message: " + code + " from "
                            + connection.getUsername() + " ("
                            + connection.getIpEndpoint() + "); "
                            + message.length + " bytes");
            }
        } catch (Throwable failure) {
            report(code, connection, failure);
        }
    }

    /**
     * Runs a peer's answer off the read loop, reporting whatever escapes it.
     *
     * @param code the message being answered, for the diagnostic
     * @param connection the peer being answered
     * @param work the answer
     */
    private void answer(MessageCode.Peer code, MessageConnection connection, Runnable work) {
        responses.execute(() -> {
            try {
                work.run();
            } catch (Throwable failure) {
                report(code, connection, failure);
            }
        });
    }

    private void report(MessageCode.Peer code, MessageConnection connection, Throwable failure) {
        Throwable cause = unwrap(failure);
        diagnostic.warning(
                "Error handling peer message: " + code + " from "
                        + connection.getUsername() + " ("
                        + connection.getIpEndpoint() + "); "
                        + message(cause),
                cause);
    }

    @Override
    public void handleMessageReceived(MessageConnection connection, MessageReceivedEvent eventData) {
        MessageCode.Peer code;
        try {
            code = MessageCode.Peer.fromValue(ByteBuffer.wrap(eventData.getCode())
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .getInt());
        } catch (IllegalArgumentException unknown) {
            diagnostic.debug(
                    "Ignored an unknown peer message from " + connection.getUsername() + ": " + unknown.getMessage());
            return;
        }
        try {
            if (code == MessageCode.Peer.BROWSE_RESPONSE) {
                waiter.complete(
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

    private void handleSearchResponse(byte[] message) {
        SearchResponse response = SearchResponseFactory.fromByteArray(message);
        SearchInternal search = searches.get().get(response.getToken());
        if (search != null) {
            search.tryAddResponse(response);
        }
    }

    private void handleBrowseResponse(MessageConnection connection, byte[] message) {
        WaitKey key = new WaitKey(MessageCode.Peer.BROWSE_RESPONSE, connection.getUsername());
        try {
            waiter.complete(key, BrowseResponseFactory.fromByteArray(message));
        } catch (Throwable failure) {
            waiter.fail(key, new MessageReadException("The peer returned an invalid browse response", failure));
            throw failure;
        }
    }

    private void handleInfoRequest(MessageConnection connection) {
        // Read rather than resolved: the profile is a value this account set,
        // not a question to ask on every request.
        dev.slsk.user.UserProfile profile = services.profile();
        UserInfo info = new UserInfo(
                profile.description(),
                profile.uploadSlots(),
                profile.queueLength(),
                profile.hasFreeUploadSlot(),
                profile.picture().orElse(null));
        answer(MessageCode.Peer.INFO_REQUEST, connection, () -> {
            connection.write(info.toByteArray());
            diagnostic.info("User info sent to " + connection.getUsername());
        });
    }

    private void handleSearchRequest(MessageConnection connection, byte[] message) {
        PeerSearchRequest request = PeerSearchRequest.fromByteArray(message);
        answer(MessageCode.Peer.SEARCH_REQUEST, connection, () -> {
            SearchResponse response;
            try {
                response = Catalogs.searchResponse(
                        loggedInUsername.get(),
                        request.getToken(),
                        // The match cap, not the concurrency option that used
                        // to be passed here — which silently answered every
                        // direct peer search with at most two files.
                        services.catalog()
                                .search(
                                        dev.slsk.user.Username.of(connection.getUsername()),
                                        request.getQuery(),
                                        Catalogs.MAXIMUM_SEARCH_MATCHES),
                        true,
                        0,
                        0);
            } catch (Throwable failure) {
                Throwable cause = unwrap(failure);
                diagnostic.warning(
                        "Error resolving search response for query '"
                                + request.getQuery() + "' requested by "
                                + connection.getUsername() + " with token "
                                + request.getToken() + ": " + message(cause),
                        cause);
                return;
            }
            if (response instanceof RawSearchResponse raw) {
                connection.write(raw.getLength(), raw.getStream());
                closeQuietly(raw.getStream());
            } else if (response != null && response.getFileCount() + response.getLockedFileCount() > 0) {
                connection.write(response.toByteArray());
            }
        });
    }

    private void handleBrowseRequest(MessageConnection connection) {
        answer(MessageCode.Peer.BROWSE_REQUEST, connection, () -> {
            BrowseResponse response;
            try {
                response =
                        Catalogs.browse(services.catalog().browse(dev.slsk.user.Username.of(connection.getUsername())));
            } catch (Throwable failure) {
                Throwable cause = unwrap(failure);
                // A catalog that throws is a bug in the application, not a
                // reason to leave a peer hanging on a read that never
                // completes. Answer with nothing, the same as a share we
                // decline to show them.
                diagnostic.warning("The share catalog failed to answer a browse: " + message(cause), cause);
                response = new BrowseResponse();
            }
            if (response instanceof RawBrowseResponse raw) {
                connection.write(raw.getLength(), raw.getStream());
                closeQuietly(raw.getStream());
            } else {
                connection.write(response.toByteArray());
            }
            diagnostic.info("Share contents sent to " + connection.getUsername());
        });
    }

    private void handleFolderContentsRequest(MessageConnection connection, byte[] message) {
        FolderContentsRequest request = FolderContentsRequest.fromByteArray(message);
        answer(MessageCode.Peer.FOLDER_CONTENTS_REQUEST, connection, () -> {
            Iterable<Directory> directories;
            try {
                directories = Catalogs.directories(services.catalog()
                        .directory(dev.slsk.user.Username.of(connection.getUsername()), request.getDirectoryName()));
            } catch (Throwable failure) {
                Throwable cause = unwrap(failure);
                diagnostic.warning("The share catalog failed to answer a folder request: " + message(cause), cause);
                return;
            }
            connection.write(new FolderContentsResponse(request.getToken(), request.getDirectoryName(), directories));
            diagnostic.info(
                    "Folder contents for " + request.getDirectoryName() + " sent to " + connection.getUsername());
        });
    }

    private void handleQueueDownload(MessageConnection connection, byte[] message) {
        QueueDownloadRequest request = QueueDownloadRequest.fromByteArray(message);
        answer(MessageCode.Peer.QUEUE_DOWNLOAD, connection, () -> {
            EnqueueResult result =
                    tryEnqueueDownload(connection.getUsername(), connection.getIpEndpoint(), request.getFilename());
            if (result.rejected()) {
                connection.write(new UploadDenied(request.getFilename(), result.message()));
            } else {
                trySendPlaceInQueue(connection, request.getFilename());
            }
        });
    }

    private void handleTransferRequest(MessageConnection connection, byte[] message) {
        TransferRequest request = TransferRequest.fromByteArray(message);
        if (request.getDirection() == TransferDirection.UPLOAD) {
            boolean tracked = !downloads.get().isEmpty()
                    && downloads.get().values().stream()
                            .anyMatch(download -> Objects.equals(download.getUsername(), connection.getUsername())
                                    && Objects.equals(download.getFilename(), request.getFilename()));
            if (tracked) {
                waiter.complete(
                        new WaitKey(MessageCode.Peer.TRANSFER_REQUEST, connection.getUsername(), request.getFilename()),
                        request);
                return;
            }
            // Not live in the engine, which is not the same as not wanted: the
            // queue holds downloads that have not been given a slot yet, and
            // this offer is the peer telling us our turn has come. Dispatched:
            // taking the offer ends in the consumer's TransferStore.save, and
            // consumer code never runs on a read loop.
            answer(MessageCode.Peer.TRANSFER_REQUEST, connection, () -> {
                PeerServices.OfferDisposition disposition =
                        services.offered(connection.getUsername(), request.getFilename(), request);
                if (disposition == PeerServices.OfferDisposition.TAKEN) {
                    diagnostic.debug("Taking up an offered upload from " + connection.getUsername()
                            + " for " + request.getFilename() + " with token "
                            + request.getToken() + "; the queued download starts now");
                    // Deliberately no reply. The download writes the acceptance
                    // once it has the peer connection, exactly as it would have
                    // done had it been waiting on this message all along.
                    return;
                }

                String reason = disposition == PeerServices.OfferDisposition.COMPLETE ? "Complete" : "Cancelled";
                diagnostic.debug("Rejecting unknown upload from " + connection.getUsername()
                        + " for " + request.getFilename() + " with token "
                        + request.getToken() + " (" + reason + ")");
                connection.write(new TransferResponse(request.getToken(), reason));
            });
            return;
        }

        answer(MessageCode.Peer.TRANSFER_REQUEST, connection, () -> {
            EnqueueResult result =
                    tryEnqueueDownload(connection.getUsername(), connection.getIpEndpoint(), request.getFilename());
            if (result.rejected()) {
                connection.write(new TransferResponse(request.getToken(), result.message()));
                connection.write(new UploadDenied(request.getFilename(), result.message()));
            } else {
                connection.write(new TransferResponse(request.getToken(), "Queued"));
                trySendPlaceInQueue(connection, request.getFilename());
            }
        });
    }

    private void handleUploadDenied(MessageConnection connection, byte[] message) {
        UploadDenied denied = UploadDenied.fromByteArray(message);
        diagnostic.debug("Download of " + denied.getFilename() + " from "
                + connection.getUsername() + " was denied: "
                + denied.getMessage());
        waiter.fail(
                new WaitKey(MessageCode.Peer.TRANSFER_REQUEST, connection.getUsername(), denied.getFilename()),
                new TransferRejectedException(denied.getMessage()));
        DownloadDeniedEvent eventData =
                new DownloadDeniedEvent(connection.getUsername(), denied.getFilename(), denied.getMessage());
        downloadDeniedListeners.forEach(listener -> listener.handle(this, eventData));
    }

    private void handleUploadFailed(MessageConnection connection, byte[] message) {
        UploadFailed failed = UploadFailed.fromByteArray(message);
        diagnostic.debug("Download of " + failed.getFilename() + " reported as failed by " + connection.getUsername());
        waiter.fail(
                new WaitKey(MessageCode.Peer.TRANSFER_REQUEST, connection.getUsername(), failed.getFilename()),
                new TransferReportedFailedException("Download reported as failed by remote client"));
        DownloadFailedEvent eventData = new DownloadFailedEvent(connection.getUsername(), failed.getFilename());
        downloadFailedListeners.forEach(listener -> listener.handle(this, eventData));
    }

    /**
     * Asks the upload policy what to do about a peer's request.
     *
     * <p>Four callbacks used to answer parts of this and none of them could see
     * the others. One decision replaces them. Runs where its caller runs, and
     * every caller is already off the read loop, because a policy is a
     * consumer's code.
     */
    private EnqueueResult tryEnqueueDownload(String username, java.net.InetSocketAddress endpoint, String filename) {
        dev.slsk.spi.UploadPolicy.Decision decision =
                services.admission().decide(dev.slsk.user.Username.of(username), filename);
        if (decision instanceof dev.slsk.spi.UploadPolicy.Decision.Deny denied) {
            return new EnqueueResult(true, denied.message());
        }
        if (decision instanceof dev.slsk.spi.UploadPolicy.Decision.Allow) {
            try {
                services.serve(dev.slsk.user.Username.of(username), filename);
            } catch (RuntimeException failure) {
                // The admission catches a throwing policy itself; this is the
                // upload failing to start after the policy said yes. The peer
                // still deserves an answer, and — as the C# source does for
                // any enqueue failure — a generic one: the real message can
                // carry filesystem details a stranger should not see. Silence
                // would leave the peer hanging until its own timeout.
                diagnostic.warning(
                        "Failed to start serving " + filename + " to " + username + ": " + message(failure),
                        unwrap(failure));
                return new EnqueueResult(true, "Enqueue failed due to internal error");
            }
        }
        return new EnqueueResult(false, "");
    }

    /**
     * Tells a peer where they are in our queue.
     *
     * <p>The queue is the one the policy put them in, so there is nothing to
     * resolve: a peer that is not waiting gets no answer, which is what a peer
     * asking about a file we are not holding for them should get.
     */
    private void trySendPlaceInQueue(MessageConnection connection, String filename) {
        Integer place = services.admission().place(dev.slsk.user.Username.of(connection.getUsername()), filename);
        if (place != null) {
            connection.write(new PlaceInQueueResponse(filename, place));
        }
    }

    private void raiseDiagnostic(DiagnosticEvent eventData) {
        diagnosticListeners.forEach(listener -> listener.handle(this, eventData));
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
