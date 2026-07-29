// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static dev.slsk.internal.TransferEngine.await;
import static dev.slsk.internal.TransferEngine.determineOutputPosition;
import static dev.slsk.internal.TransferEngine.filenameOnly;
import static dev.slsk.internal.TransferEngine.isQueuedResponse;
import static dev.slsk.internal.TransferEngine.seekOutputStream;

import dev.slsk.CancellationController;
import dev.slsk.CancellationSignal;
import dev.slsk.CancellationSubscription;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.exceptions.TransferSizeMismatchException;
import dev.slsk.exceptions.TransferStreamException;
import dev.slsk.exceptions.UserOfflineException;
import dev.slsk.internal.EngineEvents.Kind;
import dev.slsk.internal.common.CommonUtils;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.NetworkExecutor;
import dev.slsk.internal.common.Permits;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.events.TransferProgressUpdatedEvent;
import dev.slsk.internal.events.TransferStateChangedEvent;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.messages.TransferRequest;
import dev.slsk.internal.messaging.messages.TransferResponse;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.tcp.Connection;
import dev.slsk.internal.network.tcp.ConnectionDataEvent;
import dev.slsk.internal.network.tcp.ConnectionDisconnectedEvent;
import dev.slsk.internal.network.tcp.ConnectionEventListener;
import dev.slsk.internal.options.TransferOptions;
import dev.slsk.internal.options.TransferProgressUpdate;
import dev.slsk.internal.options.TransferStateChange;
import dev.slsk.internal.transfer.TransferInternal;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * One download in flight: slot acquisition, the peer handshake, the read loop
 * and the terminal state transitions.
 *
 * <p>Was an inner class of {@link TransferEngine}. Lifted out so neither type
 * carries the other's bulk; it keeps an explicit reference to the engine for
 * the concurrency limits and duplicate keys the engine owns, and reaches the
 * client through the same engine the transfer engine holds.
 */
final class DownloadOperation {

    /** The engine that owns the concurrency limits and duplicate keys. */
    private final TransferEngine engine;

    private final TransferInternal download;
    private final Supplier<OutputStream> outputStreamFactory;
    private final TransferOptions transferOptions;

    /**
     * A peer's standing offer of this file, or {@code null}.
     *
     * <p>Present when this download exists <em>because</em> the peer said it was
     * ready. Its token and size are the same ones the ordinary path waits for,
     * so having it up front does not shorten the handshake by guesswork — it
     * skips a round trip whose only possible answer we already hold.
     */
    private final TransferRequest offer;

    private final CancellationSignal cancellationSignal;
    private final String uniqueKey;
    private final AtomicBoolean globalPermit = new AtomicBoolean();
    private final CompletableFuture<Void> disconnected = new CompletableFuture<>();
    private final WaitKey transferStartRequestedWaitKey;
    private TransferState lastState = TransferState.NONE;
    private InetSocketAddress endpoint;
    private Connection connection;
    private OutputStream outputStream;
    private TransferEngine.PositionTrackingOutputStream trackingStream;
    private ConnectionEventListener<ConnectionDataEvent> dataReadListener;
    private ConnectionEventListener<ConnectionDisconnectedEvent> disconnectedListener;

    DownloadOperation(
            TransferEngine engine,
            TransferInternal download,
            Supplier<OutputStream> outputStreamFactory,
            TransferOptions transferOptions,
            TransferRequest offer,
            CancellationSignal cancellationSignal,
            String uniqueKey) {
        this.offer = offer;
        this.engine = engine;
        this.download = download;
        this.outputStreamFactory = outputStreamFactory;
        this.transferOptions = transferOptions;
        this.cancellationSignal = cancellationSignal;
        this.uniqueKey = uniqueKey;
        transferStartRequestedWaitKey =
                new WaitKey(MessageCode.Peer.TRANSFER_REQUEST, download.getUsername(), download.getFilename());
    }

    Transfer execute() {
        try {
            updateState(TransferState.QUEUED.or(TransferState.LOCALLY));
            Permits.acquire(engine.globalDownloadSemaphore, cancellationSignal);
            globalPermit.set(true);
            engine.context
                    .getDiagnostic()
                    .debug("Global download semaphore for file "
                            + filenameOnly(download.getFilename()) + " to "
                            + download.getUsername() + " acquired");

            endpoint = engine.context.resolveUserEndpoint(download.getUsername(), cancellationSignal);
            MessageConnection peerConnection = engine.context
                    .getPeerConnectionManager()
                    .getOrAddMessageConnection(download.getUsername(), endpoint, cancellationSignal);
            engine.context
                    .getDiagnostic()
                    .debug("Fetched peer connection for download of "
                            + filenameOnly(download.getFilename()) + " from "
                            + download.getUsername() + " (id: " + peerConnection.getId()
                            + ", state: " + peerConnection.getState() + ")");

            if (offer != null) {
                // The peer already told us it is ready, so there is nothing to
                // ask for. Asking anyway would send a fresh request against the
                // queue we have just reached the front of, and a peer with one
                // free slot would answer the second request with "Queued".
                engine.context
                        .getDiagnostic()
                        .debug("Download of " + filenameOnly(download.getFilename())
                                + " from " + download.getUsername()
                                + " is taking up an offer already made (remote token: "
                                + offer.getToken() + ")");
                updateState(TransferState.REQUESTED);
                beginQueuedDownload(() -> offer, peerConnection);
                return receiveFile();
            }

            Wait<TransferResponse> transferRequestAcknowledged = engine.context
                    .getWaiter()
                    .register(
                            new WaitKey(
                                    MessageCode.Peer.TRANSFER_RESPONSE, download.getUsername(), download.getToken()),
                            TransferResponse.class,
                            engine.context
                                    .getClientOptions()
                                    .getPeerConnectionOptions()
                                    .getInactivityTimeout(),
                            cancellationSignal);
            Wait<TransferRequest> transferStartRequested = engine.context
                    .getWaiter()
                    .registerIndefinitely(transferStartRequestedWaitKey, TransferRequest.class, cancellationSignal);

            peerConnection.write(
                    new TransferRequest(TransferDirection.DOWNLOAD, download.getToken(), download.getFilename()),
                    CommonUtils.token(cancellationSignal));
            engine.context
                    .getDiagnostic()
                    .debug("Wrote transfer request for download of "
                            + filenameOnly(download.getFilename()) + " from "
                            + download.getUsername() + " (id: " + peerConnection.getId()
                            + ", state: " + peerConnection.getState() + ")");
            updateState(TransferState.REQUESTED);

            TransferResponse acknowledgement = transferRequestAcknowledged.await();
            engine.context
                    .getDiagnostic()
                    .debug("Received transfer request ACK for download of "
                            + filenameOnly(download.getFilename()) + " from "
                            + download.getUsername() + ": allowed: " + acknowledgement.isAllowed()
                            + ", message: " + acknowledgement.getMessage()
                            + " (token: " + download.getToken() + ")");
            if (acknowledgement.isAllowed()) {
                peerConnection = beginImmediateDownload(acknowledgement, peerConnection);
            } else if (!isQueuedResponse(acknowledgement.getMessage())) {
                throw new TransferRejectedException("Transfer rejected: " + acknowledgement.getMessage());
            } else {
                peerConnection = beginQueuedDownload(transferStartRequested, peerConnection);
            }

            return receiveFile();
        } catch (Throwable failure) {
            Throwable cause = Failures.unwrap(failure);
            handleFailure(cause);
            throw new CompletionException(mapDownloadFailure(cause));
        } finally {
            cleanup();
        }
    }

    /**
     * Reads the file off the transfer connection.
     *
     * <p>Shared by both ways of getting here — asking a peer for a file, and
     * taking up a file a peer offered. Only the handshake differs; once there is
     * a transfer connection there is exactly one way to receive bytes, and
     * keeping it in one place is what stops the offered path drifting into a
     * second, less-tested copy of the download.
     *
     * @return the completed transfer
     */
    private Transfer receiveFile() throws IOException {
        bindConnectionEvents();
        outputStream = Objects.requireNonNull(outputStreamFactory.get(), "outputStreamFactory result");
        positionOutputStream();
        trackingStream = new TransferEngine.PositionTrackingOutputStream(
                outputStream,
                determineOutputPosition(
                        outputStream,
                        transferOptions.isSeekOutputStreamAutomatically() ? download.getStartOffset() : 0));
        readTransfer();

        updateProgress(currentOutputPosition());
        updateState(TransferState.COMPLETED.or(TransferState.SUCCEEDED));
        engine.context
                .getDiagnostic()
                .info("Download of " + filenameOnly(download.getFilename())
                        + " from " + download.getUsername() + " complete ("
                        + currentOutputPosition() + " of " + download.getSize() + " bytes).");
        connection.disconnect("Transfer complete");
        return download.toTransfer();
    }

    private MessageConnection beginImmediateDownload(
            TransferResponse acknowledgement, MessageConnection peerConnection) {
        validateRemoteSize(acknowledgement.getFileSize());
        updateState(TransferState.QUEUED.or(TransferState.REMOTELY));
        if (download.getSize() == null) {
            download.setSize(acknowledgement.getFileSize());
        }
        updateState(TransferState.INITIALIZING);
        connection = engine.context
                .getPeerConnectionManager()
                .getTransferConnection(
                        download.getUsername(), endpoint, acknowledgement.getToken(), cancellationSignal);
        engine.context
                .getDiagnostic()
                .debug("Fetched transfer connection for download of "
                        + filenameOnly(download.getFilename()) + " from "
                        + download.getUsername() + " (id: " + connection.getId()
                        + ", state: " + connection.getState() + ")");
        download.setConnection(connection);
        return peerConnection;
    }

    private MessageConnection beginQueuedDownload(
            Wait<TransferRequest> transferStartRequested, MessageConnection peerConnection) {
        updateState(TransferState.QUEUED.or(TransferState.REMOTELY));
        TransferRequest request = transferStartRequested.await();
        validateRemoteSize(request.getFileSize());
        if (download.getSize() == null) {
            download.setSize(request.getFileSize());
        }
        download.setRemoteToken(request.getToken());
        updateState(TransferState.INITIALIZING);

        MessageConnection refreshed = engine.context
                .getPeerConnectionManager()
                .getOrAddMessageConnection(download.getUsername(), endpoint, cancellationSignal);
        engine.context
                .getDiagnostic()
                .debug("Fetched peer connection for download of "
                        + filenameOnly(download.getFilename()) + " from "
                        + download.getUsername() + " (id: " + refreshed.getId()
                        + ", state: " + refreshed.getState() + ")");
        // Started before the acceptance is written, because the peer opens the
        // transfer connection the moment it reads it.
        CompletableFuture<Connection> connectionTask = NetworkExecutor.supplyAsync(() -> engine.context
                .getPeerConnectionManager()
                .awaitTransferConnection(
                        download.getUsername(), download.getFilename(), download.getRemoteToken(), cancellationSignal));
        refreshed.write(
                new TransferResponse(download.getRemoteToken(), download.getSize() == null ? 0 : download.getSize()),
                CommonUtils.token(cancellationSignal));
        try {
            connection = await(connectionTask);
            engine.context
                    .getDiagnostic()
                    .debug("Fetched transfer connection for download of "
                            + filenameOnly(download.getFilename()) + " from "
                            + download.getUsername() + " (id: " + connection.getId()
                            + ", state: " + connection.getState() + ")");
        } catch (Throwable failure) {
            Throwable cause = Failures.unwrap(failure);
            if (!(cause instanceof ConnectionException)) {
                throw failure;
            }
            // The remote client never initiated the transfer connection, so initiate one from
            // this end. The remote client in this scenario is most likely Nicotine+.
            engine.context
                    .getDiagnostic()
                    .warning("Attempting to initiate a second-chance transfer connection to " + download.getUsername()
                            + " for download of " + download.getFilename());
            connection = engine.context
                    .getPeerConnectionManager()
                    .getTransferConnection(
                            download.getUsername(), endpoint, download.getRemoteToken(), cancellationSignal);
            engine.context
                    .getDiagnostic()
                    .warning("Successfully established a second-chance transfer connection to " + download.getUsername()
                            + " for download of " + download.getFilename());
        }
        download.setConnection(connection);
        return refreshed;
    }

    private void validateRemoteSize(long remoteSize) {
        if (download.getSize() != null && download.getSize() != remoteSize) {
            throw new TransferSizeMismatchException(
                    "Transfer aborted: the remote size of "
                            + remoteSize
                            + " does not match expected size "
                            + download.getSize(),
                    download.getSize(),
                    remoteSize);
        }
    }

    private void bindConnectionEvents() {
        dataReadListener =
                (sender, eventData) -> updateProgress(download.getStartOffset() + eventData.getCurrentLength());
        disconnectedListener = (sender, eventData) -> {
            Throwable failure = eventData.getException();
            if (failure instanceof CancellationException || failure instanceof TimeoutException) {
                disconnected.completeExceptionally(failure);
            } else {
                disconnected.completeExceptionally(
                        new ConnectionException("Transfer failed: " + eventData.getMessage(), failure));
            }
        };
        connection.addDataReadListener(dataReadListener);
        connection.addDisconnectedListener(disconnectedListener);
    }

    private void positionOutputStream() {
        if (download.getStartOffset() <= 0 || !transferOptions.isSeekOutputStreamAutomatically()) {
            return;
        }
        engine.context
                .getDiagnostic()
                .debug("Seeking output stream for download of "
                        + filenameOnly(download.getFilename()) + " from "
                        + download.getUsername() + " to starting offset of "
                        + download.getStartOffset() + " bytes");
        try {
            seekOutputStream(outputStream, download.getStartOffset());
        } catch (IOException failure) {
            throw new TransferStreamException(
                    "Requested non-zero start offset but output " + "stream does not support seeking", failure);
        }
    }

    private void readTransfer() {
        try (CancellationController linkedController = new CancellationController();
                CancellationSubscription registration = cancellationSignal.register(linkedController::cancel)) {
            CancellationSignal linkedToken = linkedController.getSignal();
            engine.context
                    .getDiagnostic()
                    .debug("Seeking download of " + filenameOnly(download.getFilename())
                            + " from " + download.getUsername() + " to starting offset of "
                            + download.getStartOffset() + " bytes");
            byte[] offset = ByteBuffer.allocate(8)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .putLong(download.getStartOffset())
                    .array();
            connection.write(offset, linkedToken);
            updateState(TransferState.IN_PROGRESS);
            updateProgress(download.getStartOffset());

            // A thread of its own because the read is raced against the
            // connection dropping and against the peer completing the transfer
            // out from under it; racing is work blocking code cannot do.
            CompletableFuture<Void> read = NetworkExecutor.runAsync(() -> connection.read(
                    download.getSize() - download.getStartOffset(),
                    trackingStream,
                    // The bucket is the whole of the metering now. A pluggable
                    // per-transfer governor sat in front of it and every
                    // implementation granted everything, which is what the
                    // bucket already does when the rate is unlimited.
                    (requestedBytes, governorToken) ->
                            engine.context.getDownloadTokenBucket().get(requestedBytes, governorToken),
                    (attemptedBytes, grantedBytes, transferredBytes) -> {
                        if (transferOptions.getReporter() != null) {
                            transferOptions
                                    .getReporter()
                                    .report(download.toTransfer(), attemptedBytes, grantedBytes, transferredBytes);
                        }
                        engine.context.getDownloadTokenBucket().returnTokens(grantedBytes - transferredBytes);
                    },
                    linkedToken));

            CompletableFuture<Integer> readRace = read.handle((ignored, failure) -> 0);
            CompletableFuture<Integer> disconnectRace = disconnected.handle((ignored, failure) -> 1);
            CompletableFuture<Integer> remoteRace =
                    download.getRemoteTaskCompletionSource().handle((ignored, failure) -> 2);
            int winner = await(CompletableFuture.anyOf(readRace, disconnectRace, remoteRace)
                    .thenApply(value -> (Integer) value));
            linkedController.cancel();
            if (winner == 2) {
                await(download.getRemoteTaskCompletionSource());
            } else if (winner == 1) {
                await(disconnected);
            }
            await(read);
        }
    }

    private void handleFailure(Throwable failure) {
        if (failure instanceof TransferRejectedException) {
            download.setException(failure);
            updateState(TransferState.COMPLETED.or(TransferState.REJECTED));
            return;
        }
        if (failure instanceof TransferSizeMismatchException) {
            download.setException(failure);
            updateState(TransferState.COMPLETED.or(TransferState.ABORTED));
            return;
        }
        if (failure instanceof CancellationException) {
            disconnectTransfer("Transfer cancelled", failure);
            download.setException(failure);
            updateProgress(currentOutputPosition());
            updateState(TransferState.COMPLETED.or(TransferState.CANCELLED));
            return;
        }
        if (failure instanceof TimeoutException) {
            disconnectTransfer("Transfer timed out", failure);
            download.setException(failure);
            updateProgress(currentOutputPosition());
            updateState(TransferState.COMPLETED.or(TransferState.TIMED_OUT));
            return;
        }
        disconnectTransfer("Transfer error", failure);
        download.setException(failure);
        updateProgress(currentOutputPosition());
        updateState(TransferState.COMPLETED.or(TransferState.ERRORED));
    }

    private Throwable mapDownloadFailure(Throwable failure) {
        if (failure instanceof TransferRejectedException
                || failure instanceof TransferSizeMismatchException
                || failure instanceof CancellationException
                || failure instanceof TimeoutException
                || failure instanceof UserOfflineException) {
            return failure;
        }
        return new SoulseekClientException(
                "Failed to download file "
                        + download.getFilename()
                        + " from user " + download.getUsername()
                        + ": " + Failures.message(failure),
                failure);
    }

    private void disconnectTransfer(String message, Throwable failure) {
        if (connection != null) {
            connection.disconnect(
                    message, failure instanceof Exception exception ? exception : new RuntimeException(failure));
        }
    }

    private void cleanup() {
        try {
            try {
                engine.context.getWaiter().cancel(transferStartRequestedWaitKey);
            } catch (Throwable failure) {
                engine.context
                        .getDiagnostic()
                        .warning(
                                "Failed to cancel wait for key "
                                        + transferStartRequestedWaitKey
                                        + ": " + Failures.message(failure),
                                failure);
            }
            try {
                unbindConnectionEvents();
            } catch (Throwable failure) {
                engine.context
                        .getDiagnostic()
                        .warning(
                                "Failed to remove transfer connection "
                                        + "listeners for file "
                                        + download.getFilename() + " from user "
                                        + download.getUsername() + ": "
                                        + Failures.message(failure),
                                failure);
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (Throwable failure) {
                    engine.context
                            .getDiagnostic()
                            .warning(
                                    "Failed to dispose transfer connection "
                                            + "for file "
                                            + download.getFilename()
                                            + " from user "
                                            + download.getUsername() + ": "
                                            + Failures.message(failure),
                                    failure);
                }
            }
            determineFinalOutputPosition();
            if (transferOptions.isDisposeOutputStreamOnCompletion() && outputStream != null) {
                try {
                    try {
                        outputStream.flush();
                    } finally {
                        outputStream.close();
                    }
                } catch (Throwable failure) {
                    engine.context
                            .getDiagnostic()
                            .warning(
                                    "Failed to finalize output stream for "
                                            + "file "
                                            + filenameOnly(download.getFilename())
                                            + " from "
                                            + download.getUsername() + ": "
                                            + Failures.message(failure),
                                    failure);
                }
            }
        } finally {
            if (globalPermit.compareAndSet(true, false)) {
                try {
                    engine.globalDownloadSemaphore.release();
                } catch (Throwable failure) {
                    engine.context
                            .getDiagnostic()
                            .warning(
                                    "Failed to release global download "
                                            + "semaphore for file "
                                            + filenameOnly(download.getFilename())
                                            + " from "
                                            + download.getUsername() + ": "
                                            + Failures.message(failure),
                                    failure);
                }
            }
            engine.context.getDownloadRegistry().remove(download.getToken(), download);
            engine.uniqueKeys.remove(uniqueKey);
        }
    }

    private void unbindConnectionEvents() {
        if (connection == null) {
            return;
        }
        if (dataReadListener != null) {
            connection.removeDataReadListener(dataReadListener);
        }
        if (disconnectedListener != null) {
            connection.removeDisconnectedListener(disconnectedListener);
        }
    }

    private void updateState(TransferState state) {
        download.setState(state);
        Transfer transfer = download.toTransfer();
        TransferStateChangedEvent eventData = new TransferStateChangedEvent(lastState, transfer);
        TransferState previous = lastState;
        lastState = state;
        if (transferOptions.getStateChanged() != null) {
            transferOptions.getStateChanged().onStateChanged(new TransferStateChange(previous, transfer));
        }
        engine.context.raiseEvent(Kind.TRANSFER_STATE_CHANGED, eventData);
    }

    private void updateProgress(long bytesDownloaded) {
        long previous = download.getBytesTransferred();
        download.updateProgress(bytesDownloaded);
        Transfer transfer = download.toTransfer();
        if (transferOptions.getProgressUpdated() != null) {
            transferOptions.getProgressUpdated().onProgressUpdated(new TransferProgressUpdate(previous, transfer));
        }
        engine.context.raiseEvent(Kind.TRANSFER_PROGRESS_UPDATED, new TransferProgressUpdatedEvent(previous, transfer));
    }

    private long currentOutputPosition() {
        if (trackingStream != null) {
            return trackingStream.getPosition();
        }
        if (outputStream != null) {
            try {
                return determineOutputPosition(outputStream, 0);
            } catch (Throwable ignored) {
                return 0;
            }
        }
        return 0;
    }

    private long determineFinalOutputPosition() {
        if (outputStream == null) {
            return 0;
        }
        try {
            return determineOutputPosition(outputStream, trackingStream == null ? 0 : trackingStream.getPosition());
        } catch (Throwable failure) {
            engine.context
                    .getDiagnostic()
                    .warning(
                            "Failed to determine final position of output "
                                    + "stream for file "
                                    + filenameOnly(download.getFilename())
                                    + " from " + download.getUsername() + ": "
                                    + Failures.message(failure),
                            failure);
            return 0;
        }
    }
}
