// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static dev.slsk.internal.TransferEngine.await;
import static dev.slsk.internal.TransferEngine.determinePosition;
import static dev.slsk.internal.TransferEngine.filenameOnly;
import static dev.slsk.internal.TransferEngine.seekInputStream;

import dev.slsk.CancellationSignal;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.ConnectionReadException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.exceptions.TransferException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.exceptions.TransferStreamException;
import dev.slsk.exceptions.UserOfflineException;
import dev.slsk.internal.EngineEvents.Kind;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.Permits;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.events.TransferProgressUpdatedEvent;
import dev.slsk.internal.events.TransferStateChangedEvent;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.messages.OutgoingMessage;
import dev.slsk.internal.messaging.messages.TransferRequest;
import dev.slsk.internal.messaging.messages.TransferResponse;
import dev.slsk.internal.messaging.messages.UploadDenied;
import dev.slsk.internal.messaging.messages.UploadFailed;
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
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongFunction;

/**
 * One upload in flight: slot acquisition, the peer handshake, the write loop
 * and the terminal state transitions.
 *
 * <p>The counterpart to {@link DownloadOperation}; see there for why both are
 * top-level.
 */
final class UploadOperation {

    /** The engine that owns the concurrency limits and duplicate keys. */
    private final TransferEngine engine;

    private final TransferInternal upload;
    private final LongFunction<InputStream> inputStreamFactory;
    private final TransferOptions transferOptions;
    private final CancellationSignal cancellationSignal;
    private final String uniqueKey;
    private final AtomicBoolean perUserPermit = new AtomicBoolean();
    private final AtomicBoolean slot = new AtomicBoolean();
    private final AtomicBoolean globalPermit = new AtomicBoolean();
    private final CompletableFuture<Void> disconnected = new CompletableFuture<>();
    private TransferState lastState = TransferState.NONE;
    private Semaphore perUserSemaphore;
    private InetSocketAddress endpoint;
    private Connection connection;
    private InputStream inputStream;
    private TransferEngine.PositionTrackingInputStream trackingStream;
    private ConnectionEventListener<ConnectionDataEvent> dataWrittenListener;
    private ConnectionEventListener<ConnectionDisconnectedEvent> disconnectedListener;

    UploadOperation(
            TransferEngine engine,
            TransferInternal upload,
            LongFunction<InputStream> inputStreamFactory,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal,
            String uniqueKey) {
        this.engine = engine;
        this.upload = upload;
        this.inputStreamFactory = inputStreamFactory;
        this.transferOptions = transferOptions;
        this.cancellationSignal = cancellationSignal;
        this.uniqueKey = uniqueKey;
    }

    Transfer execute() {
        try {
            Permits.acquire(engine.uploadSemaphoreSyncRoot, cancellationSignal);
            try {
                perUserSemaphore = engine.uploadSemaphores.computeIfAbsent(
                        upload.getUsername(),
                        ignored ->
                                new Semaphore(engine.context.getClientOptions().getMaximumConcurrentUploadsPerUser()));
            } finally {
                engine.uploadSemaphoreSyncRoot.release();
            }

            // Announced before the wait rather than during it. The wait used to
            // be started inside the sync root and awaited after, which is the
            // same two events in the same order; it just needed a future to
            // carry the not-yet-finished acquisition across the announcement.
            updateState(TransferState.QUEUED.or(TransferState.LOCALLY));

            Permits.acquire(perUserSemaphore, cancellationSignal);
            perUserPermit.set(true);
            engine.context
                    .getDiagnostic()
                    .debug("Upload semaphore for file "
                            + filenameOnly(upload.getFilename()) + " to "
                            + upload.getUsername() + " acquired");

            try {
                // The slot gate is the upload policy: serveUpload is only
                // reached on an Allow, and Allow is only returned when a slot is
                // free. A second, pluggable gate in front of it was two places
                // to express one rule.
                slot.set(true);
                engine.context
                        .getDiagnostic()
                        .debug("Upload slot for file "
                                + filenameOnly(upload.getFilename()) + " to "
                                + upload.getUsername() + " acquired");
            } catch (Throwable failure) {
                Throwable cause = Failures.unwrap(failure);
                if (cause instanceof CancellationException) {
                    throw cause;
                }
                throw new TransferException(
                        "Failed to acquire an upload slot for file "
                                + filenameOnly(upload.getFilename())
                                + " to " + upload.getUsername() + ": "
                                + Failures.message(cause),
                        cause);
            }

            Permits.acquire(engine.globalUploadSemaphore, cancellationSignal);
            globalPermit.set(true);
            engine.context
                    .getDiagnostic()
                    .debug("Global upload semaphore for file "
                            + filenameOnly(upload.getFilename()) + " to "
                            + upload.getUsername() + " acquired");

            endpoint = await(engine.context.resolveUserEndpoint(upload.getUsername(), cancellationSignal));
            MessageConnection messageConnection = await(engine.context
                    .getPeerConnectionManager()
                    .getOrAddMessageConnectionAsync(upload.getUsername(), endpoint, cancellationSignal));
            engine.context
                    .getDiagnostic()
                    .debug("Fetched peer connection for upload of "
                            + filenameOnly(upload.getFilename()) + " to "
                            + upload.getUsername() + " (id: " + messageConnection.getId()
                            + ", state: " + messageConnection.getState() + ")");

            CompletableFuture<TransferResponse> transferRequestAcknowledged = engine.context
                    .getWaiter()
                    .waitAsync(
                            new WaitKey(MessageCode.Peer.TRANSFER_RESPONSE, upload.getUsername(), upload.getToken()),
                            TransferResponse.class,
                            engine.context
                                    .getClientOptions()
                                    .getPeerConnectionOptions()
                                    .getInactivityTimeout(),
                            cancellationSignal);
            await(engine.context.writeToPeer(
                    messageConnection,
                    new TransferRequest(
                            TransferDirection.UPLOAD, upload.getToken(), upload.getFilename(), upload.getSize()),
                    cancellationSignal));
            engine.context
                    .getDiagnostic()
                    .debug("Wrote transfer request for upload of "
                            + filenameOnly(upload.getFilename()) + " to "
                            + upload.getUsername() + " (id: " + messageConnection.getId()
                            + ", state: " + messageConnection.getState() + ")");
            updateState(TransferState.REQUESTED);

            TransferResponse acknowledgement = await(transferRequestAcknowledged);
            engine.context
                    .getDiagnostic()
                    .debug("Received transfer request ACK for upload of "
                            + filenameOnly(upload.getFilename()) + " to "
                            + upload.getUsername() + ": allowed: " + acknowledgement.isAllowed()
                            + ", message: " + acknowledgement.getMessage()
                            + " (token: " + upload.getToken() + ")");
            if (!acknowledgement.isAllowed()) {
                throw new TransferRejectedException("Transfer rejected: " + acknowledgement.getMessage());
            }

            updateState(TransferState.INITIALIZING);
            connection = await(engine.context
                    .getPeerConnectionManager()
                    .getTransferConnectionAsync(upload.getUsername(), endpoint, upload.getToken(), cancellationSignal));
            engine.context
                    .getDiagnostic()
                    .debug("Fetched transfer connection for upload of "
                            + filenameOnly(upload.getFilename()) + " to "
                            + upload.getUsername() + " (id: " + connection.getId()
                            + ", state: " + connection.getState() + ")");
            upload.setConnection(connection);
            bindConnectionEvents();

            readStartOffset();
            if (upload.getStartOffset() > upload.getSize()) {
                throw new TransferException("Requested start offset of "
                        + upload.getStartOffset()
                        + " bytes exceeds file length of "
                        + upload.getSize() + " bytes");
            }

            engine.context
                    .getDiagnostic()
                    .debug("Resolving input stream for upload of " + filenameOnly(upload.getFilename()) + " to "
                            + upload.getUsername());
            inputStream = Objects.requireNonNull(
                    inputStreamFactory.apply(upload.getStartOffset()), "inputStreamFactory result");
            positionInputStream();
            trackingStream = new TransferEngine.PositionTrackingInputStream(
                    inputStream, determinePosition(inputStream, upload.getStartOffset()));

            updateState(TransferState.IN_PROGRESS);
            updateProgress(upload.getStartOffset());
            writeAndAwaitDisconnectRace();
            linger();

            updateProgress(currentStreamPosition());
            updateState(TransferState.COMPLETED.or(TransferState.SUCCEEDED));
            return upload.toTransfer();
        } catch (Throwable failure) {
            Throwable cause = Failures.unwrap(failure);
            handleFailure(cause);
            throw new CompletionException(mapUploadFailure(cause));
        } finally {
            cleanup();
        }
    }

    private void bindConnectionEvents() {
        dataWrittenListener =
                (sender, eventData) -> updateProgress(upload.getStartOffset() + eventData.getCurrentLength());
        disconnectedListener = (sender, eventData) -> {
            Throwable failure = eventData.getException();
            if (failure instanceof CancellationException || failure instanceof TimeoutException) {
                disconnected.completeExceptionally(failure);
            } else {
                disconnected.completeExceptionally(
                        new ConnectionException("Transfer failed: " + eventData.getMessage(), failure));
            }
        };
        connection.addDataWrittenListener(dataWrittenListener);
        connection.addDisconnectedListener(disconnectedListener);
    }

    private void readStartOffset() {
        try {
            byte[] bytes = await(connection.readAsync(8, cancellationSignal));
            if (bytes.length != 8) {
                throw new IOException("Expected 8 bytes but received " + bytes.length);
            }
            upload.setStartOffset(
                    ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong());
        } catch (Throwable failure) {
            Throwable cause = Failures.unwrap(failure);
            engine.context
                    .getDiagnostic()
                    .debug("Failed to read start offset for upload of "
                            + filenameOnly(upload.getFilename()) + " to "
                            + upload.getUsername() + ": " + Failures.message(cause));
            if (cause instanceof CancellationException || cause instanceof TimeoutException) {
                throw new CompletionException(cause);
            }
            throw new MessageReadException("Failed to read transfer start offset: " + Failures.message(cause), cause);
        }
    }

    private void positionInputStream() {
        if (upload.getStartOffset() <= 0 || !transferOptions.isSeekInputStreamAutomatically()) {
            return;
        }
        engine.context
                .getDiagnostic()
                .debug("Seeking input stream for upload of "
                        + filenameOnly(upload.getFilename()) + " to "
                        + upload.getUsername() + " to starting offset of "
                        + upload.getStartOffset() + " bytes");
        try {
            seekInputStream(inputStream, upload.getStartOffset());
        } catch (IOException failure) {
            throw new TransferStreamException(
                    "Requested non-zero start offset but input " + "stream does not support seeking", failure);
        }
    }

    private void writeAndAwaitDisconnectRace() {
        long remaining = upload.getSize() - upload.getStartOffset();
        CompletableFuture<Void> write = remaining == 0
                ? CompletableFuture.completedFuture(null)
                : connection.writeAsync(
                        remaining,
                        trackingStream,
                        (requestedBytes, governorToken) ->
                                engine.context.getUploadTokenBucket().getAsync(requestedBytes, cancellationSignal),
                        (attemptedBytes, grantedBytes, transferredBytes) -> {
                            if (transferOptions.getReporter() != null) {
                                transferOptions
                                        .getReporter()
                                        .report(upload.toTransfer(), attemptedBytes, grantedBytes, transferredBytes);
                            }
                            engine.context.getUploadTokenBucket().returnTokens(grantedBytes - transferredBytes);
                        },
                        cancellationSignal);
        CompletableFuture<Object> first = CompletableFuture.anyOf(write, disconnected);
        await(first);
        if (disconnected.isCompletedExceptionally() && !write.isDone()) {
            await(disconnected);
        }
        await(write);
    }

    private void linger() {
        long deadline =
                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(0, transferOptions.getMaximumLingerTime()));
        try {
            while (!cancellationSignal.isCancellationRequested()) {
                long remainingNanos = deadline - System.nanoTime();
                if (remainingNanos <= 0) {
                    connection.disconnect("Transfer complete, maximum linger " + "time exceeded");
                    return;
                }
                long remainingMillis = Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
                try {
                    await(connection
                            .readAsync(1, cancellationSignal)
                            .orTimeout(remainingMillis, TimeUnit.MILLISECONDS));
                } catch (Throwable failure) {
                    Throwable cause = Failures.unwrap(failure);
                    if (cause instanceof TimeoutException) {
                        connection.disconnect("Transfer complete, maximum " + "linger time exceeded");
                        return;
                    }
                    throw failure;
                }
                await(CompletableFuture.runAsync(
                        () -> {}, CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS)));
            }
            cancellationSignal.throwIfCancellationRequested();
        } catch (Throwable failure) {
            if (!(Failures.unwrap(failure) instanceof ConnectionReadException)) {
                throw failure;
            }
        }
    }

    private void handleFailure(Throwable failure) {
        if (failure instanceof TransferRejectedException) {
            upload.setException(failure);
            updateState(TransferState.COMPLETED.or(TransferState.REJECTED));
            return;
        }
        if (failure instanceof CancellationException) {
            disconnectTransfer("Transfer cancelled", failure);
            upload.setException(failure);
            updateProgress(currentStreamPosition());
            updateState(TransferState.COMPLETED.or(TransferState.CANCELLED));
            return;
        }
        if (failure instanceof TimeoutException) {
            disconnectTransfer("Transfer timed out", failure);
            upload.setException(failure);
            updateProgress(currentStreamPosition());
            updateState(TransferState.COMPLETED.or(TransferState.TIMED_OUT));
            return;
        }
        disconnectTransfer("Transfer error", failure);
        upload.setException(failure);
        updateProgress(currentStreamPosition());
        updateState(TransferState.COMPLETED.or(TransferState.ERRORED));
    }

    private Throwable mapUploadFailure(Throwable failure) {
        if (failure instanceof TransferRejectedException
                || failure instanceof CancellationException
                || failure instanceof TimeoutException
                || failure instanceof UserOfflineException) {
            return failure;
        }
        return new SoulseekClientException(
                "Failed to upload file " + upload.getFilename()
                        + " to user " + upload.getUsername() + ": "
                        + Failures.message(failure),
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
            unbindConnectionEvents();
            if (connection != null) {
                try {
                    connection.close();
                } catch (Throwable ignored) {
                    // Best-effort connection cleanup.
                }
            }
            currentStreamPosition();
            if (transferOptions.isDisposeInputStreamOnCompletion() && inputStream != null) {
                try {
                    inputStream.close();
                } catch (Throwable ignored) {
                    // Best-effort stream cleanup.
                }
            }
            if (!upload.getState().contains(TransferState.SUCCEEDED)) {
                notifyUploadFailure();
            }
        } finally {
            releasePermits();
            engine.context.getUploadRegistry().remove(upload.getToken(), upload);
            engine.uniqueKeys.remove(uniqueKey);
        }
    }

    private void unbindConnectionEvents() {
        if (connection == null) {
            return;
        }
        if (dataWrittenListener != null) {
            connection.removeDataWrittenListener(dataWrittenListener);
        }
        if (disconnectedListener != null) {
            connection.removeDisconnectedListener(disconnectedListener);
        }
    }

    private void notifyUploadFailure() {
        try {
            InetSocketAddress currentEndpoint =
                    await(engine.context.resolveUserEndpoint(upload.getUsername(), CancellationSignal.none()));
            MessageConnection messageConnection = await(engine.context
                    .getPeerConnectionManager()
                    .getOrAddMessageConnectionAsync(upload.getUsername(), currentEndpoint, CancellationSignal.none()));
            OutgoingMessage message = upload.getState().contains(TransferState.CANCELLED)
                    ? new UploadDenied(upload.getFilename(), "Cancelled")
                    : new UploadFailed(upload.getFilename());
            await(engine.context.writeToPeer(messageConnection, message, CancellationSignal.none()));
        } catch (Throwable ignored) {
            // Failure notification is intentionally best effort.
        }
    }

    private void releasePermits() {
        if (perUserPermit.compareAndSet(true, false)) {
            perUserSemaphore.release();
            engine.context
                    .getDiagnostic()
                    .debug("Upload semaphore for file "
                            + filenameOnly(upload.getFilename()) + " to "
                            + upload.getUsername() + " released");
        }
        if (slot.compareAndSet(true, false)) {
            engine.context
                    .getDiagnostic()
                    .debug("Upload slot for file "
                            + filenameOnly(upload.getFilename()) + " to "
                            + upload.getUsername() + " released");
            if (transferOptions.getSlotReleased() != null) {
                try {
                    Thread.sleep(10);
                    transferOptions.getSlotReleased().onSlotReleased(upload.toTransfer());
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                } catch (Throwable ignored) {
                    // Slot-release callbacks cannot block cleanup.
                }
            }
        }
        if (globalPermit.compareAndSet(true, false)) {
            engine.globalUploadSemaphore.release();
            engine.context
                    .getDiagnostic()
                    .debug("Global upload semaphore for file "
                            + filenameOnly(upload.getFilename()) + " to "
                            + upload.getUsername() + " released");
        }
    }

    private void updateState(TransferState state) {
        upload.setState(state);
        Transfer transfer = upload.toTransfer();
        TransferStateChangedEvent eventData = new TransferStateChangedEvent(lastState, transfer);
        TransferState previous = lastState;
        lastState = state;
        if (transferOptions.getStateChanged() != null) {
            transferOptions.getStateChanged().onStateChanged(new TransferStateChange(previous, transfer));
        }
        engine.context.raiseEvent(Kind.TRANSFER_STATE_CHANGED, eventData);
    }

    private void updateProgress(long bytesUploaded) {
        long previous = upload.getBytesTransferred();
        upload.updateProgress(bytesUploaded);
        Transfer transfer = upload.toTransfer();
        if (transferOptions.getProgressUpdated() != null) {
            transferOptions.getProgressUpdated().onProgressUpdated(new TransferProgressUpdate(previous, transfer));
        }
        engine.context.raiseEvent(Kind.TRANSFER_PROGRESS_UPDATED, new TransferProgressUpdatedEvent(previous, transfer));
    }

    private long currentStreamPosition() {
        if (trackingStream != null) {
            return trackingStream.getPosition();
        }
        if (inputStream != null) {
            try {
                return determinePosition(inputStream, 0);
            } catch (Throwable ignored) {
                return 0;
            }
        }
        return 0;
    }
}
