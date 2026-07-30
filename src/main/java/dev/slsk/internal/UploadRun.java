// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static dev.slsk.internal.transfer.TransferStreams.determinePosition;
import static dev.slsk.internal.transfer.TransferStreams.filenameOnly;
import static dev.slsk.internal.transfer.TransferStreams.seekInputStream;

import dev.slsk.CancellationSignal;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.ConnectionReadException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.exceptions.TransferException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.exceptions.TransferStreamException;
import dev.slsk.internal.common.CommonUtils;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.NetworkExecutor;
import dev.slsk.internal.common.Permits;
import dev.slsk.internal.common.Settlement;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
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
import dev.slsk.internal.transfer.TransferStreams;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongFunction;

/**
 * One upload, start to finish, on the thread that asked for it.
 *
 * <p>The counterpart to {@link DownloadRun}; see there.
 */
final class UploadRun {

    /** The domain that decided this run may happen, and owns what it needs. */
    private final TransferDomain domain;

    private final TransferInternal upload;
    private final LongFunction<InputStream> inputStreamFactory;
    private final TransferOptions transferOptions;
    private final CancellationSignal cancellationSignal;
    private final String uniqueKey;
    private final AtomicBoolean perUserPermit = new AtomicBoolean();
    private final AtomicBoolean slot = new AtomicBoolean();
    private final AtomicBoolean globalPermit = new AtomicBoolean();
    private TransferState lastState = TransferState.NONE;
    private Semaphore perUserSemaphore;
    private InetSocketAddress endpoint;
    private Connection connection;
    private InputStream inputStream;
    private TransferStreams.PositionTrackingInputStream trackingStream;
    private ConnectionEventListener<ConnectionDataEvent> dataWrittenListener;
    private ConnectionEventListener<ConnectionDisconnectedEvent> disconnectedListener;

    UploadRun(
            TransferDomain domain,
            TransferInternal upload,
            LongFunction<InputStream> inputStreamFactory,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal,
            String uniqueKey) {
        this.domain = domain;
        this.upload = upload;
        this.inputStreamFactory = inputStreamFactory;
        this.transferOptions = transferOptions;
        this.cancellationSignal = cancellationSignal;
        this.uniqueKey = uniqueKey;
    }

    /**
     * Runs the upload to a terminal state and reports what it reached.
     *
     * <p>Returns rather than throws; see {@link DownloadRun#execute()}.
     *
     * @return the transfer in its terminal state
     */
    Transfer execute() {
        try {
            perUserSemaphore = domain.uploadSemaphoreFor(upload.getUsername(), cancellationSignal);

            // Announced before the wait rather than during it. The wait used to
            // be started inside the sync root and awaited after, which is the
            // same two events in the same order; it just needed a future to
            // carry the not-yet-finished acquisition across the announcement.
            updateState(TransferState.QUEUED.or(TransferState.LOCALLY));

            Permits.acquire(perUserSemaphore, cancellationSignal);
            perUserPermit.set(true);
            domain.diagnostic.debug("Upload semaphore for file "
                    + filenameOnly(upload.getFilename()) + " to "
                    + upload.getUsername() + " acquired");

            // The slot gate is the upload policy: serveUpload is only reached
            // on an Allow, and Allow is only returned when a slot is free. A
            // second, pluggable gate in front of it was two places to express
            // one rule; the try/catch that guarded the pluggable one went with
            // it, because setting a flag cannot fail.
            slot.set(true);
            domain.diagnostic.debug("Upload slot for file "
                    + filenameOnly(upload.getFilename()) + " to "
                    + upload.getUsername() + " acquired");

            Permits.acquire(domain.globalUploadSemaphore(), cancellationSignal);
            globalPermit.set(true);
            domain.diagnostic.debug("Global upload semaphore for file "
                    + filenameOnly(upload.getFilename()) + " to "
                    + upload.getUsername() + " acquired");

            endpoint = domain.endpoint(upload.getUsername(), cancellationSignal);
            MessageConnection messageConnection =
                    domain.peers().getOrAddMessageConnection(upload.getUsername(), endpoint, cancellationSignal);
            domain.diagnostic.debug("Fetched peer connection for upload of "
                    + filenameOnly(upload.getFilename()) + " to "
                    + upload.getUsername() + " (id: " + messageConnection.getId()
                    + ", state: " + messageConnection.getState() + ")");

            Wait<TransferResponse> transferRequestAcknowledged = domain.waiter.register(
                    new WaitKey(MessageCode.Peer.TRANSFER_RESPONSE, upload.getUsername(), upload.getToken()),
                    TransferResponse.class,
                    domain.clientOptions().getPeerConnectionOptions().getInactivityTimeout(),
                    cancellationSignal);
            messageConnection.write(
                    new TransferRequest(
                            TransferDirection.UPLOAD, upload.getToken(), upload.getFilename(), upload.getSize()),
                    CommonUtils.token(cancellationSignal));
            domain.diagnostic.debug("Wrote transfer request for upload of "
                    + filenameOnly(upload.getFilename()) + " to "
                    + upload.getUsername() + " (id: " + messageConnection.getId()
                    + ", state: " + messageConnection.getState() + ")");
            updateState(TransferState.REQUESTED);

            TransferResponse acknowledgement = transferRequestAcknowledged.await();
            domain.diagnostic.debug("Received transfer request ACK for upload of "
                    + filenameOnly(upload.getFilename()) + " to "
                    + upload.getUsername() + ": allowed: " + acknowledgement.isAllowed()
                    + ", message: " + acknowledgement.getMessage()
                    + " (token: " + upload.getToken() + ")");
            if (!acknowledgement.isAllowed()) {
                throw new TransferRejectedException("Transfer rejected: " + acknowledgement.getMessage());
            }

            updateState(TransferState.INITIALIZING);
            connection = domain.peers()
                    .getTransferConnection(upload.getUsername(), endpoint, upload.getToken(), cancellationSignal);
            domain.diagnostic.debug("Fetched transfer connection for upload of "
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

            domain.diagnostic.debug("Resolving input stream for upload of " + filenameOnly(upload.getFilename())
                    + " to " + upload.getUsername());
            inputStream = Objects.requireNonNull(
                    inputStreamFactory.apply(upload.getStartOffset()), "inputStreamFactory result");
            positionInputStream();
            trackingStream = new TransferStreams.PositionTrackingInputStream(
                    inputStream, determinePosition(inputStream, upload.getStartOffset()));

            updateState(TransferState.IN_PROGRESS);
            updateProgress(upload.getStartOffset());
            writeAndAwaitDisconnectRace();
            linger();

            updateProgress(currentStreamPosition());
            updateState(TransferState.COMPLETED.or(TransferState.SUCCEEDED));
        } catch (Throwable failure) {
            handleFailure(Failures.unwrap(failure));
        } finally {
            cleanup();
        }
        return upload.toTransfer();
    }

    private void bindConnectionEvents() {
        dataWrittenListener =
                (sender, eventData) -> updateProgress(upload.getStartOffset() + eventData.getCurrentLength());
        disconnectedListener = (sender, eventData) -> {
            Throwable failure = eventData.getException();
            if (failure instanceof CancellationException || failure instanceof TimeoutException) {
                upload.settlement().fail(failure);
            } else {
                upload.settlement()
                        .fail(new ConnectionException("Transfer failed: " + eventData.getMessage(), failure));
            }
        };
        connection.addDataWrittenListener(dataWrittenListener);
        connection.addDisconnectedListener(disconnectedListener);
    }

    private void readStartOffset() {
        try {
            byte[] bytes = connection.read(8, cancellationSignal);
            if (bytes.length != 8) {
                throw new IOException("Expected 8 bytes but received " + bytes.length);
            }
            upload.setStartOffset(
                    ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong());
        } catch (Throwable failure) {
            Throwable cause = Failures.unwrap(failure);
            domain.diagnostic.debug("Failed to read start offset for upload of "
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
        domain.diagnostic.debug("Seeking input stream for upload of "
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
        Settlement settlement = upload.settlement();
        if (remaining == 0) {
            // Nothing to send, so nothing to race: the peer is re-requesting a
            // file it already has all of.
            settlement.succeed();
        } else {
            // A thread of its own because the write is raced against the
            // connection dropping; racing is work blocking code cannot do. Both
            // settle the one cell, so the first to arrive is the answer.
            NetworkExecutor.executor().execute(() -> {
                try {
                    connection.write(
                            remaining,
                            trackingStream,
                            (requestedBytes, governorToken) ->
                                    domain.uploadTokenBucket.get(requestedBytes, cancellationSignal),
                            (attemptedBytes, grantedBytes, transferredBytes) -> {
                                if (transferOptions.getReporter() != null) {
                                    transferOptions
                                            .getReporter()
                                            .report(
                                                    upload.toTransfer(),
                                                    attemptedBytes,
                                                    grantedBytes,
                                                    transferredBytes);
                                }
                                domain.uploadTokenBucket.returnTokens(grantedBytes - transferredBytes);
                            },
                            cancellationSignal);
                    settlement.succeed();
                } catch (Throwable failure) {
                    settlement.fail(failure);
                }
            });
        }

        Throwable failure = settlement.await();
        if (failure != null) {
            throw Failures.propagate(failure);
        }
    }

    /**
     * Waits for the receiving end to hang up, and hangs up first if it will not.
     *
     * <p>Ideally the receiver disconnects, because that is how we know it got
     * everything. Reading encourages it to — Soulseek NS and Qt oblige promptly,
     * Nicotine+ takes its time — so this reads until the peer goes away or the
     * linger budget lapses.
     */
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
                // On a thread of its own so the budget can lapse while the read
                // is still parked: a peer that says nothing is the ordinary case
                // here, and giving up on it is the point.
                Settlement read = new Settlement();
                NetworkExecutor.executor().execute(() -> {
                    try {
                        connection.read(1, cancellationSignal);
                        read.succeed();
                    } catch (Throwable failure) {
                        read.fail(failure);
                    }
                });
                if (!read.await(remainingMillis)) {
                    connection.disconnect("Transfer complete, maximum " + "linger time exceeded");
                    return;
                }
                Throwable failure = read.failure();
                if (failure != null) {
                    throw Failures.propagate(failure);
                }
                Thread.sleep(100);
            }
            cancellationSignal.throwIfCancellationRequested();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
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
            if (perUserSemaphore != null) {
                domain.releaseUploadSemaphoreLease(upload.getUsername());
            }
            domain.uploads().remove(upload.getToken(), upload);
            domain.releaseUniqueKey(uniqueKey);
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
            InetSocketAddress currentEndpoint = domain.endpoint(upload.getUsername(), CancellationSignal.none());
            MessageConnection messageConnection = domain.peers()
                    .getOrAddMessageConnection(upload.getUsername(), currentEndpoint, CancellationSignal.none());
            OutgoingMessage message = upload.getState().contains(TransferState.CANCELLED)
                    ? new UploadDenied(upload.getFilename(), "Cancelled")
                    : new UploadFailed(upload.getFilename());
            messageConnection.write(message, CancellationSignal.none());
        } catch (Throwable ignored) {
            // Failure notification is intentionally best effort.
        }
    }

    private void releasePermits() {
        if (perUserPermit.compareAndSet(true, false)) {
            perUserSemaphore.release();
            domain.diagnostic.debug("Upload semaphore for file "
                    + filenameOnly(upload.getFilename()) + " to "
                    + upload.getUsername() + " released");
        }
        if (slot.compareAndSet(true, false)) {
            domain.diagnostic.debug("Upload slot for file "
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
            domain.globalUploadSemaphore().release();
            domain.diagnostic.debug("Global upload semaphore for file "
                    + filenameOnly(upload.getFilename()) + " to "
                    + upload.getUsername() + " released");
        }
    }

    private void updateState(TransferState state) {
        upload.setState(state);
        Transfer transfer = upload.toTransfer();
        TransferState previous = lastState;
        lastState = state;
        if (transferOptions.getStateChanged() != null) {
            transferOptions.getStateChanged().onStateChanged(new TransferStateChange(previous, transfer));
        }
    }

    private void updateProgress(long bytesUploaded) {
        long previous = upload.getBytesTransferred();
        upload.updateProgress(bytesUploaded);
        Transfer transfer = upload.toTransfer();
        if (transferOptions.getProgressUpdated() != null) {
            transferOptions.getProgressUpdated().onProgressUpdated(new TransferProgressUpdate(previous, transfer));
        }
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
