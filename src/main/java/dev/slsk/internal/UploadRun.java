// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static dev.slsk.internal.transfer.TransferChannels.filenameOnly;

import dev.slsk.Subscription;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.ConnectionReadException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.exceptions.TransferException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.internal.common.CancellationSignals;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.Permits;
import dev.slsk.internal.common.Settlement;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.messages.OutgoingMessage;
import dev.slsk.internal.messaging.messages.TransferRequest;
import dev.slsk.internal.messaging.messages.TransferResponse;
import dev.slsk.internal.messaging.messages.UploadDenied;
import dev.slsk.internal.messaging.messages.UploadFailed;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.tcp.ConnectionDataEvent;
import dev.slsk.internal.network.tcp.ConnectionDisconnectedEvent;
import dev.slsk.internal.network.tcp.TransportConnection;
import dev.slsk.internal.options.TransferOptions;
import dev.slsk.internal.options.TransferProgressUpdate;
import dev.slsk.internal.options.TransferStateChange;
import dev.slsk.internal.transfer.Transfer;
import dev.slsk.internal.transfer.TransferChannels;
import dev.slsk.internal.transfer.TransferDirection;
import dev.slsk.internal.transfer.TransferInternal;
import dev.slsk.internal.transfer.TransferPhase;
import dev.slsk.internal.transfer.TransferQueueLocation;
import dev.slsk.internal.transfer.TransferTermination;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * One upload, start to finish, on the thread that asked for it.
 *
 * <p>The counterpart to {@link DownloadRun}; see there.
 */
final class UploadRun {

    /** The domain that decided this run may happen, and owns what it needs. */
    private final TransferDomain domain;

    private final TransferInternal upload;
    private final TransferChannels.SourceFactory sourceFactory;
    private final TransferOptions transferOptions;
    private final CancellationSignal cancellationSignal;
    private final String uniqueKey;
    private boolean perUserPermit;
    private boolean slot;
    private boolean globalPermit;
    private TransferPhase lastPhase = TransferPhase.NONE;
    private Semaphore perUserSemaphore;
    private InetSocketAddress endpoint;
    private TransportConnection connection;
    private TransferChannels.TrackingReadableChannel source;
    private Consumer<ConnectionDataEvent> dataWrittenListener;
    private Consumer<ConnectionDisconnectedEvent> disconnectedListener;
    private Subscription dataWrittenSubscription;
    private Subscription disconnectedSubscription;

    UploadRun(
            TransferDomain domain,
            TransferInternal upload,
            TransferChannels.SourceFactory sourceFactory,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal,
            String uniqueKey) {
        this.domain = domain;
        this.upload = upload;
        this.sourceFactory = sourceFactory;
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
            perUserSemaphore = domain.uploadSemaphoreFor(upload.getUsername());

            // Announced before the wait rather than during it. The wait used to
            // be started inside the sync root and awaited after, which is the
            // same two events in the same order; it just needed a future to
            // carry the not-yet-finished acquisition across the announcement.
            updateQueued(TransferQueueLocation.LOCAL);

            Permits.acquire(perUserSemaphore, cancellationSignal);
            perUserPermit = true;
            domain.log.debug("Upload semaphore for file "
                    + filenameOnly(upload.getFilename()) + " to "
                    + upload.getUsername() + " acquired");

            // The slot gate is the upload policy: serveUpload is only reached
            // on an Allow, and Allow is only returned when a slot is free. A
            // second, pluggable gate in front of it was two places to express
            // one rule; the try/catch that guarded the pluggable one went with
            // it, because setting a flag cannot fail.
            slot = true;
            domain.log.debug("Upload slot for file "
                    + filenameOnly(upload.getFilename()) + " to "
                    + upload.getUsername() + " acquired");

            Permits.acquire(domain.globalUploadSemaphore(), cancellationSignal);
            globalPermit = true;
            domain.log.debug("Global upload semaphore for file "
                    + filenameOnly(upload.getFilename()) + " to "
                    + upload.getUsername() + " acquired");

            endpoint = domain.endpoint(upload.getUsername(), cancellationSignal);
            MessageConnection messageConnection =
                    domain.peers().getOrAddMessageConnection(upload.getUsername(), endpoint, cancellationSignal);
            domain.log.debug("Fetched peer connection for upload of "
                    + filenameOnly(upload.getFilename()) + " to "
                    + upload.getUsername() + " (id: " + messageConnection.getId()
                    + ", state: " + messageConnection.getState() + ")");

            Wait<TransferResponse> transferRequestAcknowledged = domain.waiter.register(
                    new WaitKey.PeerToken(MessageCode.Peer.TRANSFER_RESPONSE, upload.getUsername(), upload.getToken()),
                    TransferResponse.class,
                    domain.clientOptions().peerConnectionOptions().inactivityTimeout(),
                    cancellationSignal);
            messageConnection.write(
                    new TransferRequest(
                            TransferDirection.UPLOAD, upload.getToken(), upload.getFilename(), upload.getSize()),
                    CancellationSignals.orNone(cancellationSignal));
            domain.log.debug("Wrote transfer request for upload of "
                    + filenameOnly(upload.getFilename()) + " to "
                    + upload.getUsername() + " (id: " + messageConnection.getId()
                    + ", state: " + messageConnection.getState() + ")");
            updateState(TransferPhase.REQUESTED);

            TransferResponse acknowledgement = transferRequestAcknowledged.await();
            domain.log.debug("Received transfer request ACK for upload of "
                    + filenameOnly(upload.getFilename()) + " to "
                    + upload.getUsername() + ": allowed: " + acknowledgement.isAllowed()
                    + ", message: " + acknowledgement.getMessage()
                    + " (token: " + upload.getToken() + ")");
            if (!acknowledgement.isAllowed()) {
                throw new TransferRejectedException("Transfer rejected: " + acknowledgement.getMessage());
            }

            updateState(TransferPhase.INITIALIZING);
            connection = domain.peers()
                    .getTransferConnection(upload.getUsername(), endpoint, upload.getToken(), cancellationSignal);
            domain.log.debug("Fetched transfer connection for upload of "
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

            domain.log.debug("Resolving input channel for upload of " + filenameOnly(upload.getFilename())
                    + " to " + upload.getUsername());
            source = Objects.requireNonNull(
                    sourceFactory.open(upload.getStartOffset(), transferOptions.seekInputStreamAutomatically()),
                    "sourceFactory result");

            updateState(TransferPhase.IN_PROGRESS);
            updateProgress(upload.getStartOffset());
            writeAndAwaitDisconnectRace();
            linger();

            updateProgress(currentStreamPosition());
            complete(TransferTermination.SUCCEEDED);
        } catch (Throwable failure) {
            handleFailure(failure);
        } finally {
            cleanup();
        }
        return upload.toTransfer();
    }

    private void bindConnectionEvents() {
        dataWrittenListener = eventData -> updateProgress(upload.getStartOffset() + eventData.currentLength());
        disconnectedListener = eventData -> {
            Throwable failure = eventData.exception();
            if (failure instanceof CancellationException || failure instanceof TimeoutException) {
                upload.settlement().fail(failure);
            } else {
                upload.settlement().fail(new ConnectionException("Transfer failed: " + eventData.message(), failure));
            }
        };
        dataWrittenSubscription = connection.subscribe(TransportConnection.Kind.DATA_WRITTEN, dataWrittenListener);
        disconnectedSubscription = connection.subscribe(TransportConnection.Kind.DISCONNECTED, disconnectedListener);
    }

    private void readStartOffset() throws InterruptedException, TimeoutException {
        try {
            byte[] bytes = connection.read(8, cancellationSignal);
            if (bytes.length != 8) {
                throw new IOException("Expected 8 bytes but received " + bytes.length);
            }
            upload.setStartOffset(
                    ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong());
        } catch (Throwable cause) {
            domain.log.debug("Failed to read start offset for upload of "
                    + filenameOnly(upload.getFilename()) + " to "
                    + upload.getUsername() + ": " + Failures.message(cause));
            if (cause instanceof CancellationException || cause instanceof TimeoutException) {
                throw Failures.rethrow(cause);
            }
            throw new MessageReadException("Failed to read transfer start offset: " + Failures.message(cause), cause);
        }
    }

    private void writeAndAwaitDisconnectRace() throws InterruptedException, TimeoutException {
        long remaining = upload.getSize() - upload.getStartOffset();
        Settlement<Void> settlement = upload.settlement();
        if (remaining == 0) {
            // Nothing to send, so nothing to race: the peer is re-requesting a
            // file it already has all of.
            settlement.succeed();
        } else {
            // A thread of its own because the write is raced against the
            // connection dropping; racing is work blocking code cannot do. Both
            // settle the one cell, so the first to arrive is the answer.
            domain.networkExecutor().executor().execute(() -> {
                try {
                    connection.write(
                            remaining,
                            source,
                            (requestedBytes, governorToken) ->
                                    domain.uploadTokenBucket.get(requestedBytes, cancellationSignal),
                            (attemptedBytes, grantedBytes, transferredBytes) -> {
                                if (transferOptions.reporter() != null) {
                                    transferOptions
                                            .reporter()
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

        Throwable failure = settlement.await().failure();
        if (failure != null) {
            throw Failures.rethrow(failure);
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
    private void linger() throws TimeoutException {
        long deadline = System.nanoTime()
                + Math.max(0, transferOptions.maximumLingerTime().toNanos());
        try {
            while (!cancellationSignal.isCancellationRequested()) {
                long remainingNanos = deadline - System.nanoTime();
                if (remainingNanos <= 0) {
                    connection.disconnect("Transfer complete, maximum linger " + "time exceeded");
                    return;
                }
                // On a thread of its own so the budget can lapse while the read
                // is still parked: a peer that says nothing is the ordinary case
                // here, and giving up on it is the point.
                Settlement<Void> read = new Settlement<>();
                domain.networkExecutor().executor().execute(() -> {
                    try {
                        connection.read(1, cancellationSignal);
                        read.succeed();
                    } catch (Throwable failure) {
                        read.fail(failure);
                    }
                });
                if (!read.await(Duration.ofNanos(remainingNanos))) {
                    connection.disconnect("Transfer complete, maximum " + "linger time exceeded");
                    return;
                }
                Throwable failure = read.outcome().failure();
                if (failure != null) {
                    throw Failures.rethrow(failure);
                }
            }
            cancellationSignal.throwIfCancellationRequested();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (Throwable failure) {
            if (!(failure instanceof ConnectionReadException)) {
                throw failure;
            }
        }
    }

    private void handleFailure(Throwable failure) {
        reportFailure(failure);
        if (failure instanceof TransferRejectedException) {
            upload.setException(failure);
            complete(TransferTermination.REJECTED);
            return;
        }
        if (failure instanceof TransferException) {
            // The only TransferException this run raises is the start offset
            // check above: the peer asked to resume past the end of our file.
            // Its offset cannot shrink and the file cannot grow, so a re-offer
            // replays the same exchange to the same end — the same reasoning
            // that makes a download's size mismatch ABORTED rather than
            // ERRORED. Falling through to the retryable branch below is what
            // spent three re-offers, then the peer's next request, then three
            // more, on an answer that was settled the first time.
            disconnectTransfer("Transfer aborted", failure);
            upload.setException(failure);
            updateProgress(currentStreamPosition());
            complete(TransferTermination.ABORTED);
            return;
        }
        if (failure instanceof CancellationException) {
            disconnectTransfer("Transfer cancelled", failure);
            upload.setException(failure);
            updateProgress(currentStreamPosition());
            complete(TransferTermination.CANCELLED);
            return;
        }
        if (failure instanceof TimeoutException) {
            disconnectTransfer("Transfer timed out", failure);
            upload.setException(failure);
            updateProgress(currentStreamPosition());
            complete(TransferTermination.TIMED_OUT);
            return;
        }
        disconnectTransfer("Transfer error", failure);
        upload.setException(failure);
        updateProgress(currentStreamPosition());
        complete(TransferTermination.ERRORED);
    }

    /**
     * Says why an upload ended badly, at the volume that ending deserves.
     *
     * <p>A peer that refuses, goes quiet, or a transfer we cancelled ourselves
     * are ordinary events on this network and go to debug alongside the rest of
     * the run's narration — a peer declining an upload it already finished is
     * the routine end of a race, not a fault. Anything else is worth surfacing,
     * and gets the exception with it.
     *
     * <p>That anything at all is written here is the fix. Every other step of an
     * upload announced itself and this one did not, so a failed upload left a
     * trace that stopped at whatever it had last managed to do and never said
     * what went wrong — the byte counts were all there, the reason never was.
     */
    private void reportFailure(Throwable failure) {
        String summary = "Upload of " + filenameOnly(upload.getFilename())
                + " to " + upload.getUsername() + " failed after "
                + currentStreamPosition() + " of " + upload.getSize()
                + " bytes (start offset " + upload.getStartOffset() + "): "
                + failure.getClass().getSimpleName() + ": " + Failures.message(failure);
        if (failure instanceof TransferRejectedException
                || failure instanceof CancellationException
                || failure instanceof TimeoutException) {
            domain.log.debug(summary);
            return;
        }
        domain.log.warn(
                summary, failure instanceof Exception exception ? exception : new RuntimeException(failure));
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
            if (transferOptions.closeInputStreamOnCompletion() && source != null) {
                try {
                    source.close();
                } catch (Throwable ignored) {
                    // Best-effort stream cleanup.
                }
            }
        } finally {
            releasePermits();
            if (perUserSemaphore != null) {
                domain.releaseUploadSemaphoreLease(upload.getUsername());
            }
            domain.uploads().remove(upload.getToken(), upload);
            domain.releaseUniqueKey(uniqueKey);
        }
        // After the permits, never before them. This used to sit inside the try
        // above, so the courtesy notification was sent while the run still held
        // the per-user semaphore, the upload slot and a global upload permit.
        if (upload.getTermination() != TransferTermination.SUCCEEDED) {
            notifyUploadFailure();
        }
    }

    private void unbindConnectionEvents() {
        if (connection == null) {
            return;
        }
        if (dataWrittenSubscription != null) {
            dataWrittenSubscription.close();
        }
        if (disconnectedSubscription != null) {
            disconnectedSubscription.close();
        }
    }

    /**
     * Tells the peer their upload failed, if we can already reach them.
     *
     * <p>Over a connection we already hold, or not at all. It used to resolve
     * the endpoint through the server and then call {@code
     * getOrAddMessageConnection}, which is the full direct-plus-indirect
     * establish — with {@link CancellationSignal#none()}, so nothing could cut
     * it short. For a peer that had just gone unreachable that is a guaranteed
     * wait of the whole indirect budget, and it bought nothing: an upload that
     * failed <em>because</em> the peer could not be reached cannot then be told
     * so. A recorded session spent ten seconds here on every one of forty
     * attempts to one firewalled peer, doubling the cost of each retry.
     *
     * <p>Nicotine+ sends the same message on the same event ({@code
     * uploads.py}, {@code send_message_to_peer}), but its send is a queue-and-
     * return on an event loop. The blocking establish is this port's own, and
     * it belongs to neither the run nor the slot it was holding.
     */
    private void notifyUploadFailure() {
        try {
            MessageConnection messageConnection = domain.peers().getCachedMessageConnection(upload.getUsername());
            if (messageConnection == null) {
                return;
            }
            OutgoingMessage message = upload.getTermination() == TransferTermination.CANCELLED
                    ? new UploadDenied(upload.getFilename(), "Cancelled")
                    : new UploadFailed(upload.getFilename());
            messageConnection.write(message, CancellationSignal.none());
        } catch (Throwable ignored) {
            // Failure notification is intentionally best effort.
        }
    }

    private void releasePermits() {
        if (perUserPermit) {
            perUserPermit = false;
            perUserSemaphore.release();
            domain.log.debug("Upload semaphore for file "
                    + filenameOnly(upload.getFilename()) + " to "
                    + upload.getUsername() + " released");
        }
        if (slot) {
            slot = false;
            domain.log.debug("Upload slot for file "
                    + filenameOnly(upload.getFilename()) + " to "
                    + upload.getUsername() + " released");
            if (transferOptions.slotReleased() != null) {
                try {
                    transferOptions.slotReleased().accept(upload.toTransfer());
                } catch (Throwable ignored) {
                    // Slot-release callbacks cannot block cleanup.
                }
            }
        }
        if (globalPermit) {
            globalPermit = false;
            domain.globalUploadSemaphore().release();
            domain.log.debug("Global upload semaphore for file "
                    + filenameOnly(upload.getFilename()) + " to "
                    + upload.getUsername() + " released");
        }
    }

    private void updateState(TransferPhase phase) {
        upload.setPhase(phase);
        publishStateChange(phase);
    }

    private void updateQueued(TransferQueueLocation location) {
        upload.queue(location);
        publishStateChange(TransferPhase.QUEUED);
    }

    private void complete(TransferTermination termination) {
        upload.complete(termination);
        publishStateChange(TransferPhase.COMPLETED);
    }

    private void publishStateChange(TransferPhase phase) {
        Transfer transfer = upload.toTransfer();
        TransferPhase previous = lastPhase;
        lastPhase = phase;
        if (transferOptions.stateChanged() != null) {
            transferOptions.stateChanged().accept(new TransferStateChange(previous, transfer));
        }
    }

    private void updateProgress(long bytesUploaded) {
        long previous = upload.getBytesTransferred();
        upload.updateProgress(bytesUploaded);
        Transfer transfer = upload.toTransfer();
        if (transferOptions.progressUpdated() != null) {
            transferOptions.progressUpdated().accept(new TransferProgressUpdate(previous, transfer));
        }
    }

    private long currentStreamPosition() {
        return source == null ? 0 : source.position();
    }
}
