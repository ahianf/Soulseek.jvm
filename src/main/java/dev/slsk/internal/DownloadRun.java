// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static dev.slsk.internal.transfer.TransferStreams.determineOutputPosition;
import static dev.slsk.internal.transfer.TransferStreams.filenameOnly;
import static dev.slsk.internal.transfer.TransferStreams.seekOutputStream;

import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.exceptions.TransferSizeMismatchException;
import dev.slsk.exceptions.TransferStreamException;
import dev.slsk.internal.common.CommonUtils;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.NetworkExecutor;
import dev.slsk.internal.common.Permits;
import dev.slsk.internal.common.Settlement;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.concurrent.CancellationController;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.concurrent.CancellationSubscription;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.messages.QueueDownloadRequest;
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
import dev.slsk.internal.transfer.Transfer;
import dev.slsk.internal.transfer.TransferInternal;
import dev.slsk.internal.transfer.TransferState;
import dev.slsk.internal.transfer.TransferStreams;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * One download, start to finish, on the thread that asked for it.
 *
 * <p>Slot acquisition, the peer handshake, the read loop and the terminal state
 * transitions, straight down the page. It is the <em>do</em> half of the split
 * {@link DownloadQueue} models and {@link TransferDomain} now applies to every
 * transfer: the domain decides that this download may run, and this runs it.
 *
 * <p>What it holds is its own domain, not the engine. Everything it needs — the
 * options, the correlator, the peer connections, the rate bucket, the limit it
 * waits on and the key it releases — belongs to transfers, and reaching two
 * hops through a client that owns everything was the arrangement this goal
 * exists to remove.
 */
final class DownloadRun {

    /** The domain that decided this run may happen, and owns what it needs. */
    private final TransferDomain domain;

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

    /**
     * The ceilings this run took, or {@code null} until it takes them.
     *
     * <p>Held rather than re-fetched, because a policy change replaces them: a
     * run that released to whatever the domain currently hands out would return
     * a permit to a semaphore that never issued one, and inflate the new
     * ceiling by one for the rest of the session.
     */
    private final AtomicReference<java.util.concurrent.Semaphore> globalPermit = new AtomicReference<>();

    private final AtomicReference<java.util.concurrent.Semaphore> userPermit = new AtomicReference<>();
    private final WaitKey transferStartRequestedWaitKey;
    private TransferState lastState = TransferState.NONE;
    private InetSocketAddress endpoint;
    private Connection connection;
    private OutputStream outputStream;
    private TransferStreams.PositionTrackingOutputStream trackingStream;
    private ConnectionEventListener<ConnectionDataEvent> dataReadListener;
    private ConnectionEventListener<ConnectionDisconnectedEvent> disconnectedListener;

    DownloadRun(
            TransferDomain domain,
            TransferInternal download,
            Supplier<OutputStream> outputStreamFactory,
            TransferOptions transferOptions,
            TransferRequest offer,
            CancellationSignal cancellationSignal,
            String uniqueKey) {
        this.offer = offer;
        this.domain = domain;
        this.download = download;
        this.outputStreamFactory = outputStreamFactory;
        this.transferOptions = transferOptions;
        this.cancellationSignal = cancellationSignal;
        this.uniqueKey = uniqueKey;
        transferStartRequestedWaitKey =
                new WaitKey(MessageCode.Peer.TRANSFER_REQUEST, download.getUsername(), download.getFilename());
    }

    /**
     * Runs the download to a terminal state and reports what it reached.
     *
     * <p>Returns rather than throws. A transfer that fails is not an exceptional
     * control flow — it is one of four things a transfer can do — and the layer
     * above turns this snapshot into the {@code TransferOutcome} a consumer
     * sees. What used to be here was a translation into
     * {@code SoulseekClientException} for a caller that no longer exists.
     *
     * @return the transfer in its terminal state
     */
    Transfer execute() {
        try {
            updateState(TransferState.QUEUED.or(TransferState.LOCALLY));
            endpoint = domain.endpoint(download.getUsername(), cancellationSignal);
            MessageConnection peerConnection =
                    domain.peers().getOrAddMessageConnection(download.getUsername(), endpoint, cancellationSignal);
            domain.diagnostic.debug("Fetched peer connection for download of "
                    + filenameOnly(download.getFilename()) + " from "
                    + download.getUsername() + " (id: " + peerConnection.getId()
                    + ", state: " + peerConnection.getState() + ")");

            if (offer != null) {
                // The peer already told us it is ready, so there is nothing to
                // ask for. Asking anyway would send a fresh request against the
                // queue we have just reached the front of, and a peer with one
                // free slot would answer the second request with "Queued".
                domain.diagnostic.debug("Download of " + filenameOnly(download.getFilename())
                        + " from " + download.getUsername()
                        + " is taking up an offer already made (remote token: "
                        + offer.getToken() + ")");
                updateState(TransferState.REQUESTED);
                beginQueuedDownload(() -> offer);
                receiveFile();
                return download.toTransfer();
            }

            // Registered before the ask, because a peer with a free slot can
            // answer before the write call returns and a wait registered
            // afterwards would miss its own reply.
            Wait<TransferRequest> transferStartRequested = domain.waiter.registerIndefinitely(
                    transferStartRequestedWaitKey, TransferRequest.class, cancellationSignal);

            // QueueUpload, not a download-direction TransferRequest. Both ask
            // for the same thing and every peer still understands the older
            // form, but only this one is fire-and-forget: there is no
            // acknowledgement to wait out, so asking for a whole album costs
            // one write per file instead of a round trip per file. A refusal
            // arrives later and out of band as UploadDenied, which fails the
            // wait registered above.
            peerConnection.write(
                    new QueueDownloadRequest(download.getFilename()), CommonUtils.token(cancellationSignal));
            domain.diagnostic.debug("Asked " + download.getUsername() + " to queue "
                    + filenameOnly(download.getFilename()) + " (id: " + peerConnection.getId()
                    + ", state: " + peerConnection.getState() + ")");
            updateState(TransferState.REQUESTED);

            beginQueuedDownload(transferStartRequested);
            receiveFile();
        } catch (Throwable failure) {
            handleFailure(Failures.unwrap(failure));
        } finally {
            cleanup();
        }
        return download.toTransfer();
    }

    /**
     * Reads the file off the transfer connection.
     *
     * <p>Shared by both ways of getting here — asking a peer for a file, and
     * taking up a file a peer offered. Only the handshake differs; once there is
     * a transfer connection there is exactly one way to receive bytes, and
     * keeping it in one place is what stops the offered path drifting into a
     * second, less-tested copy of the download.
     */
    private void receiveFile() throws IOException, InterruptedException {
        bindConnectionEvents();
        outputStream = Objects.requireNonNull(outputStreamFactory.get(), "outputStreamFactory result");
        positionOutputStream();
        trackingStream = new TransferStreams.PositionTrackingOutputStream(
                outputStream,
                determineOutputPosition(
                        outputStream,
                        transferOptions.isSeekOutputStreamAutomatically() ? download.getStartOffset() : 0));
        readTransfer();

        updateProgress(currentOutputPosition());
        updateState(TransferState.COMPLETED.or(TransferState.SUCCEEDED));
        domain.diagnostic.info("Download of " + filenameOnly(download.getFilename())
                + " from " + download.getUsername() + " complete ("
                + currentOutputPosition() + " of " + download.getSize() + " bytes).");
        connection.disconnect("Transfer complete");
    }

    private void beginQueuedDownload(Wait<TransferRequest> transferStartRequested)
            throws InterruptedException, TimeoutException {
        updateState(TransferState.QUEUED.or(TransferState.REMOTELY));
        TransferRequest request = transferStartRequested.await();

        // Acquired here rather than at the top of the run, because what these
        // bound is transfers and until now there was none. A download waiting
        // in a peer's queue holds no connection of its own — the peer message
        // connection is shared with every other download from that peer — so
        // charging it a slot bought nothing and cost everything: a whole album
        // queued one file at a time, each one rejoining the back of the peer's
        // queue after the last had finished.
        java.util.concurrent.Semaphore perUser = domain.downloadSemaphoreFor(download.getUsername());
        Permits.acquire(perUser, cancellationSignal);
        userPermit.set(perUser);
        java.util.concurrent.Semaphore overall = domain.globalDownloadSemaphore();
        Permits.acquire(overall, cancellationSignal);
        globalPermit.set(overall);
        domain.diagnostic.debug("Download slots for file "
                + filenameOnly(download.getFilename()) + " from "
                + download.getUsername() + " acquired");

        validateRemoteSize(request.getFileSize());
        if (download.getSize() == null) {
            download.setSize(request.getFileSize());
        }
        download.setRemoteToken(request.getToken());
        updateState(TransferState.INITIALIZING);

        MessageConnection refreshed =
                domain.peers().getOrAddMessageConnection(download.getUsername(), endpoint, cancellationSignal);
        domain.diagnostic.debug("Fetched peer connection for download of "
                + filenameOnly(download.getFilename()) + " from "
                + download.getUsername() + " (id: " + refreshed.getId()
                + ", state: " + refreshed.getState() + ")");
        // Started before the acceptance is written, because the peer opens the
        // transfer connection the moment it reads it. A thread of its own is
        // what "before" means here: the wait has to be in place while the write
        // happens, and neither can wait for the other.
        AtomicReference<Connection> incoming = new AtomicReference<>();
        Settlement established = new Settlement();
        NetworkExecutor.executor().execute(() -> {
            try {
                incoming.set(domain.peers()
                        .awaitTransferConnection(
                                download.getUsername(),
                                download.getFilename(),
                                download.getRemoteToken(),
                                cancellationSignal));
                established.succeed();
            } catch (Throwable failure) {
                established.fail(failure);
            }
        });
        refreshed.write(
                new TransferResponse(download.getRemoteToken(), download.getSize() == null ? 0 : download.getSize()),
                CommonUtils.token(cancellationSignal));

        Throwable failure = established.await();
        if (failure == null) {
            connection = incoming.get();
            domain.diagnostic.debug("Fetched transfer connection for download of "
                    + filenameOnly(download.getFilename()) + " from "
                    + download.getUsername() + " (id: " + connection.getId()
                    + ", state: " + connection.getState() + ")");
        } else {
            Throwable cause = Failures.unwrap(failure);
            if (!(cause instanceof ConnectionException)) {
                throw Failures.propagate(failure);
            }
            // The remote client never initiated the transfer connection, so initiate one from
            // this end. The remote client in this scenario is most likely Nicotine+.
            domain.diagnostic.warning("Attempting to initiate a second-chance transfer connection to "
                    + download.getUsername() + " for download of " + download.getFilename());
            connection = domain.peers()
                    .getTransferConnection(
                            download.getUsername(), endpoint, download.getRemoteToken(), cancellationSignal);
            domain.diagnostic.warning("Successfully established a second-chance transfer connection to "
                    + download.getUsername() + " for download of " + download.getFilename());
        }
        download.setConnection(connection);
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
                download.settlement().fail(failure);
            } else {
                download.settlement()
                        .fail(new ConnectionException("Transfer failed: " + eventData.getMessage(), failure));
            }
        };
        connection.addDataReadListener(dataReadListener);
        connection.addDisconnectedListener(disconnectedListener);
    }

    private void positionOutputStream() {
        if (download.getStartOffset() <= 0 || !transferOptions.isSeekOutputStreamAutomatically()) {
            return;
        }
        domain.diagnostic.debug("Seeking output stream for download of "
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

    private void readTransfer() throws InterruptedException {
        try (CancellationController linkedController = new CancellationController();
                CancellationSubscription registration = cancellationSignal.register(linkedController::cancel)) {
            CancellationSignal linkedToken = linkedController.getSignal();
            domain.diagnostic.debug("Seeking download of " + filenameOnly(download.getFilename())
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
            // connection dropping and against the peer reporting the transfer
            // failed on an entirely different connection; racing is work
            // blocking code cannot do. All three settle the one cell, so the
            // first to arrive is the answer and the rest are no-ops.
            Settlement settlement = download.settlement();
            NetworkExecutor.executor().execute(() -> {
                try {
                    connection.read(
                            download.getSize() - download.getStartOffset(),
                            trackingStream,
                            // The bucket is the whole of the metering now. A
                            // pluggable per-transfer governor sat in front of
                            // it and every implementation granted everything,
                            // which is what the bucket already does when the
                            // rate is unlimited.
                            (requestedBytes, governorToken) ->
                                    domain.downloadTokenBucket.get(requestedBytes, governorToken),
                            (attemptedBytes, grantedBytes, transferredBytes) -> {
                                if (transferOptions.getReporter() != null) {
                                    transferOptions
                                            .getReporter()
                                            .report(
                                                    download.toTransfer(),
                                                    attemptedBytes,
                                                    grantedBytes,
                                                    transferredBytes);
                                }
                                domain.downloadTokenBucket.returnTokens(grantedBytes - transferredBytes);
                            },
                            linkedToken);
                    settlement.succeed();
                } catch (Throwable failure) {
                    settlement.fail(failure);
                }
            });

            Throwable failure = settlement.await();
            // Whoever lost is still working; stopping it is what the linked
            // controller is for.
            linkedController.cancel();
            if (failure != null) {
                throw Failures.propagate(failure);
            }
        }
    }

    private void handleFailure(Throwable failure) {
        reportFailure(failure);
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

    /**
     * Says why a download ended badly, at the volume that ending deserves.
     *
     * <p>The counterpart to {@code UploadRun.reportFailure}, and it exists for
     * the same reason: a download narrated every step it took and then went
     * quiet at the only step worth reading, so a failed transfer left a trace
     * that stopped at whatever it had last managed to do and never said what
     * went wrong. Success has said {@code "Download of … complete"} all along.
     *
     * <p>A peer that refuses, goes quiet, or a transfer we cancelled ourselves
     * are ordinary events on this network and go to debug alongside the rest of
     * the run's narration. Anything else is a fault worth surfacing, and gets
     * the exception with it.
     */
    private void reportFailure(Throwable failure) {
        String summary = "Download of " + filenameOnly(download.getFilename())
                + " from " + download.getUsername() + " failed after "
                + currentOutputPosition() + " of " + download.getSize()
                + " bytes (start offset " + download.getStartOffset() + "): "
                + failure.getClass().getSimpleName() + ": " + Failures.message(failure);
        if (failure instanceof TransferRejectedException
                || failure instanceof CancellationException
                || failure instanceof TimeoutException) {
            domain.diagnostic.debug(summary);
            return;
        }
        domain.diagnostic.warning(
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
            try {
                domain.waiter.cancel(transferStartRequestedWaitKey);
            } catch (Throwable failure) {
                domain.diagnostic.warning(
                        "Failed to cancel wait for key "
                                + transferStartRequestedWaitKey
                                + ": " + Failures.message(failure),
                        failure);
            }
            try {
                unbindConnectionEvents();
            } catch (Throwable failure) {
                domain.diagnostic.warning(
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
                    domain.diagnostic.warning(
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
                    domain.diagnostic.warning(
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
            release(globalPermit, "global");
            release(userPermit, "per-user");
            domain.downloads().remove(download.getToken(), download);
            domain.releaseUniqueKey(uniqueKey);
        }
    }

    /** Returns one held ceiling, at most once, to the instance it was taken from. */
    private void release(AtomicReference<java.util.concurrent.Semaphore> held, String which) {
        java.util.concurrent.Semaphore permit = held.getAndSet(null);
        if (permit == null) {
            return;
        }
        try {
            permit.release();
        } catch (Throwable failure) {
            domain.diagnostic.warning(
                    "Failed to release the " + which + " download slot for file "
                            + filenameOnly(download.getFilename())
                            + " from " + download.getUsername() + ": "
                            + Failures.message(failure),
                    failure);
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
        TransferState previous = lastState;
        lastState = state;
        if (transferOptions.getStateChanged() != null) {
            transferOptions.getStateChanged().onStateChanged(new TransferStateChange(previous, transfer));
        }
    }

    private void updateProgress(long bytesDownloaded) {
        long previous = download.getBytesTransferred();
        download.updateProgress(bytesDownloaded);
        Transfer transfer = download.toTransfer();
        if (transferOptions.getProgressUpdated() != null) {
            transferOptions.getProgressUpdated().onProgressUpdated(new TransferProgressUpdate(previous, transfer));
        }
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
            domain.diagnostic.warning(
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
