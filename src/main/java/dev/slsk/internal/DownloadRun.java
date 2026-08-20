// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static dev.slsk.internal.transfer.TransferChannels.filenameOnly;

import dev.slsk.Subscription;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.exceptions.TransferSizeMismatchException;
import dev.slsk.internal.common.CancellationSignals;
import dev.slsk.internal.common.Failures;
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
import dev.slsk.internal.network.tcp.ConnectionDataEvent;
import dev.slsk.internal.network.tcp.ConnectionDisconnectedEvent;
import dev.slsk.internal.network.tcp.TransportConnection;
import dev.slsk.internal.options.TransferOptions;
import dev.slsk.internal.options.TransferProgressUpdate;
import dev.slsk.internal.options.TransferStateChange;
import dev.slsk.internal.transfer.Transfer;
import dev.slsk.internal.transfer.TransferChannels;
import dev.slsk.internal.transfer.TransferInternal;
import dev.slsk.internal.transfer.TransferPhase;
import dev.slsk.internal.transfer.TransferQueueLocation;
import dev.slsk.internal.transfer.TransferTermination;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

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
    private final TransferChannels.DestinationFactory destinationFactory;
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
    private java.util.concurrent.Semaphore globalPermit;

    private java.util.concurrent.Semaphore userPermit;
    private final WaitKey transferStartRequestedWaitKey;
    private TransferPhase lastPhase = TransferPhase.NONE;
    private InetSocketAddress endpoint;
    private TransportConnection connection;
    private TransferChannels.TrackingWritableChannel destination;
    private Consumer<ConnectionDataEvent> dataReadListener;
    private Consumer<ConnectionDisconnectedEvent> disconnectedListener;
    private Subscription dataReadSubscription;
    private Subscription disconnectedSubscription;

    DownloadRun(
            TransferDomain domain,
            TransferInternal download,
            TransferChannels.DestinationFactory destinationFactory,
            TransferOptions transferOptions,
            TransferRequest offer,
            CancellationSignal cancellationSignal,
            String uniqueKey) {
        this.offer = offer;
        this.domain = domain;
        this.download = download;
        this.destinationFactory = destinationFactory;
        this.transferOptions = transferOptions;
        this.cancellationSignal = cancellationSignal;
        this.uniqueKey = uniqueKey;
        transferStartRequestedWaitKey =
                new WaitKey.PeerFile(MessageCode.Peer.TRANSFER_REQUEST, download.getUsername(), download.getFilename());
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
            updateQueued(TransferQueueLocation.LOCAL);
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
                updateState(TransferPhase.REQUESTED);
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
                    new QueueDownloadRequest(download.getFilename()), CancellationSignals.orNone(cancellationSignal));
            domain.diagnostic.debug("Asked " + download.getUsername() + " to queue "
                    + filenameOnly(download.getFilename()) + " (id: " + peerConnection.getId()
                    + ", state: " + peerConnection.getState() + ")");
            updateState(TransferPhase.REQUESTED);

            beginQueuedDownload(transferStartRequested);
            receiveFile();
        } catch (Throwable failure) {
            handleFailure(failure);
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
    private void receiveFile() throws IOException, InterruptedException, TimeoutException {
        bindConnectionEvents();
        destination = Objects.requireNonNull(
                destinationFactory.open(download.getStartOffset(), transferOptions.seekOutputStreamAutomatically()),
                "destinationFactory result");
        readTransfer();
        if (!transferOptions.closeOutputStreamOnCompletion()) {
            destination.flush();
        }

        updateProgress(currentOutputPosition());
        complete(TransferTermination.SUCCEEDED);
        domain.diagnostic.info("Download of " + filenameOnly(download.getFilename())
                + " from " + download.getUsername() + " complete ("
                + currentOutputPosition() + " of " + download.getSize() + " bytes).");
        connection.disconnect("Transfer complete");
    }

    private void beginQueuedDownload(Wait<TransferRequest> transferStartRequested)
            throws InterruptedException, TimeoutException {
        updateQueued(TransferQueueLocation.REMOTE);
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
        userPermit = perUser;
        java.util.concurrent.Semaphore overall = domain.globalDownloadSemaphore();
        Permits.acquire(overall, cancellationSignal);
        globalPermit = overall;
        domain.diagnostic.debug("Download slots for file "
                + filenameOnly(download.getFilename()) + " from "
                + download.getUsername() + " acquired");

        validateRemoteSize(request.getFileSize());
        if (download.getSize() == null) {
            download.setSize(request.getFileSize());
        }
        download.setRemoteToken(request.getToken());
        updateState(TransferPhase.INITIALIZING);

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
        Settlement<TransportConnection> established = new Settlement<>();
        domain.networkExecutor().executor().execute(() -> {
            try {
                established.succeed(domain.peers()
                        .awaitTransferConnection(
                                download.getUsername(),
                                download.getFilename(),
                                download.getRemoteToken(),
                                cancellationSignal));
            } catch (Throwable failure) {
                established.fail(failure);
            }
        });
        refreshed.write(
                new TransferResponse(download.getRemoteToken(), download.getSize() == null ? 0 : download.getSize()),
                CancellationSignals.orNone(cancellationSignal));

        Settlement.Outcome<TransportConnection> establishment = established.await();
        Throwable failure = establishment.failure();
        if (failure == null) {
            connection = establishment.value();
            domain.diagnostic.debug("Fetched transfer connection for download of "
                    + filenameOnly(download.getFilename()) + " from "
                    + download.getUsername() + " (id: " + connection.getId()
                    + ", state: " + connection.getState() + ")");
        } else {
            if (!(failure instanceof ConnectionException)) {
                throw Failures.rethrow(failure);
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
        dataReadListener = eventData -> updateProgress(download.getStartOffset() + eventData.currentLength());
        disconnectedListener = eventData -> {
            Throwable failure = eventData.exception();
            if (failure instanceof CancellationException || failure instanceof TimeoutException) {
                download.settlement().fail(failure);
            } else {
                download.settlement().fail(new ConnectionException("Transfer failed: " + eventData.message(), failure));
            }
        };
        dataReadSubscription = connection.subscribe(TransportConnection.Kind.DATA_READ, dataReadListener);
        disconnectedSubscription = connection.subscribe(TransportConnection.Kind.DISCONNECTED, disconnectedListener);
    }

    private void readTransfer() throws InterruptedException, TimeoutException {
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
            updateState(TransferPhase.IN_PROGRESS);
            updateProgress(download.getStartOffset());

            // A thread of its own because the read is raced against the
            // connection dropping and against the peer reporting the transfer
            // failed on an entirely different connection; racing is work
            // blocking code cannot do. All three settle the one cell, so the
            // first to arrive is the answer and the rest are no-ops.
            Settlement<Void> settlement = download.settlement();
            domain.networkExecutor().executor().execute(() -> {
                try {
                    connection.read(
                            download.getSize() - download.getStartOffset(),
                            destination,
                            // The bucket is the whole of the metering now. A
                            // pluggable per-transfer governor sat in front of
                            // it and every implementation granted everything,
                            // which is what the bucket already does when the
                            // rate is unlimited.
                            (requestedBytes, governorToken) ->
                                    domain.downloadTokenBucket.get(requestedBytes, governorToken),
                            (attemptedBytes, grantedBytes, transferredBytes) -> {
                                if (transferOptions.reporter() != null) {
                                    transferOptions
                                            .reporter()
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

            Throwable failure = settlement.await().failure();
            // Whoever lost is still working; stopping it is what the linked
            // controller is for.
            linkedController.cancel();
            if (failure != null) {
                throw Failures.rethrow(failure);
            }
        }
    }

    private void handleFailure(Throwable failure) {
        reportFailure(failure);
        if (failure instanceof TransferRejectedException) {
            download.setException(failure);
            complete(TransferTermination.REJECTED);
            return;
        }
        if (failure instanceof TransferSizeMismatchException) {
            download.setException(failure);
            complete(TransferTermination.ABORTED);
            return;
        }
        if (failure instanceof CancellationException) {
            disconnectTransfer("Transfer cancelled", failure);
            download.setException(failure);
            updateProgress(currentOutputPosition());
            complete(TransferTermination.CANCELLED);
            return;
        }
        if (failure instanceof TimeoutException) {
            disconnectTransfer("Transfer timed out", failure);
            download.setException(failure);
            updateProgress(currentOutputPosition());
            complete(TransferTermination.TIMED_OUT);
            return;
        }
        disconnectTransfer("Transfer error", failure);
        download.setException(failure);
        updateProgress(currentOutputPosition());
        complete(TransferTermination.ERRORED);
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
                            "Failed to close transfer connection "
                                    + "for file "
                                    + download.getFilename()
                                    + " from user "
                                    + download.getUsername() + ": "
                                    + Failures.message(failure),
                            failure);
                }
            }
            if (transferOptions.closeOutputStreamOnCompletion() && destination != null) {
                try {
                    try {
                        destination.flush();
                    } finally {
                        destination.close();
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
            globalPermit = release(globalPermit, "global");
            userPermit = release(userPermit, "per-user");
            domain.downloads().remove(download.getToken(), download);
            domain.releaseUniqueKey(uniqueKey);
        }
    }

    /** Returns one held ceiling, at most once, to the instance it was taken from. */
    private java.util.concurrent.Semaphore release(java.util.concurrent.Semaphore permit, String which) {
        if (permit == null) {
            return null;
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
        return null;
    }

    private void unbindConnectionEvents() {
        if (connection == null) {
            return;
        }
        if (dataReadSubscription != null) {
            dataReadSubscription.close();
        }
        if (disconnectedSubscription != null) {
            disconnectedSubscription.close();
        }
    }

    private void updateState(TransferPhase phase) {
        download.setPhase(phase);
        publishStateChange(phase);
    }

    private void updateQueued(TransferQueueLocation location) {
        download.queue(location);
        publishStateChange(TransferPhase.QUEUED);
    }

    private void complete(TransferTermination termination) {
        download.complete(termination);
        publishStateChange(TransferPhase.COMPLETED);
    }

    private void publishStateChange(TransferPhase phase) {
        Transfer transfer = download.toTransfer();
        TransferPhase previous = lastPhase;
        lastPhase = phase;
        if (transferOptions.stateChanged() != null) {
            transferOptions.stateChanged().accept(new TransferStateChange(previous, transfer));
        }
    }

    private void updateProgress(long bytesDownloaded) {
        long previous = download.getBytesTransferred();
        download.updateProgress(bytesDownloaded);
        Transfer transfer = download.toTransfer();
        if (transferOptions.progressUpdated() != null) {
            transferOptions.progressUpdated().accept(new TransferProgressUpdate(previous, transfer));
        }
    }

    private long currentOutputPosition() {
        return destination == null ? 0 : destination.position();
    }
}
