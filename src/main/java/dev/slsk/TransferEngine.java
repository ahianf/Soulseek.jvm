// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static dev.slsk.ClientSupport.acquirePermit;
import static dev.slsk.ClientSupport.failureMessage;
import static dev.slsk.ClientSupport.mapClientFailure;
import static dev.slsk.ClientSupport.requireText;
import static dev.slsk.ClientSupport.unwrap;

import dev.slsk.common.NetworkExecutor;
import dev.slsk.common.WaitKey;
import dev.slsk.events.TransferProgressUpdatedEvent;
import dev.slsk.events.TransferStateChangedEvent;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.ConnectionReadException;
import dev.slsk.exceptions.DuplicateTokenException;
import dev.slsk.exceptions.DuplicateTransferException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.exceptions.TransferException;
import dev.slsk.exceptions.TransferNotFoundException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.exceptions.TransferSizeMismatchException;
import dev.slsk.exceptions.TransferStreamException;
import dev.slsk.exceptions.UserOfflineException;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.messages.OutgoingMessage;
import dev.slsk.messaging.messages.PlaceInQueueRequest;
import dev.slsk.messaging.messages.PlaceInQueueResponse;
import dev.slsk.messaging.messages.TransferRequest;
import dev.slsk.messaging.messages.TransferResponse;
import dev.slsk.messaging.messages.UploadDenied;
import dev.slsk.messaging.messages.UploadFailed;
import dev.slsk.network.MessageConnection;
import dev.slsk.network.tcp.Connection;
import dev.slsk.network.tcp.ConnectionDataEvent;
import dev.slsk.network.tcp.ConnectionDisconnectedEvent;
import dev.slsk.network.tcp.ConnectionEventListener;
import dev.slsk.options.DownloadStreamFactory;
import dev.slsk.options.PositionableInputStream;
import dev.slsk.options.PositionableOutputStream;
import dev.slsk.options.TransferOptions;
import dev.slsk.options.TransferProgressUpdate;
import dev.slsk.options.TransferStateChange;
import dev.slsk.options.UploadStreamFactory;
import dev.slsk.transfer.TransferInternal;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Download and upload orchestration: queueing, slot acquisition, the per-user
 * and global concurrency limits, rate limiting and the transfer state machine.
 *
 * <p>The largest piece of the split, and the one whose state is genuinely
 * shared: the download and upload registries stay on the client because
 * incoming peer messages are dispatched against them from the message
 * handlers. What this owns is everything the client did not need to see — the
 * concurrency semaphores, the rate-limit buckets, and the duplicate-transfer
 * keys.
 */
final class TransferEngine {

    private final ClientContext context;

    /** Global transfer concurrency limits; a transfer concern, so owned here. */
    private final java.util.concurrent.Semaphore globalDownloadSemaphore;

    private final java.util.concurrent.Semaphore globalUploadSemaphore;

    /** Per-user upload limits, and the lock guarding their creation. */
    private final java.util.Map<String, java.util.concurrent.Semaphore> uploadSemaphores =
            new java.util.concurrent.ConcurrentHashMap<>();

    private final java.util.concurrent.Semaphore uploadSemaphoreSyncRoot = new java.util.concurrent.Semaphore(1);

    /** Duplicate-transfer keys; owned here, since this is what detects duplicates. */
    private final java.util.Map<String, Boolean> uniqueKeys = new java.util.concurrent.ConcurrentHashMap<>();

    TransferEngine(ClientContext context) {
        this.context = java.util.Objects.requireNonNull(context, "context");
        this.globalDownloadSemaphore =
                new java.util.concurrent.Semaphore(context.getClientOptions().getMaximumConcurrentDownloads());
        this.globalUploadSemaphore =
                new java.util.concurrent.Semaphore(context.getClientOptions().getMaximumConcurrentUploads());
    }

    /** Downloads a remote file to a local file. */
    CompletableFuture<Transfer> download(String requestedUsername, String remoteFilename, String localFilename) {
        return download(
                requestedUsername, remoteFilename, localFilename, null, 0, null, null, CancellationSignal.none());
    }
    /** Downloads a remote file with an expected size. */
    CompletableFuture<Transfer> download(
            String requestedUsername, String remoteFilename, String localFilename, Long size) {
        return download(
                requestedUsername, remoteFilename, localFilename, size, 0, null, null, CancellationSignal.none());
    }
    /** Downloads a remote file with cancellation. */
    CompletableFuture<Transfer> download(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            CancellationSignal cancellationSignal) {
        return download(requestedUsername, remoteFilename, localFilename, null, 0, null, null, cancellationSignal);
    }
    /** Downloads a remote file from a resume offset. */
    CompletableFuture<Transfer> download(
            String requestedUsername, String remoteFilename, String localFilename, Long size, long startOffset) {
        return download(
                requestedUsername,
                remoteFilename,
                localFilename,
                size,
                startOffset,
                null,
                null,
                CancellationSignal.none());
    }
    /** Downloads a remote file with a specific token. */
    CompletableFuture<Transfer> download(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Long size,
            long startOffset,
            Integer token) {
        return download(
                requestedUsername,
                remoteFilename,
                localFilename,
                size,
                startOffset,
                token,
                null,
                CancellationSignal.none());
    }
    /** Downloads a remote file using supplied transfer context.getClientOptions(). */
    CompletableFuture<Transfer> download(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions) {
        return download(
                requestedUsername,
                remoteFilename,
                localFilename,
                size,
                startOffset,
                token,
                transferOptions,
                CancellationSignal.none());
    }
    /** Downloads a remote file to a local file. */
    CompletableFuture<Transfer> download(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        requireText(requestedUsername, "username");
        requireText(remoteFilename, "remoteFilename");
        requireText(localFilename, "localFilename");
        validateDownloadRange(size, startOffset);
        context.requireLoggedIn("download files");
        int transferToken = token == null ? context.getTokenFactory().nextToken() : token;
        validateDownloadUniqueness(requestedUsername, remoteFilename, transferToken);
        TransferOptions options =
                (transferOptions == null ? new TransferOptions() : transferOptions).withDisposalOptions(null, true);
        return downloadToStreamAsync(
                requestedUsername,
                remoteFilename,
                () -> {
                    try {
                        return CompletableFuture.completedFuture(
                                context.getIoAdapter().getOutputStream(localFilename, startOffset > 0));
                    } catch (IOException failure) {
                        return CompletableFuture.failedFuture(new UncheckedIOException(failure));
                    }
                },
                size,
                startOffset,
                transferToken,
                options,
                context.defaultToken(cancellationSignal));
    }
    /** Downloads data to a stream created by a factory. */
    CompletableFuture<Transfer> download(
            String requestedUsername, String remoteFilename, DownloadStreamFactory outputStreamFactory) {
        return download(
                requestedUsername, remoteFilename, outputStreamFactory, null, 0, null, null, CancellationSignal.none());
    }
    /** Downloads stream data with an expected size. */
    CompletableFuture<Transfer> download(
            String requestedUsername, String remoteFilename, DownloadStreamFactory outputStreamFactory, Long size) {
        return download(
                requestedUsername, remoteFilename, outputStreamFactory, size, 0, null, null, CancellationSignal.none());
    }
    /** Downloads stream data with cancellation. */
    CompletableFuture<Transfer> download(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            CancellationSignal cancellationSignal) {
        return download(
                requestedUsername, remoteFilename, outputStreamFactory, null, 0, null, null, cancellationSignal);
    }
    /** Downloads stream data from a resume offset. */
    CompletableFuture<Transfer> download(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset) {
        return download(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                null,
                null,
                CancellationSignal.none());
    }
    /** Downloads stream data with a specific token. */
    CompletableFuture<Transfer> download(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token) {
        return download(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                token,
                null,
                CancellationSignal.none());
    }
    /** Downloads stream data using supplied transfer context.getClientOptions(). */
    CompletableFuture<Transfer> download(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions) {
        return download(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                token,
                transferOptions,
                CancellationSignal.none());
    }
    /** Downloads data to a stream created by a factory. */
    CompletableFuture<Transfer> download(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        requireText(requestedUsername, "username");
        requireText(remoteFilename, "remoteFilename");
        validateDownloadRange(size, startOffset);
        Objects.requireNonNull(outputStreamFactory, "outputStreamFactory");
        context.requireLoggedIn("download files");
        int transferToken = token == null ? context.getTokenFactory().nextToken() : token;
        validateDownloadUniqueness(requestedUsername, remoteFilename, transferToken);
        return downloadToStreamAsync(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                transferToken,
                transferOptions == null ? new TransferOptions() : transferOptions,
                context.defaultToken(cancellationSignal));
    }
    /** Uploads a local file to a peer. */
    CompletableFuture<Transfer> upload(String requestedUsername, String remoteFilename, String localFilename) {
        return upload(requestedUsername, remoteFilename, localFilename, null, null, CancellationSignal.none());
    }
    /** Uploads a local file to a peer with a specific token. */
    CompletableFuture<Transfer> upload(
            String requestedUsername, String remoteFilename, String localFilename, Integer token) {
        return upload(requestedUsername, remoteFilename, localFilename, token, null, CancellationSignal.none());
    }
    /** Uploads a local file with cancellation. */
    CompletableFuture<Transfer> upload(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            CancellationSignal cancellationSignal) {
        return upload(requestedUsername, remoteFilename, localFilename, null, null, cancellationSignal);
    }
    /** Uploads a local file using the supplied context.getClientOptions(). */
    CompletableFuture<Transfer> upload(
            String requestedUsername, String remoteFilename, String localFilename, TransferOptions transferOptions) {
        return upload(
                requestedUsername, remoteFilename, localFilename, null, transferOptions, CancellationSignal.none());
    }
    /** Uploads a local file to a peer using the supplied context.getClientOptions(). */
    CompletableFuture<Transfer> upload(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Integer token,
            TransferOptions transferOptions) {
        return upload(
                requestedUsername, remoteFilename, localFilename, token, transferOptions, CancellationSignal.none());
    }
    /** Uploads a local file to a peer. */
    CompletableFuture<Transfer> upload(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Integer token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        requireText(requestedUsername, "username");
        requireText(remoteFilename, "remoteFilename");
        requireText(localFilename, "localFilename");
        if (!context.getIoAdapter().exists(localFilename)) {
            throw new UncheckedIOException(
                    new FileNotFoundException("The local file does not exist: " + localFilename));
        }
        context.requireLoggedIn("upload files");
        try (InputStream ignored = context.getIoAdapter().getInputStream(localFilename)) {
            // Probe readability before allocating a transfer token.
        } catch (IOException failure) {
            throw new UncheckedIOException(
                    "The local file " + localFilename + " could not be opened for reading: " + failureMessage(failure),
                    failure);
        }

        int transferToken = token == null ? context.getTokenFactory().nextToken() : token;
        validateUploadUniqueness(requestedUsername, remoteFilename, transferToken);
        TransferOptions options = transferOptions == null ? new TransferOptions() : transferOptions;
        TransferOptions fileOptions =
                (options == null ? new TransferOptions() : options).withDisposalOptions(true, null);
        long size;
        try {
            size = context.getIoAdapter().getFileInfo(localFilename).size();
        } catch (IOException failure) {
            return CompletableFuture.failedFuture(new UncheckedIOException(failure));
        }
        return uploadFromStreamAsync(
                requestedUsername,
                remoteFilename,
                size,
                ignoredOffset -> {
                    try {
                        return CompletableFuture.completedFuture(
                                context.getIoAdapter().getInputStream(localFilename));
                    } catch (IOException failure) {
                        return CompletableFuture.failedFuture(new UncheckedIOException(failure));
                    }
                },
                transferToken,
                fileOptions,
                context.defaultToken(cancellationSignal));
    }
    /** Uploads data supplied by an asynchronous stream factory. */
    CompletableFuture<Transfer> upload(
            String requestedUsername, String remoteFilename, long size, UploadStreamFactory inputStreamFactory) {
        return upload(
                requestedUsername, remoteFilename, size, inputStreamFactory, null, null, CancellationSignal.none());
    }
    /** Uploads stream data with a specific transfer token. */
    CompletableFuture<Transfer> upload(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            Integer token) {
        return upload(
                requestedUsername, remoteFilename, size, inputStreamFactory, token, null, CancellationSignal.none());
    }
    /** Uploads stream data with cancellation. */
    CompletableFuture<Transfer> upload(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            CancellationSignal cancellationSignal) {
        return upload(requestedUsername, remoteFilename, size, inputStreamFactory, null, null, cancellationSignal);
    }
    /** Uploads stream data using the supplied context.getClientOptions(). */
    CompletableFuture<Transfer> upload(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            TransferOptions transferOptions) {
        return upload(
                requestedUsername,
                remoteFilename,
                size,
                inputStreamFactory,
                null,
                transferOptions,
                CancellationSignal.none());
    }
    /** Uploads stream data using the supplied transfer context.getClientOptions(). */
    CompletableFuture<Transfer> upload(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            Integer token,
            TransferOptions transferOptions) {
        return upload(
                requestedUsername,
                remoteFilename,
                size,
                inputStreamFactory,
                token,
                transferOptions,
                CancellationSignal.none());
    }
    /** Uploads data supplied by an asynchronous stream factory. */
    CompletableFuture<Transfer> upload(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            Integer token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        requireText(requestedUsername, "username");
        requireText(remoteFilename, "remoteFilename");
        if (size < 0) {
            throw new IllegalArgumentException("size must be greater than or equal to zero");
        }
        Objects.requireNonNull(inputStreamFactory, "inputStreamFactory");
        context.requireLoggedIn("upload files");
        int transferToken = token == null ? context.getTokenFactory().nextToken() : token;
        validateUploadUniqueness(requestedUsername, remoteFilename, transferToken);
        return uploadFromStreamAsync(
                requestedUsername,
                remoteFilename,
                size,
                inputStreamFactory,
                transferToken,
                transferOptions == null ? new TransferOptions() : transferOptions,
                context.defaultToken(cancellationSignal));
    }
    /** Enqueues a local-file download. */
    CompletableFuture<CompletableFuture<Transfer>> enqueueDownload(
            String requestedUsername, String remoteFilename, String localFilename) {
        return enqueueDownload(
                requestedUsername, remoteFilename, localFilename, null, 0, null, null, CancellationSignal.none());
    }
    /** Enqueues a local-file download with an expected size. */
    CompletableFuture<CompletableFuture<Transfer>> enqueueDownload(
            String requestedUsername, String remoteFilename, String localFilename, Long size) {
        return enqueueDownload(
                requestedUsername, remoteFilename, localFilename, size, 0, null, null, CancellationSignal.none());
    }
    /** Enqueues a local-file download from a resume offset. */
    CompletableFuture<CompletableFuture<Transfer>> enqueueDownload(
            String requestedUsername, String remoteFilename, String localFilename, Long size, long startOffset) {
        return enqueueDownload(
                requestedUsername,
                remoteFilename,
                localFilename,
                size,
                startOffset,
                null,
                null,
                CancellationSignal.none());
    }
    /** Enqueues a local-file download with a specific token. */
    CompletableFuture<CompletableFuture<Transfer>> enqueueDownload(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Long size,
            long startOffset,
            Integer token) {
        return enqueueDownload(
                requestedUsername,
                remoteFilename,
                localFilename,
                size,
                startOffset,
                token,
                null,
                CancellationSignal.none());
    }
    /** Enqueues a local-file download using supplied context.getClientOptions(). */
    CompletableFuture<CompletableFuture<Transfer>> enqueueDownload(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions) {
        return enqueueDownload(
                requestedUsername,
                remoteFilename,
                localFilename,
                size,
                startOffset,
                token,
                transferOptions,
                CancellationSignal.none());
    }
    /** Enqueues a local-file download. */
    CompletableFuture<CompletableFuture<Transfer>> enqueueDownload(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        CompletableFuture<Boolean> enqueued = new CompletableFuture<>();
        TransferOptions options = (transferOptions == null ? new TransferOptions() : transferOptions)
                .withAdditionalStateChanged(change ->
                        completeDownloadEnqueue(enqueued, change.transfer().getState()));
        CompletableFuture<Transfer> download = download(
                requestedUsername,
                remoteFilename,
                localFilename,
                size,
                startOffset,
                token,
                options,
                cancellationSignal);
        return nestedDownloadWhenEnqueued(enqueued, download);
    }
    /** Enqueues a stream-factory download. */
    CompletableFuture<CompletableFuture<Transfer>> enqueueDownload(
            String requestedUsername, String remoteFilename, DownloadStreamFactory outputStreamFactory) {
        return enqueueDownload(
                requestedUsername, remoteFilename, outputStreamFactory, null, 0, null, null, CancellationSignal.none());
    }
    /** Enqueues a stream-factory download with an expected size. */
    CompletableFuture<CompletableFuture<Transfer>> enqueueDownload(
            String requestedUsername, String remoteFilename, DownloadStreamFactory outputStreamFactory, Long size) {
        return enqueueDownload(
                requestedUsername, remoteFilename, outputStreamFactory, size, 0, null, null, CancellationSignal.none());
    }
    /** Enqueues a stream-factory download from a resume offset. */
    CompletableFuture<CompletableFuture<Transfer>> enqueueDownload(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset) {
        return enqueueDownload(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                null,
                null,
                CancellationSignal.none());
    }
    /** Enqueues a stream-factory download with a specific token. */
    CompletableFuture<CompletableFuture<Transfer>> enqueueDownload(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token) {
        return enqueueDownload(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                token,
                null,
                CancellationSignal.none());
    }
    /** Enqueues a stream-factory download using supplied context.getClientOptions(). */
    CompletableFuture<CompletableFuture<Transfer>> enqueueDownload(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions) {
        return enqueueDownload(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                token,
                transferOptions,
                CancellationSignal.none());
    }
    /** Enqueues a stream-factory download. */
    CompletableFuture<CompletableFuture<Transfer>> enqueueDownload(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        CompletableFuture<Boolean> enqueued = new CompletableFuture<>();
        TransferOptions options = (transferOptions == null ? new TransferOptions() : transferOptions)
                .withAdditionalStateChanged(change ->
                        completeDownloadEnqueue(enqueued, change.transfer().getState()));
        CompletableFuture<Transfer> download = download(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                token,
                options,
                cancellationSignal);
        return nestedDownloadWhenEnqueued(enqueued, download);
    }
    /** Enqueues a local-file upload and returns its nested completion future. */
    CompletableFuture<CompletableFuture<Transfer>> enqueueUpload(
            String requestedUsername, String remoteFilename, String localFilename) {
        return enqueueUpload(requestedUsername, remoteFilename, localFilename, null, null, CancellationSignal.none());
    }
    /** Enqueues a local-file upload with a specific token. */
    CompletableFuture<CompletableFuture<Transfer>> enqueueUpload(
            String requestedUsername, String remoteFilename, String localFilename, Integer token) {
        return enqueueUpload(requestedUsername, remoteFilename, localFilename, token, null, CancellationSignal.none());
    }
    /** Enqueues a local-file upload with cancellation. */
    CompletableFuture<CompletableFuture<Transfer>> enqueueUpload(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            CancellationSignal cancellationSignal) {
        return enqueueUpload(requestedUsername, remoteFilename, localFilename, null, null, cancellationSignal);
    }
    /** Enqueues a local-file upload using supplied context.getClientOptions(). */
    CompletableFuture<CompletableFuture<Transfer>> enqueueUpload(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Integer token,
            TransferOptions transferOptions) {
        return enqueueUpload(
                requestedUsername, remoteFilename, localFilename, token, transferOptions, CancellationSignal.none());
    }
    /** Enqueues a local-file upload. */
    CompletableFuture<CompletableFuture<Transfer>> enqueueUpload(
            String requestedUsername,
            String remoteFilename,
            String localFilename,
            Integer token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        CompletableFuture<Boolean> enqueued = new CompletableFuture<>();
        TransferOptions options = (transferOptions == null ? new TransferOptions() : transferOptions)
                .withAdditionalStateChanged(change -> {
                    if (change.transfer().getState().equals(TransferState.QUEUED.or(TransferState.LOCALLY))) {
                        enqueued.complete(true);
                    }
                });
        CompletableFuture<Transfer> upload =
                upload(requestedUsername, remoteFilename, localFilename, token, options, cancellationSignal);
        return enqueued.thenApply(ignored -> upload);
    }
    /** Enqueues a stream-factory upload. */
    CompletableFuture<CompletableFuture<Transfer>> enqueueUpload(
            String requestedUsername, String remoteFilename, long size, UploadStreamFactory inputStreamFactory) {
        return enqueueUpload(
                requestedUsername, remoteFilename, size, inputStreamFactory, null, null, CancellationSignal.none());
    }
    /** Enqueues a stream-factory upload with a specific token. */
    CompletableFuture<CompletableFuture<Transfer>> enqueueUpload(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            Integer token) {
        return enqueueUpload(
                requestedUsername, remoteFilename, size, inputStreamFactory, token, null, CancellationSignal.none());
    }
    /** Enqueues a stream-factory upload with cancellation. */
    CompletableFuture<CompletableFuture<Transfer>> enqueueUpload(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            CancellationSignal cancellationSignal) {
        return enqueueUpload(
                requestedUsername, remoteFilename, size, inputStreamFactory, null, null, cancellationSignal);
    }
    /** Enqueues a stream-factory upload using supplied context.getClientOptions(). */
    CompletableFuture<CompletableFuture<Transfer>> enqueueUpload(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            Integer token,
            TransferOptions transferOptions) {
        return enqueueUpload(
                requestedUsername,
                remoteFilename,
                size,
                inputStreamFactory,
                token,
                transferOptions,
                CancellationSignal.none());
    }
    /** Enqueues a stream-factory upload. */
    CompletableFuture<CompletableFuture<Transfer>> enqueueUpload(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            Integer token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        CompletableFuture<Boolean> enqueued = new CompletableFuture<>();
        TransferOptions options = (transferOptions == null ? new TransferOptions() : transferOptions)
                .withAdditionalStateChanged(change -> {
                    if (change.transfer().getState().equals(TransferState.QUEUED.or(TransferState.LOCALLY))) {
                        enqueued.complete(true);
                    }
                });
        CompletableFuture<Transfer> upload =
                upload(requestedUsername, remoteFilename, size, inputStreamFactory, token, options, cancellationSignal);
        return enqueued.thenApply(ignored -> upload);
    }

    CompletableFuture<Integer> getDownloadPlaceInQueue(String requestedUsername, String filename) {
        return getDownloadPlaceInQueue(requestedUsername, filename, CancellationSignal.none());
    }

    CompletableFuture<Integer> getDownloadPlaceInQueue(
            String requestedUsername, String filename, CancellationSignal cancellationSignal) {
        requireText(requestedUsername, "username");
        requireText(filename, "filename");
        context.requireLoggedIn("check download queue position");
        boolean active = context.getDownloadRegistry().values().stream()
                .anyMatch(download -> Objects.equals(download.getUsername(), requestedUsername)
                        && Objects.equals(download.getFilename(), filename));
        if (!active) {
            throw new TransferNotFoundException(
                    "A download of " + filename + " from user " + requestedUsername + " is not active");
        }
        CancellationSignal token = context.defaultToken(cancellationSignal);
        CompletableFuture<PlaceInQueueResponse> responseWait;
        try {
            responseWait = context.getWaiter()
                    .waitAsync(
                            new WaitKey(MessageCode.Peer.PLACE_IN_QUEUE_RESPONSE, requestedUsername, filename),
                            PlaceInQueueResponse.class,
                            null,
                            token);
        } catch (Throwable failure) {
            return mapClientFailure(
                    CompletableFuture.failedFuture(failure),
                    "Failed to fetch place in queue for download of " + filename + " from " + requestedUsername + ": ",
                    UserOfflineException.class);
        }
        CompletableFuture<Integer> operation = context.resolveUserEndpoint(requestedUsername, token)
                .thenCompose(endpoint -> context.getPeerConnectionManager()
                        .getOrAddMessageConnectionAsync(requestedUsername, endpoint, token))
                .thenCompose(connection -> context.writeToPeer(connection, new PlaceInQueueRequest(filename), token))
                .thenCompose(ignored -> responseWait)
                .thenApply(PlaceInQueueResponse::getPlaceInQueue);
        return mapClientFailure(
                operation,
                "Failed to fetch place in queue for download of " + filename + " from " + requestedUsername + ": ",
                UserOfflineException.class);
    }

    static void completeDownloadEnqueue(CompletableFuture<Boolean> enqueued, TransferState state) {
        if (state.equals(TransferState.QUEUED.or(TransferState.REMOTELY))) {
            enqueued.complete(true);
        } else if (state.contains(TransferState.COMPLETED)) {
            enqueued.complete(false);
        }
    }

    CompletableFuture<Transfer> downloadToStreamAsync(
            String requestedUsername,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            int token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        TransferOptions operationOptions = transferOptions == null ? new TransferOptions() : transferOptions;
        TransferInternal download = new TransferInternal(
                TransferDirection.DOWNLOAD, requestedUsername, remoteFilename, token, operationOptions);
        download.setStartOffset(startOffset);
        download.setSize(size);
        String uniqueKey = downloadUniqueKey(requestedUsername, remoteFilename);

        if (uniqueKeys.putIfAbsent(uniqueKey, true) != null) {
            return CompletableFuture.failedFuture(new DuplicateTransferException(
                    "Duplicate download of " + remoteFilename + " from " + requestedUsername + " aborted"));
        }
        if (context.getDownloadRegistry().putIfAbsent(token, download) != null) {
            uniqueKeys.remove(uniqueKey);
            return CompletableFuture.failedFuture(new DuplicateTransferException(
                    "Duplicate download of " + remoteFilename + " from " + requestedUsername + " aborted"));
        }

        DownloadOperation operation =
                new DownloadOperation(download, outputStreamFactory, operationOptions, cancellationSignal, uniqueKey);
        return NetworkExecutor.supplyAsync(operation::execute);
    }

    CompletableFuture<Transfer> uploadFromStreamAsync(
            String requestedUsername,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            int token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        TransferOptions operationOptions = transferOptions == null ? new TransferOptions() : transferOptions;
        TransferInternal upload = new TransferInternal(
                TransferDirection.UPLOAD, requestedUsername, remoteFilename, token, operationOptions);
        upload.setSize(size);
        String uniqueKey = uploadUniqueKey(requestedUsername, remoteFilename);

        if (uniqueKeys.putIfAbsent(uniqueKey, true) != null) {
            return CompletableFuture.failedFuture(new DuplicateTransferException(
                    "Duplicate upload of " + remoteFilename + " to " + requestedUsername + " aborted"));
        }
        if (context.getUploadRegistry().putIfAbsent(token, upload) != null) {
            uniqueKeys.remove(uniqueKey);
            return CompletableFuture.failedFuture(new DuplicateTransferException(
                    "Duplicate upload of " + remoteFilename + " to " + requestedUsername + " aborted"));
        }

        UploadOperation operation =
                new UploadOperation(upload, inputStreamFactory, operationOptions, cancellationSignal, uniqueKey);
        return NetworkExecutor.supplyAsync(operation::execute);
    }

    static CompletableFuture<CompletableFuture<Transfer>> nestedDownloadWhenEnqueued(
            CompletableFuture<Boolean> enqueued, CompletableFuture<Transfer> download) {
        return enqueued.thenCompose(success -> {
            if (success) {
                return CompletableFuture.completedFuture(download);
            }
            return download.thenApply(ignored -> download);
        });
    }

    static String downloadUniqueKey(String requestedUsername, String remoteFilename) {
        return "Download:" + requestedUsername + ":" + remoteFilename;
    }

    static String uploadUniqueKey(String requestedUsername, String remoteFilename) {
        return "Upload:" + requestedUsername + ":" + remoteFilename;
    }

    static void validateDownloadRange(Long size, long startOffset) {
        if (size != null && size < 0) {
            throw new IllegalArgumentException("size must be greater than or equal to zero");
        }
        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset must be greater than or equal to zero");
        }
        if (startOffset > 0 && size == null) {
            throw new NullPointerException("size must be specified when startOffset is non-zero");
        }
    }

    void validateDownloadUniqueness(String requestedUsername, String remoteFilename, int token) {
        if (context.getUploadRegistry().containsKey(token)
                || context.getDownloadRegistry().containsKey(token)) {
            throw new DuplicateTokenException("The specified or generated token " + token + " is already in progress");
        }
        boolean duplicate = context.getDownloadRegistry().values().stream()
                .anyMatch(download -> Objects.equals(download.getUsername(), requestedUsername)
                        && Objects.equals(download.getFilename(), remoteFilename));
        if (duplicate || uniqueKeys.containsKey(downloadUniqueKey(requestedUsername, remoteFilename))) {
            throw new DuplicateTransferException("An active or queued download of "
                    + remoteFilename + " from " + requestedUsername
                    + " is already in progress");
        }
    }

    void validateUploadUniqueness(String requestedUsername, String remoteFilename, int token) {
        if (context.getUploadRegistry().containsKey(token)
                || context.getDownloadRegistry().containsKey(token)) {
            throw new DuplicateTokenException("The specified or generated token " + token + " is already in progress");
        }
        boolean duplicate = context.getUploadRegistry().values().stream()
                .anyMatch(upload -> Objects.equals(upload.getUsername(), requestedUsername)
                        && Objects.equals(upload.getFilename(), remoteFilename));
        if (duplicate || uniqueKeys.containsKey(uploadUniqueKey(requestedUsername, remoteFilename))) {
            throw new DuplicateTransferException("An active or queued upload of "
                    + remoteFilename + " to " + requestedUsername
                    + " is already in progress");
        }
    }

    class DownloadOperation {
        private final TransferInternal download;
        private final DownloadStreamFactory outputStreamFactory;
        private final TransferOptions transferOptions;
        private final CancellationSignal cancellationSignal;
        private final String uniqueKey;
        private final AtomicBoolean globalPermit = new AtomicBoolean();
        private final CompletableFuture<Void> disconnected = new CompletableFuture<>();
        private final WaitKey transferStartRequestedWaitKey;
        private TransferState lastState = TransferState.NONE;
        private InetSocketAddress endpoint;
        private Connection connection;
        private OutputStream outputStream;
        private PositionTrackingOutputStream trackingStream;
        private ConnectionEventListener<ConnectionDataEvent> dataReadListener;
        private ConnectionEventListener<ConnectionDisconnectedEvent> disconnectedListener;

        private DownloadOperation(
                TransferInternal download,
                DownloadStreamFactory outputStreamFactory,
                TransferOptions transferOptions,
                CancellationSignal cancellationSignal,
                String uniqueKey) {
            this.download = download;
            this.outputStreamFactory = outputStreamFactory;
            this.transferOptions = transferOptions;
            this.cancellationSignal = cancellationSignal;
            this.uniqueKey = uniqueKey;
            transferStartRequestedWaitKey =
                    new WaitKey(MessageCode.Peer.TRANSFER_REQUEST, download.getUsername(), download.getFilename());
        }

        private Transfer execute() {
            try {
                updateState(TransferState.QUEUED.or(TransferState.LOCALLY));
                await(acquirePermit(globalDownloadSemaphore, cancellationSignal));
                globalPermit.set(true);
                context.getDiagnostic()
                        .debug("Global download semaphore for file "
                                + filenameOnly(download.getFilename()) + " to "
                                + download.getUsername() + " acquired");

                endpoint = await(context.resolveUserEndpoint(download.getUsername(), cancellationSignal));
                MessageConnection peerConnection = await(context.getPeerConnectionManager()
                        .getOrAddMessageConnectionAsync(download.getUsername(), endpoint, cancellationSignal));
                context.getDiagnostic()
                        .debug("Fetched peer connection for download of "
                                + filenameOnly(download.getFilename()) + " from "
                                + download.getUsername() + " (id: " + peerConnection.getId()
                                + ", state: " + peerConnection.getState() + ")");

                CompletableFuture<TransferResponse> transferRequestAcknowledged = context.getWaiter()
                        .waitAsync(
                                new WaitKey(
                                        MessageCode.Peer.TRANSFER_RESPONSE,
                                        download.getUsername(),
                                        download.getToken()),
                                TransferResponse.class,
                                context.getClientOptions()
                                        .getPeerConnectionOptions()
                                        .getInactivityTimeout(),
                                cancellationSignal);
                CompletableFuture<TransferRequest> transferStartRequested = context.getWaiter()
                        .waitIndefinitelyAsync(
                                transferStartRequestedWaitKey, TransferRequest.class, cancellationSignal);

                await(context.writeToPeer(
                        peerConnection,
                        new TransferRequest(TransferDirection.DOWNLOAD, download.getToken(), download.getFilename()),
                        cancellationSignal));
                context.getDiagnostic()
                        .debug("Wrote transfer request for download of "
                                + filenameOnly(download.getFilename()) + " from "
                                + download.getUsername() + " (id: " + peerConnection.getId()
                                + ", state: " + peerConnection.getState() + ")");
                updateState(TransferState.REQUESTED);

                TransferResponse acknowledgement = await(transferRequestAcknowledged);
                context.getDiagnostic()
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

                bindConnectionEvents();
                outputStream =
                        Objects.requireNonNull(await(outputStreamFactory.openAsync()), "outputStreamFactory result");
                positionOutputStream();
                trackingStream = new PositionTrackingOutputStream(
                        outputStream,
                        determineOutputPosition(
                                outputStream,
                                transferOptions.isSeekOutputStreamAutomatically() ? download.getStartOffset() : 0));
                readTransfer();

                updateProgress(currentOutputPosition());
                updateState(TransferState.COMPLETED.or(TransferState.SUCCEEDED));
                context.getDiagnostic()
                        .info("Download of " + filenameOnly(download.getFilename())
                                + " from " + download.getUsername() + " complete ("
                                + currentOutputPosition() + " of " + download.getSize() + " bytes).");
                connection.disconnect("Transfer complete");
                return download.toTransfer();
            } catch (Throwable failure) {
                Throwable cause = unwrap(failure);
                handleFailure(cause);
                throw new CompletionException(mapDownloadFailure(cause));
            } finally {
                cleanup();
            }
        }

        private MessageConnection beginImmediateDownload(
                TransferResponse acknowledgement, MessageConnection peerConnection) {
            validateRemoteSize(acknowledgement.getFileSize());
            updateState(TransferState.QUEUED.or(TransferState.REMOTELY));
            if (download.getSize() == null) {
                download.setSize(acknowledgement.getFileSize());
            }
            updateState(TransferState.INITIALIZING);
            connection = await(context.getPeerConnectionManager()
                    .getTransferConnectionAsync(
                            download.getUsername(), endpoint, acknowledgement.getToken(), cancellationSignal));
            context.getDiagnostic()
                    .debug("Fetched transfer connection for download of "
                            + filenameOnly(download.getFilename()) + " from "
                            + download.getUsername() + " (id: " + connection.getId()
                            + ", state: " + connection.getState() + ")");
            download.setConnection(connection);
            return peerConnection;
        }

        private MessageConnection beginQueuedDownload(
                CompletableFuture<TransferRequest> transferStartRequested, MessageConnection peerConnection) {
            updateState(TransferState.QUEUED.or(TransferState.REMOTELY));
            TransferRequest request = await(transferStartRequested);
            validateRemoteSize(request.getFileSize());
            if (download.getSize() == null) {
                download.setSize(request.getFileSize());
            }
            download.setRemoteToken(request.getToken());
            updateState(TransferState.INITIALIZING);

            MessageConnection refreshed = await(context.getPeerConnectionManager()
                    .getOrAddMessageConnectionAsync(download.getUsername(), endpoint, cancellationSignal));
            context.getDiagnostic()
                    .debug("Fetched peer connection for download of "
                            + filenameOnly(download.getFilename()) + " from "
                            + download.getUsername() + " (id: " + refreshed.getId()
                            + ", state: " + refreshed.getState() + ")");
            CompletableFuture<Connection> connectionTask = context.getPeerConnectionManager()
                    .awaitTransferConnectionAsync(
                            download.getUsername(),
                            download.getFilename(),
                            download.getRemoteToken(),
                            cancellationSignal);
            await(context.writeToPeer(
                    refreshed,
                    new TransferResponse(
                            download.getRemoteToken(), download.getSize() == null ? 0 : download.getSize()),
                    cancellationSignal));
            try {
                connection = await(connectionTask);
                context.getDiagnostic()
                        .debug("Fetched transfer connection for download of "
                                + filenameOnly(download.getFilename()) + " from "
                                + download.getUsername() + " (id: " + connection.getId()
                                + ", state: " + connection.getState() + ")");
            } catch (Throwable failure) {
                Throwable cause = unwrap(failure);
                if (!(cause instanceof ConnectionException)) {
                    throw failure;
                }
                // The remote client never initiated the transfer connection, so initiate one from
                // this end. The remote client in this scenario is most likely Nicotine+.
                context.getDiagnostic()
                        .warning("Attempting to initiate a second-chance transfer connection to "
                                + download.getUsername() + " for download of " + download.getFilename());
                connection = await(context.getPeerConnectionManager()
                        .getTransferConnectionAsync(
                                download.getUsername(), endpoint, download.getRemoteToken(), cancellationSignal));
                context.getDiagnostic()
                        .warning("Successfully established a second-chance transfer connection to "
                                + download.getUsername() + " for download of " + download.getFilename());
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
            context.getDiagnostic()
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
                context.getDiagnostic()
                        .debug("Seeking download of " + filenameOnly(download.getFilename())
                                + " from " + download.getUsername() + " to starting offset of "
                                + download.getStartOffset() + " bytes");
                byte[] offset = ByteBuffer.allocate(8)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putLong(download.getStartOffset())
                        .array();
                await(connection.writeAsync(offset, linkedToken));
                updateState(TransferState.IN_PROGRESS);
                updateProgress(download.getStartOffset());

                CompletableFuture<Void> read = connection.readAsync(
                        download.getSize() - download.getStartOffset(),
                        trackingStream,
                        (requestedBytes, governorToken) -> transferOptions
                                .getGovernor()
                                .grantAsync(download.toTransfer(), requestedBytes, governorToken)
                                .thenCompose(granted -> context.getDownloadTokenBucket()
                                        .getAsync(Math.min(requestedBytes, granted), governorToken)),
                        (attemptedBytes, grantedBytes, transferredBytes) -> {
                            if (transferOptions.getReporter() != null) {
                                transferOptions
                                        .getReporter()
                                        .report(download.toTransfer(), attemptedBytes, grantedBytes, transferredBytes);
                            }
                            context.getDownloadTokenBucket().returnTokens(grantedBytes - transferredBytes);
                        },
                        linkedToken);

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
                            + ": " + failureMessage(failure),
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
                    context.getWaiter().cancel(transferStartRequestedWaitKey);
                } catch (Throwable failure) {
                    context.getDiagnostic()
                            .warning(
                                    "Failed to cancel wait for key "
                                            + transferStartRequestedWaitKey
                                            + ": " + failureMessage(failure),
                                    failure);
                }
                try {
                    unbindConnectionEvents();
                } catch (Throwable failure) {
                    context.getDiagnostic()
                            .warning(
                                    "Failed to remove transfer connection "
                                            + "listeners for file "
                                            + download.getFilename() + " from user "
                                            + download.getUsername() + ": "
                                            + failureMessage(failure),
                                    failure);
                }
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (Throwable failure) {
                        context.getDiagnostic()
                                .warning(
                                        "Failed to dispose transfer connection "
                                                + "for file "
                                                + download.getFilename()
                                                + " from user "
                                                + download.getUsername() + ": "
                                                + failureMessage(failure),
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
                        context.getDiagnostic()
                                .warning(
                                        "Failed to finalize output stream for "
                                                + "file "
                                                + filenameOnly(download.getFilename())
                                                + " from "
                                                + download.getUsername() + ": "
                                                + failureMessage(failure),
                                        failure);
                    }
                }
            } finally {
                if (globalPermit.compareAndSet(true, false)) {
                    try {
                        globalDownloadSemaphore.release();
                    } catch (Throwable failure) {
                        context.getDiagnostic()
                                .warning(
                                        "Failed to release global download "
                                                + "semaphore for file "
                                                + filenameOnly(download.getFilename())
                                                + " from "
                                                + download.getUsername() + ": "
                                                + failureMessage(failure),
                                        failure);
                    }
                }
                context.getDownloadRegistry().remove(download.getToken(), download);
                uniqueKeys.remove(uniqueKey);
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
            context.raiseSearchEvent(DefaultSoulseekClient.Event.TRANSFER_STATE_CHANGED, eventData);
        }

        private void updateProgress(long bytesDownloaded) {
            long previous = download.getBytesTransferred();
            download.updateProgress(bytesDownloaded);
            Transfer transfer = download.toTransfer();
            if (transferOptions.getProgressUpdated() != null) {
                transferOptions.getProgressUpdated().onProgressUpdated(new TransferProgressUpdate(previous, transfer));
            }
            context.raiseSearchEvent(
                    DefaultSoulseekClient.Event.TRANSFER_PROGRESS_UPDATED,
                    new TransferProgressUpdatedEvent(previous, transfer));
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
                context.getDiagnostic()
                        .warning(
                                "Failed to determine final position of output "
                                        + "stream for file "
                                        + filenameOnly(download.getFilename())
                                        + " from " + download.getUsername() + ": "
                                        + failureMessage(failure),
                                failure);
                return 0;
            }
        }
    }

    class UploadOperation {
        private final TransferInternal upload;
        private final UploadStreamFactory inputStreamFactory;
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
        private PositionTrackingInputStream trackingStream;
        private ConnectionEventListener<ConnectionDataEvent> dataWrittenListener;
        private ConnectionEventListener<ConnectionDisconnectedEvent> disconnectedListener;

        private UploadOperation(
                TransferInternal upload,
                UploadStreamFactory inputStreamFactory,
                TransferOptions transferOptions,
                CancellationSignal cancellationSignal,
                String uniqueKey) {
            this.upload = upload;
            this.inputStreamFactory = inputStreamFactory;
            this.transferOptions = transferOptions;
            this.cancellationSignal = cancellationSignal;
            this.uniqueKey = uniqueKey;
        }

        private Transfer execute() {
            try {
                await(acquirePermit(uploadSemaphoreSyncRoot, cancellationSignal));
                CompletableFuture<Void> perUserWait;
                try {
                    perUserSemaphore = uploadSemaphores.computeIfAbsent(
                            upload.getUsername(),
                            ignored -> new Semaphore(context.getClientOptions().getMaximumConcurrentUploadsPerUser()));
                    perUserWait = acquirePermit(perUserSemaphore, cancellationSignal);
                } finally {
                    uploadSemaphoreSyncRoot.release();
                }

                updateState(TransferState.QUEUED.or(TransferState.LOCALLY));

                await(perUserWait);
                perUserPermit.set(true);
                context.getDiagnostic()
                        .debug("Upload semaphore for file "
                                + filenameOnly(upload.getFilename()) + " to "
                                + upload.getUsername() + " acquired");

                try {
                    await(transferOptions.getSlotAwaiter().awaitSlotAsync(upload.toTransfer(), cancellationSignal));
                    slot.set(true);
                    context.getDiagnostic()
                            .debug("Upload slot for file "
                                    + filenameOnly(upload.getFilename()) + " to "
                                    + upload.getUsername() + " acquired");
                } catch (Throwable failure) {
                    Throwable cause = unwrap(failure);
                    if (cause instanceof CancellationException) {
                        throw cause;
                    }
                    throw new TransferException(
                            "Failed to acquire an upload slot for file "
                                    + filenameOnly(upload.getFilename())
                                    + " to " + upload.getUsername() + ": "
                                    + failureMessage(cause),
                            cause);
                }

                await(acquirePermit(globalUploadSemaphore, cancellationSignal));
                globalPermit.set(true);
                context.getDiagnostic()
                        .debug("Global upload semaphore for file "
                                + filenameOnly(upload.getFilename()) + " to "
                                + upload.getUsername() + " acquired");

                endpoint = await(context.resolveUserEndpoint(upload.getUsername(), cancellationSignal));
                MessageConnection messageConnection = await(context.getPeerConnectionManager()
                        .getOrAddMessageConnectionAsync(upload.getUsername(), endpoint, cancellationSignal));
                context.getDiagnostic()
                        .debug("Fetched peer connection for upload of "
                                + filenameOnly(upload.getFilename()) + " to "
                                + upload.getUsername() + " (id: " + messageConnection.getId()
                                + ", state: " + messageConnection.getState() + ")");

                CompletableFuture<TransferResponse> transferRequestAcknowledged = context.getWaiter()
                        .waitAsync(
                                new WaitKey(
                                        MessageCode.Peer.TRANSFER_RESPONSE, upload.getUsername(), upload.getToken()),
                                TransferResponse.class,
                                context.getClientOptions()
                                        .getPeerConnectionOptions()
                                        .getInactivityTimeout(),
                                cancellationSignal);
                await(context.writeToPeer(
                        messageConnection,
                        new TransferRequest(
                                TransferDirection.UPLOAD, upload.getToken(), upload.getFilename(), upload.getSize()),
                        cancellationSignal));
                context.getDiagnostic()
                        .debug("Wrote transfer request for upload of "
                                + filenameOnly(upload.getFilename()) + " to "
                                + upload.getUsername() + " (id: " + messageConnection.getId()
                                + ", state: " + messageConnection.getState() + ")");
                updateState(TransferState.REQUESTED);

                TransferResponse acknowledgement = await(transferRequestAcknowledged);
                context.getDiagnostic()
                        .debug("Received transfer request ACK for upload of "
                                + filenameOnly(upload.getFilename()) + " to "
                                + upload.getUsername() + ": allowed: " + acknowledgement.isAllowed()
                                + ", message: " + acknowledgement.getMessage()
                                + " (token: " + upload.getToken() + ")");
                if (!acknowledgement.isAllowed()) {
                    throw new TransferRejectedException("Transfer rejected: " + acknowledgement.getMessage());
                }

                updateState(TransferState.INITIALIZING);
                connection = await(context.getPeerConnectionManager()
                        .getTransferConnectionAsync(
                                upload.getUsername(), endpoint, upload.getToken(), cancellationSignal));
                context.getDiagnostic()
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

                context.getDiagnostic()
                        .debug("Resolving input stream for upload of " + filenameOnly(upload.getFilename()) + " to "
                                + upload.getUsername());
                inputStream = Objects.requireNonNull(
                        await(inputStreamFactory.openAsync(upload.getStartOffset())), "inputStreamFactory result");
                positionInputStream();
                trackingStream = new PositionTrackingInputStream(
                        inputStream, determinePosition(inputStream, upload.getStartOffset()));

                updateState(TransferState.IN_PROGRESS);
                updateProgress(upload.getStartOffset());
                writeAndAwaitDisconnectRace();
                linger();

                updateProgress(currentStreamPosition());
                updateState(TransferState.COMPLETED.or(TransferState.SUCCEEDED));
                return upload.toTransfer();
            } catch (Throwable failure) {
                Throwable cause = unwrap(failure);
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
                Throwable cause = unwrap(failure);
                context.getDiagnostic()
                        .debug("Failed to read start offset for upload of "
                                + filenameOnly(upload.getFilename()) + " to "
                                + upload.getUsername() + ": " + failureMessage(cause));
                if (cause instanceof CancellationException || cause instanceof TimeoutException) {
                    throw new CompletionException(cause);
                }
                throw new MessageReadException("Failed to read transfer start offset: " + failureMessage(cause), cause);
            }
        }

        private void positionInputStream() {
            if (upload.getStartOffset() <= 0 || !transferOptions.isSeekInputStreamAutomatically()) {
                return;
            }
            context.getDiagnostic()
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
                            (requestedBytes, governorToken) -> transferOptions
                                    .getGovernor()
                                    .grantAsync(upload.toTransfer(), requestedBytes, governorToken)
                                    .thenCompose(granted -> context.getUploadTokenBucket()
                                            .getAsync(Math.min(requestedBytes, granted), cancellationSignal)),
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
                                context.getUploadTokenBucket().returnTokens(grantedBytes - transferredBytes);
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
            long deadline = System.nanoTime()
                    + TimeUnit.MILLISECONDS.toNanos(Math.max(0, transferOptions.getMaximumLingerTime()));
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
                        Throwable cause = unwrap(failure);
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
                if (!(unwrap(failure) instanceof ConnectionReadException)) {
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
                            + failureMessage(failure),
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
                context.getUploadRegistry().remove(upload.getToken(), upload);
                uniqueKeys.remove(uniqueKey);
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
                        await(context.resolveUserEndpoint(upload.getUsername(), CancellationSignal.none()));
                MessageConnection messageConnection = await(context.getPeerConnectionManager()
                        .getOrAddMessageConnectionAsync(
                                upload.getUsername(), currentEndpoint, CancellationSignal.none()));
                OutgoingMessage message = upload.getState().contains(TransferState.CANCELLED)
                        ? new UploadDenied(upload.getFilename(), "Cancelled")
                        : new UploadFailed(upload.getFilename());
                await(context.writeToPeer(messageConnection, message, CancellationSignal.none()));
            } catch (Throwable ignored) {
                // Failure notification is intentionally best effort.
            }
        }

        private void releasePermits() {
            if (perUserPermit.compareAndSet(true, false)) {
                perUserSemaphore.release();
                context.getDiagnostic()
                        .debug("Upload semaphore for file "
                                + filenameOnly(upload.getFilename()) + " to "
                                + upload.getUsername() + " released");
            }
            if (slot.compareAndSet(true, false)) {
                context.getDiagnostic()
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
                globalUploadSemaphore.release();
                context.getDiagnostic()
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
            context.raiseSearchEvent(DefaultSoulseekClient.Event.TRANSFER_STATE_CHANGED, eventData);
        }

        private void updateProgress(long bytesUploaded) {
            long previous = upload.getBytesTransferred();
            upload.updateProgress(bytesUploaded);
            Transfer transfer = upload.toTransfer();
            if (transferOptions.getProgressUpdated() != null) {
                transferOptions.getProgressUpdated().onProgressUpdated(new TransferProgressUpdate(previous, transfer));
            }
            context.raiseSearchEvent(
                    DefaultSoulseekClient.Event.TRANSFER_PROGRESS_UPDATED,
                    new TransferProgressUpdatedEvent(previous, transfer));
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

    private static final class PositionTrackingInputStream extends FilterInputStream {
        private long position;

        private PositionTrackingInputStream(InputStream inputStream, long initialPosition) {
            super(inputStream);
            position = initialPosition;
        }

        private long getPosition() {
            return position;
        }

        @Override
        public int read() throws IOException {
            int value = super.read();
            if (value >= 0) {
                position++;
            }
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            int read = super.read(bytes, offset, length);
            if (read > 0) {
                position += read;
            }
            return read;
        }

        @Override
        public long skip(long count) throws IOException {
            long skipped = super.skip(count);
            position += skipped;
            return skipped;
        }
    }

    private static final class PositionTrackingOutputStream extends FilterOutputStream {
        private long position;

        private PositionTrackingOutputStream(OutputStream outputStream, long initialPosition) {
            super(outputStream);
            position = initialPosition;
        }

        private long getPosition() {
            return position;
        }

        @Override
        public void write(int value) throws IOException {
            out.write(value);
            position++;
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            out.write(bytes, offset, length);
            position += length;
        }
    }
    /** Blocks on an internal future, unwrapping the completion wrapper. */
    private static <T> T await(java.util.concurrent.CompletableFuture<T> future) {
        try {
            return future.join();
        } catch (Throwable failure) {
            throw new java.util.concurrent.CompletionException(unwrap(failure));
        }
    }

    private static long determineOutputPosition(OutputStream stream, long fallback) throws IOException {
        if (stream instanceof PositionableOutputStream positionable) {
            return positionable.getPosition();
        }
        if (stream instanceof FileOutputStream fileOutputStream) {
            return fileOutputStream.getChannel().position();
        }
        return fallback;
    }

    private static long determinePosition(InputStream stream, long fallback) throws IOException {
        if (stream instanceof PositionableInputStream positionable) {
            return positionable.getPosition();
        }
        if (stream instanceof FileInputStream fileInputStream) {
            return fileInputStream.getChannel().position();
        }
        return fallback;
    }

    private static String filenameOnly(String filename) {
        try {
            Path path = Path.of(filename);
            Path leaf = path.getFileName();
            return leaf == null ? filename : leaf.toString();
        } catch (Throwable ignored) {
            return filename;
        }
    }

    private static boolean isQueuedResponse(String message) {
        int end = message.length();
        while (end > 0 && message.charAt(end - 1) == '.') {
            end--;
        }
        return message.substring(0, end).equalsIgnoreCase("Queued");
    }

    private static void seekInputStream(InputStream stream, long position) throws IOException {
        if (stream instanceof PositionableInputStream positionable) {
            positionable.setPosition(position);
            return;
        }
        if (stream instanceof FileInputStream fileInputStream) {
            fileInputStream.getChannel().position(position);
            return;
        }
        if (stream instanceof ByteArrayInputStream) {
            stream.reset();
            skipFully(stream, position);
            return;
        }
        throw new IOException("Input stream is not seekable");
    }

    private static void seekOutputStream(OutputStream stream, long position) throws IOException {
        if (stream instanceof PositionableOutputStream positionable) {
            positionable.setPosition(position);
            return;
        }
        if (stream instanceof FileOutputStream fileOutputStream) {
            fileOutputStream.getChannel().position(position);
            return;
        }
        throw new IOException("Output stream is not seekable");
    }

    private static void skipFully(InputStream stream, long count) throws IOException {
        long remaining = count;
        while (remaining > 0) {
            long skipped = stream.skip(remaining);
            if (skipped <= 0) {
                if (stream.read() < 0) {
                    throw new IOException("Input stream ended before position " + count);
                }
                skipped = 1;
            }
            remaining -= skipped;
        }
    }
    /** Exposes the duplicate-transfer keys for the client's test accessor. */
    java.util.Map<String, Boolean> getUniqueKeys() {
        return uniqueKeys;
    }
}
