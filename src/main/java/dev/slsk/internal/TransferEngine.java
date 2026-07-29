// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.CancellationSignal;
import dev.slsk.exceptions.DuplicateTokenException;
import dev.slsk.exceptions.DuplicateTransferException;
import dev.slsk.exceptions.TransferNotFoundException;
import dev.slsk.exceptions.UserOfflineException;
import dev.slsk.internal.common.CommonUtils;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.NetworkExecutor;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.messages.PlaceInQueueRequest;
import dev.slsk.internal.messaging.messages.PlaceInQueueResponse;
import dev.slsk.internal.options.PositionableInputStream;
import dev.slsk.internal.options.PositionableOutputStream;
import dev.slsk.internal.options.TransferOptions;
import dev.slsk.internal.transfer.TransferInternal;
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
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongFunction;
import java.util.function.Supplier;

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

    final SoulseekEngine context;
    final ServerLink server;

    /** Global transfer concurrency limits; a transfer concern, so owned here. */
    final java.util.concurrent.Semaphore globalDownloadSemaphore;

    final java.util.concurrent.Semaphore globalUploadSemaphore;

    /** Per-user upload limits, and the lock guarding their creation. */
    final java.util.Map<String, java.util.concurrent.Semaphore> uploadSemaphores =
            new java.util.concurrent.ConcurrentHashMap<>();

    final java.util.concurrent.Semaphore uploadSemaphoreSyncRoot = new java.util.concurrent.Semaphore(1);

    /** Duplicate-transfer keys; owned here, since this is what detects duplicates. */
    final java.util.Map<String, Boolean> uniqueKeys = new java.util.concurrent.ConcurrentHashMap<>();

    TransferEngine(SoulseekEngine context, ServerLink server) {
        this.context = java.util.Objects.requireNonNull(context, "context");
        this.server = java.util.Objects.requireNonNull(server, "server");
        this.globalDownloadSemaphore =
                new java.util.concurrent.Semaphore(context.getClientOptions().getMaximumConcurrentDownloads());
        this.globalUploadSemaphore =
                new java.util.concurrent.Semaphore(context.getClientOptions().getMaximumConcurrentUploads());
    }

    /** Downloads a remote file to a local file. */
    /**
     * Downloads what the request describes.
     *
     * <p>The request object carries both shapes — to a file, or to a stream —
     * and choosing between them is a property of the request, not of the
     * caller. The client used to make that choice in a blocking wrapper on its
     * way here; the choice belongs with the engine that acts on it.
     *
     * @param request the download to perform
     * @return the completed transfer
     */
    Transfer download(DownloadRequest request) {
        java.util.Objects.requireNonNull(request, "request");
        try {
            return await(downloadOperation(request));
        } catch (Throwable failure) {
            throw Failures.surface(failure);
        }
    }

    private CompletableFuture<Transfer> downloadOperation(DownloadRequest request) {
        return request.isToStream()
                ? download(
                        request.getUsername(),
                        request.getRemoteFilename(),
                        request.getOutputStreamFactory(),
                        request.getSize(),
                        request.getStartOffset(),
                        request.getToken(),
                        request.getOptions(),
                        request.getOffer(),
                        request.getCancellationSignal())
                : download(
                        request.getUsername(),
                        request.getRemoteFilename(),
                        request.getLocalFilename(),
                        request.getSize(),
                        request.getStartOffset(),
                        request.getToken(),
                        request.getOptions(),
                        request.getCancellationSignal());
    }

    /**
     * Asks a peer to queue what the request describes.
     *
     * @param request the download to enqueue
     * @return an operation completing with the transfer's own completion
     */
    CompletableFuture<CompletableFuture<Transfer>> enqueueDownload(DownloadRequest request) {
        java.util.Objects.requireNonNull(request, "request");
        return request.isToStream()
                ? enqueueDownload(
                        request.getUsername(),
                        request.getRemoteFilename(),
                        request.getOutputStreamFactory(),
                        request.getSize(),
                        request.getStartOffset(),
                        request.getToken(),
                        request.getOptions(),
                        request.getCancellationSignal())
                : enqueueDownload(
                        request.getUsername(),
                        request.getRemoteFilename(),
                        request.getLocalFilename(),
                        request.getSize(),
                        request.getStartOffset(),
                        request.getToken(),
                        request.getOptions(),
                        request.getCancellationSignal());
    }

    /**
     * Uploads what the request describes.
     *
     * @param request the upload to perform
     * @return the completed transfer
     */
    Transfer upload(UploadRequest request) {
        java.util.Objects.requireNonNull(request, "request");
        try {
            return await(uploadOperation(request));
        } catch (Throwable failure) {
            throw Failures.surface(failure);
        }
    }

    private CompletableFuture<Transfer> uploadOperation(UploadRequest request) {
        return request.isFromStream()
                ? upload(
                        request.getUsername(),
                        request.getRemoteFilename(),
                        request.getSize(),
                        request.getInputStreamFactory(),
                        request.getToken(),
                        request.getOptions(),
                        request.getCancellationSignal())
                : upload(
                        request.getUsername(),
                        request.getRemoteFilename(),
                        request.getLocalFilename(),
                        request.getToken(),
                        request.getOptions(),
                        request.getCancellationSignal());
    }

    /**
     * Offers a peer what the request describes.
     *
     * @param request the upload to enqueue
     * @return an operation completing with the transfer's own completion
     */
    CompletableFuture<CompletableFuture<Transfer>> enqueueUpload(UploadRequest request) {
        java.util.Objects.requireNonNull(request, "request");
        return request.isFromStream()
                ? enqueueUpload(
                        request.getUsername(),
                        request.getRemoteFilename(),
                        request.getSize(),
                        request.getInputStreamFactory(),
                        request.getToken(),
                        request.getOptions(),
                        request.getCancellationSignal())
                : enqueueUpload(
                        request.getUsername(),
                        request.getRemoteFilename(),
                        request.getLocalFilename(),
                        request.getToken(),
                        request.getOptions(),
                        request.getCancellationSignal());
    }

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
        CommonUtils.requireText(requestedUsername, "username");
        CommonUtils.requireText(remoteFilename, "remoteFilename");
        CommonUtils.requireText(localFilename, "localFilename");
        validateDownloadRange(size, startOffset);
        server.requireLoggedIn("download files");
        int transferToken = token == null ? context.getTokenFactory().nextToken() : token;
        validateDownloadUniqueness(requestedUsername, remoteFilename, transferToken);
        TransferOptions options =
                (transferOptions == null ? new TransferOptions() : transferOptions).withDisposalOptions(null, true);
        return downloadToStreamAsync(
                requestedUsername,
                remoteFilename,
                () -> {
                    try {
                        return context.getIoAdapter().getOutputStream(localFilename, startOffset > 0);
                    } catch (IOException failure) {
                        throw new UncheckedIOException(failure);
                    }
                },
                size,
                startOffset,
                transferToken,
                options,
                CommonUtils.token(cancellationSignal));
    }
    /** Downloads data to a stream created by a factory. */
    CompletableFuture<Transfer> download(
            String requestedUsername, String remoteFilename, Supplier<OutputStream> outputStreamFactory) {
        return download(
                requestedUsername, remoteFilename, outputStreamFactory, null, 0, null, null, CancellationSignal.none());
    }
    /** Downloads stream data with an expected size. */
    CompletableFuture<Transfer> download(
            String requestedUsername, String remoteFilename, Supplier<OutputStream> outputStreamFactory, Long size) {
        return download(
                requestedUsername, remoteFilename, outputStreamFactory, size, 0, null, null, CancellationSignal.none());
    }
    /** Downloads stream data with cancellation. */
    CompletableFuture<Transfer> download(
            String requestedUsername,
            String remoteFilename,
            Supplier<OutputStream> outputStreamFactory,
            CancellationSignal cancellationSignal) {
        return download(
                requestedUsername, remoteFilename, outputStreamFactory, null, 0, null, null, cancellationSignal);
    }
    /** Downloads stream data from a resume offset. */
    CompletableFuture<Transfer> download(
            String requestedUsername,
            String remoteFilename,
            Supplier<OutputStream> outputStreamFactory,
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
            Supplier<OutputStream> outputStreamFactory,
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
            Supplier<OutputStream> outputStreamFactory,
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
            Supplier<OutputStream> outputStreamFactory,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        return download(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                token,
                transferOptions,
                null,
                cancellationSignal);
    }

    /** Downloads data to a stream, taking up a peer's standing offer if there is one. */
    CompletableFuture<Transfer> download(
            String requestedUsername,
            String remoteFilename,
            Supplier<OutputStream> outputStreamFactory,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions transferOptions,
            dev.slsk.internal.messaging.messages.TransferRequest offer,
            CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        CommonUtils.requireText(remoteFilename, "remoteFilename");
        validateDownloadRange(size, startOffset);
        Objects.requireNonNull(outputStreamFactory, "outputStreamFactory");
        server.requireLoggedIn("download files");
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
                offer,
                CommonUtils.token(cancellationSignal));
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
        CommonUtils.requireText(requestedUsername, "username");
        CommonUtils.requireText(remoteFilename, "remoteFilename");
        CommonUtils.requireText(localFilename, "localFilename");
        if (!context.getIoAdapter().exists(localFilename)) {
            throw new UncheckedIOException(
                    new FileNotFoundException("The local file does not exist: " + localFilename));
        }
        server.requireLoggedIn("upload files");
        try (InputStream ignored = context.getIoAdapter().getInputStream(localFilename)) {
            // Probe readability before allocating a transfer token.
        } catch (IOException failure) {
            throw new UncheckedIOException(
                    "The local file " + localFilename + " could not be opened for reading: "
                            + Failures.message(failure),
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
                        return context.getIoAdapter().getInputStream(localFilename);
                    } catch (IOException failure) {
                        throw new UncheckedIOException(failure);
                    }
                },
                transferToken,
                fileOptions,
                CommonUtils.token(cancellationSignal));
    }
    /** Uploads data supplied by an asynchronous stream factory. */
    CompletableFuture<Transfer> upload(
            String requestedUsername, String remoteFilename, long size, LongFunction<InputStream> inputStreamFactory) {
        return upload(
                requestedUsername, remoteFilename, size, inputStreamFactory, null, null, CancellationSignal.none());
    }
    /** Uploads stream data with a specific transfer token. */
    CompletableFuture<Transfer> upload(
            String requestedUsername,
            String remoteFilename,
            long size,
            LongFunction<InputStream> inputStreamFactory,
            Integer token) {
        return upload(
                requestedUsername, remoteFilename, size, inputStreamFactory, token, null, CancellationSignal.none());
    }
    /** Uploads stream data with cancellation. */
    CompletableFuture<Transfer> upload(
            String requestedUsername,
            String remoteFilename,
            long size,
            LongFunction<InputStream> inputStreamFactory,
            CancellationSignal cancellationSignal) {
        return upload(requestedUsername, remoteFilename, size, inputStreamFactory, null, null, cancellationSignal);
    }
    /** Uploads stream data using the supplied context.getClientOptions(). */
    CompletableFuture<Transfer> upload(
            String requestedUsername,
            String remoteFilename,
            long size,
            LongFunction<InputStream> inputStreamFactory,
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
            LongFunction<InputStream> inputStreamFactory,
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
            LongFunction<InputStream> inputStreamFactory,
            Integer token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        CommonUtils.requireText(remoteFilename, "remoteFilename");
        if (size < 0) {
            throw new IllegalArgumentException("size must be greater than or equal to zero");
        }
        Objects.requireNonNull(inputStreamFactory, "inputStreamFactory");
        server.requireLoggedIn("upload files");
        int transferToken = token == null ? context.getTokenFactory().nextToken() : token;
        validateUploadUniqueness(requestedUsername, remoteFilename, transferToken);
        return uploadFromStreamAsync(
                requestedUsername,
                remoteFilename,
                size,
                inputStreamFactory,
                transferToken,
                transferOptions == null ? new TransferOptions() : transferOptions,
                CommonUtils.token(cancellationSignal));
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
            String requestedUsername, String remoteFilename, Supplier<OutputStream> outputStreamFactory) {
        return enqueueDownload(
                requestedUsername, remoteFilename, outputStreamFactory, null, 0, null, null, CancellationSignal.none());
    }
    /** Enqueues a stream-factory download with an expected size. */
    CompletableFuture<CompletableFuture<Transfer>> enqueueDownload(
            String requestedUsername, String remoteFilename, Supplier<OutputStream> outputStreamFactory, Long size) {
        return enqueueDownload(
                requestedUsername, remoteFilename, outputStreamFactory, size, 0, null, null, CancellationSignal.none());
    }
    /** Enqueues a stream-factory download from a resume offset. */
    CompletableFuture<CompletableFuture<Transfer>> enqueueDownload(
            String requestedUsername,
            String remoteFilename,
            Supplier<OutputStream> outputStreamFactory,
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
            Supplier<OutputStream> outputStreamFactory,
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
            Supplier<OutputStream> outputStreamFactory,
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
            Supplier<OutputStream> outputStreamFactory,
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
            String requestedUsername, String remoteFilename, long size, LongFunction<InputStream> inputStreamFactory) {
        return enqueueUpload(
                requestedUsername, remoteFilename, size, inputStreamFactory, null, null, CancellationSignal.none());
    }
    /** Enqueues a stream-factory upload with a specific token. */
    CompletableFuture<CompletableFuture<Transfer>> enqueueUpload(
            String requestedUsername,
            String remoteFilename,
            long size,
            LongFunction<InputStream> inputStreamFactory,
            Integer token) {
        return enqueueUpload(
                requestedUsername, remoteFilename, size, inputStreamFactory, token, null, CancellationSignal.none());
    }
    /** Enqueues a stream-factory upload with cancellation. */
    CompletableFuture<CompletableFuture<Transfer>> enqueueUpload(
            String requestedUsername,
            String remoteFilename,
            long size,
            LongFunction<InputStream> inputStreamFactory,
            CancellationSignal cancellationSignal) {
        return enqueueUpload(
                requestedUsername, remoteFilename, size, inputStreamFactory, null, null, cancellationSignal);
    }
    /** Enqueues a stream-factory upload using supplied context.getClientOptions(). */
    CompletableFuture<CompletableFuture<Transfer>> enqueueUpload(
            String requestedUsername,
            String remoteFilename,
            long size,
            LongFunction<InputStream> inputStreamFactory,
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
            LongFunction<InputStream> inputStreamFactory,
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

    Integer getDownloadPlaceInQueue(String requestedUsername, String filename) {
        return getDownloadPlaceInQueue(requestedUsername, filename, CancellationSignal.none());
    }

    Integer getDownloadPlaceInQueue(String requestedUsername, String filename, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        CommonUtils.requireText(filename, "filename");
        server.requireLoggedIn("check download queue position");
        boolean active = context.getDownloadRegistry().values().stream()
                .anyMatch(download -> Objects.equals(download.getUsername(), requestedUsername)
                        && Objects.equals(download.getFilename(), filename));
        if (!active) {
            throw new TransferNotFoundException(
                    "A download of " + filename + " from user " + requestedUsername + " is not active");
        }
        CancellationSignal token = CommonUtils.token(cancellationSignal);
        try {
            Wait<PlaceInQueueResponse> responseWait = context.getWaiter()
                    .register(
                            new WaitKey(MessageCode.Peer.PLACE_IN_QUEUE_RESPONSE, requestedUsername, filename),
                            PlaceInQueueResponse.class,
                            null,
                            token);
            java.net.InetSocketAddress endpoint = context.resolveUserEndpoint(requestedUsername, token);
            dev.slsk.internal.network.MessageConnection connection =
                    context.getPeerConnectionManager().getOrAddMessageConnection(requestedUsername, endpoint, token);
            connection.write(new PlaceInQueueRequest(filename), CommonUtils.token(token));
            return responseWait.await().getPlaceInQueue();
        } catch (Throwable failure) {
            throw Failures.raise(
                    failure,
                    "Failed to fetch place in queue for download of " + filename + " from " + requestedUsername + ": ",
                    UserOfflineException.class);
        }
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
            Supplier<OutputStream> outputStreamFactory,
            Long size,
            long startOffset,
            int token,
            TransferOptions transferOptions,
            CancellationSignal cancellationSignal) {
        return downloadToStreamAsync(
                requestedUsername,
                remoteFilename,
                outputStreamFactory,
                size,
                startOffset,
                token,
                transferOptions,
                null,
                cancellationSignal);
    }

    CompletableFuture<Transfer> downloadToStreamAsync(
            String requestedUsername,
            String remoteFilename,
            Supplier<OutputStream> outputStreamFactory,
            Long size,
            long startOffset,
            int token,
            TransferOptions transferOptions,
            dev.slsk.internal.messaging.messages.TransferRequest offer,
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

        DownloadOperation operation = new DownloadOperation(
                this, download, outputStreamFactory, operationOptions, offer, cancellationSignal, uniqueKey);
        return NetworkExecutor.supplyAsync(operation::execute);
    }

    CompletableFuture<Transfer> uploadFromStreamAsync(
            String requestedUsername,
            String remoteFilename,
            long size,
            LongFunction<InputStream> inputStreamFactory,
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
                new UploadOperation(this, upload, inputStreamFactory, operationOptions, cancellationSignal, uniqueKey);
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

    static final class PositionTrackingInputStream extends FilterInputStream {
        private long position;

        PositionTrackingInputStream(InputStream inputStream, long initialPosition) {
            super(inputStream);
            position = initialPosition;
        }

        long getPosition() {
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

    static final class PositionTrackingOutputStream extends FilterOutputStream {
        private long position;

        PositionTrackingOutputStream(OutputStream outputStream, long initialPosition) {
            super(outputStream);
            position = initialPosition;
        }

        long getPosition() {
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
    static <T> T await(java.util.concurrent.CompletableFuture<T> future) {
        try {
            return future.join();
        } catch (Throwable failure) {
            throw new java.util.concurrent.CompletionException(Failures.unwrap(failure));
        }
    }

    static long determineOutputPosition(OutputStream stream, long fallback) throws IOException {
        if (stream instanceof PositionableOutputStream positionable) {
            return positionable.getPosition();
        }
        if (stream instanceof FileOutputStream fileOutputStream) {
            return fileOutputStream.getChannel().position();
        }
        return fallback;
    }

    static long determinePosition(InputStream stream, long fallback) throws IOException {
        if (stream instanceof PositionableInputStream positionable) {
            return positionable.getPosition();
        }
        if (stream instanceof FileInputStream fileInputStream) {
            return fileInputStream.getChannel().position();
        }
        return fallback;
    }

    static String filenameOnly(String filename) {
        try {
            Path path = Path.of(filename);
            Path leaf = path.getFileName();
            return leaf == null ? filename : leaf.toString();
        } catch (Throwable ignored) {
            return filename;
        }
    }

    static boolean isQueuedResponse(String message) {
        int end = message.length();
        while (end > 0 && message.charAt(end - 1) == '.') {
            end--;
        }
        return message.substring(0, end).equalsIgnoreCase("Queued");
    }

    static void seekInputStream(InputStream stream, long position) throws IOException {
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

    static void seekOutputStream(OutputStream stream, long position) throws IOException {
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

    static void skipFully(InputStream stream, long count) throws IOException {
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
