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

    /**
     * Downloads what the request describes.
     *
     * <p>The request object carries both shapes — to a file, or to a stream —
     * and choosing between them is a property of the request, not of the
     * caller. What used to sit between this and the two bodies below was
     * fourteen overloads expressing the cross product of two destinations and
     * five optional arguments; that is C# default parameters written out as
     * Java source, and the request object already says all of it.
     *
     * @param request the download to perform
     * @return the completed transfer
     */
    Transfer download(DownloadRequest request) {
        Objects.requireNonNull(request, "request");
        try {
            return await(request.isToStream() ? downloadToStream(request) : downloadToFile(request));
        } catch (Throwable failure) {
            throw Failures.surface(failure);
        }
    }

    /**
     * Uploads what the request describes.
     *
     * <p>The counterpart to {@link #download(DownloadRequest)}; see there.
     *
     * @param request the upload to perform
     * @return the completed transfer
     */
    Transfer upload(UploadRequest request) {
        Objects.requireNonNull(request, "request");
        try {
            return await(request.isFromStream() ? uploadFromStream(request) : uploadFromFile(request));
        } catch (Throwable failure) {
            throw Failures.surface(failure);
        }
    }

    /** Downloads to a local path, opening it as the destination stream. */
    private CompletableFuture<Transfer> downloadToFile(DownloadRequest request) {
        String requestedUsername = request.getUsername();
        String remoteFilename = request.getRemoteFilename();
        String localFilename = request.getLocalFilename();
        long startOffset = request.getStartOffset();
        CommonUtils.requireText(requestedUsername, "username");
        CommonUtils.requireText(remoteFilename, "remoteFilename");
        CommonUtils.requireText(localFilename, "localFilename");
        validateDownloadRange(request.getSize(), startOffset);
        server.requireLoggedIn("download files");
        int transferToken =
                request.getToken() == null ? context.getTokenFactory().nextToken() : request.getToken();
        validateDownloadUniqueness(requestedUsername, remoteFilename, transferToken);
        // A stream this opened is a stream this closes, whatever the request
        // said about a stream it did not open.
        TransferOptions options = (request.getOptions() == null ? new TransferOptions() : request.getOptions())
                .withDisposalOptions(null, true);
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
                request.getSize(),
                startOffset,
                transferToken,
                options,
                request.getOffer(),
                CommonUtils.token(request.getCancellationSignal()));
    }

    /** Downloads to a caller-supplied stream. */
    private CompletableFuture<Transfer> downloadToStream(DownloadRequest request) {
        String requestedUsername = request.getUsername();
        String remoteFilename = request.getRemoteFilename();
        CommonUtils.requireText(requestedUsername, "username");
        CommonUtils.requireText(remoteFilename, "remoteFilename");
        validateDownloadRange(request.getSize(), request.getStartOffset());
        Objects.requireNonNull(request.getOutputStreamFactory(), "outputStreamFactory");
        server.requireLoggedIn("download files");
        int transferToken =
                request.getToken() == null ? context.getTokenFactory().nextToken() : request.getToken();
        validateDownloadUniqueness(requestedUsername, remoteFilename, transferToken);
        return downloadToStreamAsync(
                requestedUsername,
                remoteFilename,
                request.getOutputStreamFactory(),
                request.getSize(),
                request.getStartOffset(),
                transferToken,
                request.getOptions() == null ? new TransferOptions() : request.getOptions(),
                request.getOffer(),
                CommonUtils.token(request.getCancellationSignal()));
    }

    /** Uploads a local path, opening it as the source stream. */
    private CompletableFuture<Transfer> uploadFromFile(UploadRequest request) {
        String requestedUsername = request.getUsername();
        String remoteFilename = request.getRemoteFilename();
        String localFilename = request.getLocalFilename();
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

        int transferToken =
                request.getToken() == null ? context.getTokenFactory().nextToken() : request.getToken();
        validateUploadUniqueness(requestedUsername, remoteFilename, transferToken);
        TransferOptions fileOptions = (request.getOptions() == null ? new TransferOptions() : request.getOptions())
                .withDisposalOptions(true, null);
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
                CommonUtils.token(request.getCancellationSignal()));
    }

    /** Uploads from a caller-supplied stream. */
    private CompletableFuture<Transfer> uploadFromStream(UploadRequest request) {
        String requestedUsername = request.getUsername();
        String remoteFilename = request.getRemoteFilename();
        CommonUtils.requireText(requestedUsername, "username");
        CommonUtils.requireText(remoteFilename, "remoteFilename");
        if (request.getSize() < 0) {
            throw new IllegalArgumentException("size must be greater than or equal to zero");
        }
        Objects.requireNonNull(request.getInputStreamFactory(), "inputStreamFactory");
        server.requireLoggedIn("upload files");
        int transferToken =
                request.getToken() == null ? context.getTokenFactory().nextToken() : request.getToken();
        validateUploadUniqueness(requestedUsername, remoteFilename, transferToken);
        return uploadFromStreamAsync(
                requestedUsername,
                remoteFilename,
                request.getSize(),
                request.getInputStreamFactory(),
                transferToken,
                request.getOptions() == null ? new TransferOptions() : request.getOptions(),
                CommonUtils.token(request.getCancellationSignal()));
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
