// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.CancellationSignal;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.DuplicateTokenException;
import dev.slsk.exceptions.DuplicateTransferException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.exceptions.TransferReportedFailedException;
import dev.slsk.exceptions.TransferSizeMismatchException;
import dev.slsk.exceptions.TransferStreamException;
import dev.slsk.internal.common.Outcomes;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.messaging.messages.OutgoingMessage;
import dev.slsk.internal.messaging.messages.QueueDownloadRequest;
import dev.slsk.internal.messaging.messages.TransferRequest;
import dev.slsk.internal.messaging.messages.TransferResponse;
import dev.slsk.internal.messaging.messages.UserAddressResponse;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.PeerConnectionManager;
import dev.slsk.internal.network.tcp.Connection;
import dev.slsk.internal.network.tcp.ConnectionDataEvent;
import dev.slsk.internal.network.tcp.ConnectionDisconnectedEvent;
import dev.slsk.internal.network.tcp.ConnectionEventListener;
import dev.slsk.internal.network.tcp.ConnectionGovernor;
import dev.slsk.internal.network.tcp.ConnectionReporter;
import dev.slsk.internal.network.tcp.ConnectionState;
import dev.slsk.internal.options.PositionableOutputStream;
import dev.slsk.internal.options.TransferOptions;
import dev.slsk.internal.transfer.TransferInternal;
import dev.slsk.transfer.RejectionReason;
import dev.slsk.transfer.TransferOutcome;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class EngineDownloadTest {
    private static final InetSocketAddress ENDPOINT = new InetSocketAddress(InetAddress.getLoopbackAddress(), 46012);

    @Test
    void validatesArgumentsRangeFactoryAndLoginState() {
        try (Fixture fixture = new Fixture()) {
            for (String bad : new String[] {null, "", " ", "\t"}) {
                assertThrows(
                        IllegalArgumentException.class,
                        () -> fixture.client
                                .transfers()
                                .download(DownloadRequest.toStream(bad, "file", outputFactory())
                                        .build()));
                assertThrows(
                        IllegalArgumentException.class,
                        () -> fixture.client
                                .transfers()
                                .download(DownloadRequest.toStream("alice", bad, outputFactory())
                                        .build()));
                assertThrows(
                        IllegalArgumentException.class,
                        () -> fixture.client
                                .transfers()
                                .download(DownloadRequest.toFile("alice", "file", bad)
                                        .build()));
            }
            assertThrows(
                    IllegalArgumentException.class,
                    () -> fixture.client
                            .transfers()
                            .download(DownloadRequest.toStream("alice", "file", outputFactory())
                                    .size(-1L)
                                    .build()));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> fixture.client
                            .transfers()
                            .download(DownloadRequest.toStream("alice", "file", outputFactory())
                                    .size(1L)
                                    .startOffset(-1)
                                    .build()));
            assertThrows(
                    NullPointerException.class,
                    () -> fixture.client
                            .transfers()
                            .download(DownloadRequest.toStream("alice", "file", outputFactory())
                                    .startOffset(1)
                                    .build()));
            assertThrows(
                    NullPointerException.class,
                    () -> fixture.client
                            .transfers()
                            .download(DownloadRequest.toStream(
                                            "alice", "file", (java.util.function.Supplier<java.io.OutputStream>) null)
                                    .build()));

            fixture.client.setStateForTest(SoulseekClientState.DISCONNECTED);
            assertThrows(
                    IllegalStateException.class,
                    () -> fixture.client
                            .transfers()
                            .download(DownloadRequest.toStream("alice", "file", outputFactory())
                                    .build()));
        }
    }

    @Test
    void rejectsDuplicateTokensTransfersAndUniqueKeys() {
        try (Fixture fixture = new Fixture()) {
            fixture.client.getDownloadRegistry().put(1, transfer(TransferDirection.DOWNLOAD, "other", "other", 1));
            assertThrows(
                    DuplicateTokenException.class,
                    () -> fixture.client
                            .transfers()
                            .download(DownloadRequest.toStream("alice", "file", outputFactory())
                                    .size(1L)
                                    .token(1)
                                    .build()));

            fixture.client.getDownloadRegistry().clear();
            fixture.client.getUploadRegistry().put(2, transfer(TransferDirection.UPLOAD, "other", "other", 2));
            assertThrows(
                    DuplicateTokenException.class,
                    () -> fixture.client
                            .transfers()
                            .download(DownloadRequest.toStream("alice", "file", outputFactory())
                                    .size(1L)
                                    .token(2)
                                    .build()));

            fixture.client.getUploadRegistry().clear();
            fixture.client.getDownloadRegistry().put(3, transfer(TransferDirection.DOWNLOAD, "alice", "file", 3));
            assertThrows(
                    DuplicateTransferException.class,
                    () -> fixture.client
                            .transfers()
                            .download(DownloadRequest.toStream("alice", "file", outputFactory())
                                    .size(1L)
                                    .token(4)
                                    .build()));

            fixture.client.getDownloadRegistry().clear();
            fixture.client.getUniqueKeys().put("Download:alice:file", true);
            assertThrows(
                    DuplicateTransferException.class,
                    () -> fixture.client
                            .transfers()
                            .download(DownloadRequest.toStream("alice", "file", outputFactory())
                                    .size(1L)
                                    .token(4)
                                    .build()));
        }
    }

    /**
     * The ask is a {@code QueueUpload}, and it carries nothing but the filename.
     *
     * <p>It used to be a download-direction {@code TransferRequest} carrying a
     * token and a size the peer had no use for, which every modern client
     * answers by queueing anyway. What the wire sees now is what Nicotine+,
     * Museek+ and the official clients send, and — because there is no
     * acknowledgement to wait for — the whole of what a queued download costs.
     */
    @Test
    void theAskIsAQueueUploadAndTheStateOrderIsUnchanged() {
        try (Fixture fixture = new Fixture()) {
            byte[] bytes = new byte[] {1, 2, 3, 4};
            fixture.transfer.data = bytes;
            fixture.waiter.startRequest = CompletableFuture.completedFuture(
                    new TransferRequest(TransferDirection.UPLOAD, 11, "file", bytes.length));
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            Timeline timeline = new Timeline();

            TransferOutcome outcome = fixture.client
                    .transfers()
                    .download(DownloadRequest.toStream("alice", "remote\\file", () -> output)
                            .token(11)
                            .options(timeline.on(options()))
                            .build());

            Transfer result = timeline.last();
            assertEquals(new TransferOutcome.Succeeded(bytes.length, result.getElapsedTime()), outcome);
            assertEquals(bytes.length, result.getSize());
            assertEquals(bytes.length, result.getBytesTransferred());
            assertArrayEquals(bytes, output.toByteArray());
            assertArrayEquals(new byte[8], fixture.transfer.offsetBytes);
            QueueDownloadRequest queued = assertInstanceOf(QueueDownloadRequest.class, fixture.message.messages.get(0));
            assertEquals("remote\\file", queued.getFilename());
            assertEquals(
                    List.of(
                            TransferState.QUEUED.or(TransferState.LOCALLY),
                            TransferState.REQUESTED,
                            TransferState.QUEUED.or(TransferState.REMOTELY),
                            TransferState.INITIALIZING,
                            TransferState.IN_PROGRESS,
                            TransferState.COMPLETED.or(TransferState.SUCCEEDED)),
                    timeline.states());
            assertTrue(timeline.progress.contains(0L));
            assertTrue(timeline.progress.contains((long) bytes.length));
            assertFalse(fixture.client.getDownloadRegistry().containsKey(11));
            assertFalse(fixture.client.getUniqueKeys().containsKey("Download:alice:remote\\file"));
        }
    }

    @Test
    void expectedSizeMismatchIsPreservedAndAborted() {
        try (Fixture fixture = new Fixture()) {
            fixture.waiter.startRequest =
                    CompletableFuture.completedFuture(new TransferRequest(TransferDirection.UPLOAD, 12, "file", 5));
            Timeline timeline = new Timeline();

            TransferOutcome outcome = fixture.client
                    .transfers()
                    .download(DownloadRequest.toStream("alice", "file", outputFactory())
                            .size(4L)
                            .token(12)
                            .options(timeline.on(options()))
                            .build());

            TransferSizeMismatchException mismatch =
                    assertInstanceOf(TransferSizeMismatchException.class, causeOf(outcome));
            assertEquals(4, mismatch.getLocalSize());
            assertEquals(5, mismatch.getRemoteSize());
            assertTrue(timeline.terminal().getState().contains(TransferState.ABORTED));
            assertSame(mismatch, timeline.terminal().getException());
        }
    }

    @Test
    void queuedSizeMismatchIsPreservedAndAborted() {
        try (Fixture fixture = new Fixture()) {
            fixture.waiter.startRequest =
                    CompletableFuture.completedFuture(new TransferRequest(TransferDirection.UPLOAD, 130, "file", 6));
            Timeline timeline = new Timeline();

            TransferOutcome outcome = fixture.client
                    .transfers()
                    .download(DownloadRequest.toStream("alice", "file", outputFactory())
                            .size(5L)
                            .token(30)
                            .options(timeline.on(options()))
                            .build());

            TransferSizeMismatchException mismatch =
                    assertInstanceOf(TransferSizeMismatchException.class, causeOf(outcome));
            assertEquals(5, mismatch.getLocalSize());
            assertEquals(6, mismatch.getRemoteSize());
            assertTrue(timeline.terminal().getState().contains(TransferState.ABORTED));
            assertSame(mismatch, timeline.terminal().getException());
        }
    }

    /**
     * A refusal is out of band now. {@code QueueUpload} is not acknowledged when
     * it succeeds, so there is no response to carry a reason; the peer says no
     * with an {@code UploadDenied}, which the handler turns into a failure of
     * the wait for its offer. The peer's own words reach the caller with nothing
     * in between to reword them — the {@code "Transfer rejected: "} prefix went
     * with the acknowledgement that used to be parsed here.
     */
    @Test
    void aDenialArrivesAsAFailedOfferAndKeepsThePeersWords() {
        try (Fixture fixture = new Fixture()) {
            fixture.waiter.startRequest =
                    CompletableFuture.failedFuture(new TransferRejectedException("File not shared."));

            TransferOutcome outcome = fixture.client
                    .transfers()
                    .download(DownloadRequest.toStream("alice", "file", outputFactory())
                            .size(1L)
                            .token(13)
                            .options(options())
                            .build());

            TransferOutcome.Rejected rejected = assertInstanceOf(TransferOutcome.Rejected.class, outcome);
            assertEquals("File not shared.", rejected.rawMessage());
            assertEquals(RejectionReason.FILE_NOT_SHARED, rejected.reason());
        }
    }

    /**
     * The two refusals that stop being true on their own. A peer whose queue is
     * full for us has not looked at the file, so the reason is classified apart
     * from a denial about the file itself and the queue above waits it out
     * rather than giving up. See {@code DownloadQueue.holdForQueueLimit}.
     */
    @Test
    void aFullQueueIsClassifiedApartFromADenial() {
        try (Fixture fixture = new Fixture()) {
            fixture.waiter.startRequest =
                    CompletableFuture.failedFuture(new TransferRejectedException("Too many files"));

            TransferOutcome outcome = fixture.client
                    .transfers()
                    .download(DownloadRequest.toStream("alice", "file", outputFactory())
                            .size(1L)
                            .token(33)
                            .options(options())
                            .build());

            assertEquals(
                    RejectionReason.TOO_MANY_FILES,
                    assertInstanceOf(TransferOutcome.Rejected.class, outcome).reason());
        }
    }

    @Test
    void queuedDownloadRespondsAndUsesIncomingConnection() {
        try (Fixture fixture = new Fixture()) {
            byte[] bytes = new byte[] {7, 8, 9};
            fixture.transfer.data = bytes;
            fixture.waiter.startRequest = CompletableFuture.completedFuture(
                    new TransferRequest(TransferDirection.UPLOAD, 99, "file", bytes.length));
            ByteArrayOutputStream output = new ByteArrayOutputStream();

            Timeline timeline = new Timeline();

            fixture.client
                    .transfers()
                    .download(DownloadRequest.toStream("alice", "file", () -> output)
                            .token(14)
                            .options(timeline.on(options()))
                            .build());

            Transfer result = timeline.last();
            assertEquals(99, result.getRemoteToken());
            assertEquals(bytes.length, result.getSize());
            assertEquals(1, fixture.peerManager.awaitCalls);
            assertEquals(0, fixture.peerManager.outgoingTransferCalls);
            TransferResponse response = assertInstanceOf(TransferResponse.class, fixture.message.messages.get(1));
            assertEquals(99, response.getToken());
            assertEquals(bytes.length, response.getFileSize());
            assertArrayEquals(bytes, output.toByteArray());
        }
    }

    @Test
    void queuedDownloadFallsBackToOutgoingConnection() {
        try (Fixture fixture = new Fixture()) {
            fixture.transfer.data = new byte[] {1};
            fixture.waiter.startRequest =
                    CompletableFuture.completedFuture(new TransferRequest(TransferDirection.UPLOAD, 100, "file", 1));
            fixture.peerManager.awaitResult =
                    CompletableFuture.failedFuture(new ConnectionException("peer did not connect"));

            TransferOutcome outcome = fixture.client
                    .transfers()
                    .download(DownloadRequest.toStream("alice", "file", outputFactory())
                            .size(1L)
                            .token(15)
                            .options(options())
                            .build());

            assertInstanceOf(TransferOutcome.Succeeded.class, outcome);
            assertEquals(1, fixture.peerManager.awaitCalls);
            assertEquals(1, fixture.peerManager.outgoingTransferCalls);
            assertEquals(100, fixture.peerManager.outgoingToken);
        }
    }

    /**
     * Replaces {@code enqueueReturnsOnRemoteQueueBeforePeerStartsTransfer},
     * deleted with the {@code enqueue*} overloads under D14.
     *
     * <p>What that test observed was not enqueueing: it was that a download the
     * peer has queued sits there until the peer says it is ready, and that the
     * peer's own transfer request is the only thing that moves it. The
     * two-phase return the {@code enqueue*} shape existed to express is now the
     * caller's choice of thread, so the assertion is made from a caller who
     * made it.
     */
    @Test
    void aRemotelyQueuedDownloadWaitsForThePeerToStartTheTransfer() {
        try (Fixture fixture = new Fixture()) {
            fixture.waiter.startRequest = new CompletableFuture<>();

            CompletableFuture<TransferOutcome> download = inBackground(() -> fixture.client
                    .transfers()
                    .download(DownloadRequest.toStream("alice", "file", outputFactory())
                            .token(16)
                            .options(options())
                            .build()));

            assertThrows(
                    TimeoutException.class,
                    () -> download.get(250, TimeUnit.MILLISECONDS),
                    "a remotely queued download must not finish before the peer starts the transfer");
            fixture.transfer.data = new byte[] {1, 2};
            fixture.waiter.startRequest.complete(new TransferRequest(TransferDirection.UPLOAD, 101, "file", 2));
            assertInstanceOf(TransferOutcome.Succeeded.class, download.join());
        }
    }

    /**
     * Replaces {@code enqueuePropagatesFailureBeforeRemoteQueueing}, deleted
     * with the {@code enqueue*} overloads under D14. The surviving behaviour is
     * that a peer's refusal reaches the caller with nothing between them to
     * rewrap it — as the instance on the transfer, and as the peer's own words
     * on the outcome.
     */
    @Test
    void aRefusalBeforeRemoteQueueingReachesTheCallerUnchanged() {
        try (Fixture fixture = new Fixture()) {
            TransferRejectedException rejection = new TransferRejectedException("rejected");
            fixture.waiter.startRequest = CompletableFuture.failedFuture(rejection);
            Timeline timeline = new Timeline();

            TransferOutcome outcome = fixture.client
                    .transfers()
                    .download(DownloadRequest.toStream("alice", "file", outputFactory())
                            .size(1L)
                            .token(17)
                            .options(timeline.on(options()))
                            .build());

            assertEquals(
                    "rejected",
                    assertInstanceOf(TransferOutcome.Rejected.class, outcome).rawMessage());
            assertSame(rejection, timeline.terminal().getException());
        }
    }

    @Test
    void resumeSeeksOutputWritesOffsetAndReadsRemainingBytes() {
        try (Fixture fixture = new Fixture()) {
            fixture.transfer.data = new byte[] {3, 4, 5};
            fixture.waiter.startRequest =
                    CompletableFuture.completedFuture(new TransferRequest(TransferDirection.UPLOAD, 18, "file", 5));
            PositionableBuffer output = new PositionableBuffer(new byte[] {1, 2});

            Timeline timeline = new Timeline();

            fixture.client
                    .transfers()
                    .download(DownloadRequest.toStream("alice", "file", () -> output)
                            .size(5L)
                            .startOffset(2)
                            .token(18)
                            .options(timeline.on(options()))
                            .build());

            Transfer result = timeline.last();
            assertArrayEquals(
                    ByteBuffer.allocate(8)
                            .order(ByteOrder.LITTLE_ENDIAN)
                            .putLong(2)
                            .array(),
                    fixture.transfer.offsetBytes);
            assertEquals(3, fixture.transfer.readLength);
            assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, output.toByteArray());
            assertEquals(2, result.getStartOffset());
            assertEquals(5, result.getBytesTransferred());
        }
    }

    @Test
    void nonSeekableResumeFailsUnlessAutomaticSeekingDisabled() {
        try (Fixture fixture = new Fixture()) {
            fixture.transfer.data = new byte[] {2};
            fixture.waiter.startRequest =
                    CompletableFuture.completedFuture(new TransferRequest(TransferDirection.UPLOAD, 19, "file", 2));
            TransferOutcome seekFailed = fixture.client
                    .transfers()
                    .download(DownloadRequest.toStream("alice", "file", outputFactory())
                            .size(2L)
                            .startOffset(1)
                            .token(19)
                            .options(options())
                            .build());
            assertInstanceOf(TransferStreamException.class, causeOf(seekFailed));

            TransferOptions noSeek = new TransferOptions(null, null, null, null, 3_000, true, false);
            fixture.transfer.data = new byte[] {2};
            Timeline timeline = new Timeline();
            fixture.client
                    .transfers()
                    .download(DownloadRequest.toStream("alice", "other", outputFactory())
                            .size(2L)
                            .startOffset(1)
                            .token(20)
                            .options(timeline.on(noSeek))
                            .build());
            assertEquals(
                    1,
                    timeline.last().getBytesTransferred(),
                    "final progress follows stream position when seek is bypassed");
        }
    }

    /**
     * The governor is gone: a pluggable per-transfer byte grant sat in front of
     * the token bucket, every implementation granted everything, and the bucket
     * already does that when the rate is unlimited. What remains is what the
     * grant was ever observed through — the reporter and the progress callback.
     */
    @Test
    void reporterAndProgressCallbacksAreInvoked() {
        try (Fixture fixture = new Fixture()) {
            fixture.transfer.data = new byte[] {1, 2, 3, 4, 5};
            fixture.transfer.maximumActualPerIteration = 2;
            fixture.waiter.startRequest =
                    CompletableFuture.completedFuture(new TransferRequest(TransferDirection.UPLOAD, 21, "file", 5));
            List<Integer> grants = new ArrayList<>();
            List<List<Integer>> reports = new ArrayList<>();
            Timeline timeline = new Timeline();
            TransferOptions options = new TransferOptions(null, null, null, (transfer, attempted, granted, actual) -> {
                grants.add(granted);
                reports.add(List.of(attempted, granted, actual));
            });

            fixture.client
                    .transfers()
                    .download(DownloadRequest.toStream("alice", "file", outputFactory())
                            .size(5L)
                            .token(21)
                            .options(timeline.on(options))
                            .build());

            assertFalse(grants.isEmpty());
            assertFalse(reports.isEmpty());
            assertTrue(reports.stream().anyMatch(report -> report.get(1) > report.get(2)));
            assertTrue(timeline.progress.contains(5L));
        }
    }

    @Test
    void remoteFailureRaceIsWrappedAndRecorded() {
        try (Fixture fixture = new Fixture()) {
            fixture.waiter.startRequest =
                    CompletableFuture.completedFuture(new TransferRequest(TransferDirection.UPLOAD, 22, "file", 1));
            fixture.transfer.readHook = () -> fixture.client
                    .getDownloadRegistry()
                    .get(22)
                    .settlement()
                    .fail(new TransferReportedFailedException("remote failed"));
            fixture.transfer.blockRead = true;
            Timeline timeline = new Timeline();

            TransferOutcome outcome = fixture.client
                    .transfers()
                    .download(DownloadRequest.toStream("alice", "file", outputFactory())
                            .size(1L)
                            .token(22)
                            .options(timeline.on(options()))
                            .build());

            // The peer's own failure, not a SoulseekClientException wrapping it.
            // The wrapper existed for a caller that caught exceptions; the queue
            // above this reads an outcome.
            Throwable cause = causeOf(outcome);
            assertInstanceOf(TransferReportedFailedException.class, cause);
            assertSame(cause, timeline.terminal().getException());
            assertTrue(timeline.terminal().getState().contains(TransferState.ERRORED));
        }
    }

    @Test
    void remoteDenialDuringReadIsPreservedAndRejected() {
        try (Fixture fixture = new Fixture()) {
            fixture.waiter.startRequest =
                    CompletableFuture.completedFuture(new TransferRequest(TransferDirection.UPLOAD, 31, "file", 1));
            TransferRejectedException rejection = new TransferRejectedException("download denied");
            fixture.transfer.readHook = () ->
                    fixture.client.getDownloadRegistry().get(31).settlement().fail(rejection);
            fixture.transfer.blockRead = true;
            Timeline timeline = new Timeline();

            TransferOutcome outcome = fixture.client
                    .transfers()
                    .download(DownloadRequest.toStream("alice", "file", outputFactory())
                            .size(1L)
                            .token(31)
                            .options(timeline.on(options()))
                            .build());

            assertEquals(
                    "download denied",
                    assertInstanceOf(TransferOutcome.Rejected.class, outcome).rawMessage());
            assertSame(rejection, timeline.terminal().getException());
            assertTrue(timeline.terminal().getState().contains(TransferState.REJECTED));
        }
    }

    @Test
    void unexpectedDisconnectIsWrappedAsConnectionFailure() {
        try (Fixture fixture = new Fixture()) {
            IOException socketFailure = new IOException("socket failed");
            fixture.waiter.startRequest =
                    CompletableFuture.completedFuture(new TransferRequest(TransferDirection.UPLOAD, 23, "file", 1));
            fixture.transfer.disconnectOnRead = socketFailure;

            TransferOutcome outcome = fixture.client
                    .transfers()
                    .download(DownloadRequest.toStream("alice", "file", outputFactory())
                            .size(1L)
                            .token(23)
                            .options(options())
                            .build());

            ConnectionException connection = assertInstanceOf(ConnectionException.class, causeOf(outcome));
            assertSame(socketFailure, connection.getCause());
        }
    }

    @Test
    void timeoutAndCancellationArePreserved() {
        try (Fixture timeoutFixture = new Fixture();
                Fixture cancellationFixture = new Fixture()) {
            TimeoutException timeout = new TimeoutException("timed out");
            timeoutFixture.waiter.startRequest = CompletableFuture.failedFuture(timeout);
            Timeline timedOut = new Timeline();

            TransferOutcome lapsed = timeoutFixture
                    .client
                    .transfers()
                    .download(DownloadRequest.toStream("alice", "file", outputFactory())
                            .size(1L)
                            .token(24)
                            .options(timedOut.on(options()))
                            .build());

            // The deadline itself, rather than the NoResponseException the old
            // blocking wrapper renamed it to on the way out.
            assertSame(timeout, causeOf(lapsed));
            assertTrue(timedOut.terminal().getState().contains(TransferState.TIMED_OUT));

            CancellationException cancellation = new CancellationException("cancelled");
            cancellationFixture.waiter.startRequest = CompletableFuture.failedFuture(cancellation);
            Timeline cancelled = new Timeline();

            TransferOutcome stopped = cancellationFixture
                    .client
                    .transfers()
                    .download(DownloadRequest.toStream("alice", "file", outputFactory())
                            .size(1L)
                            .token(25)
                            .options(cancelled.on(options()))
                            .build());

            assertInstanceOf(TransferOutcome.Cancelled.class, stopped);
            assertTrue(cancelled.terminal().getState().contains(TransferState.CANCELLED));
        }
    }

    @Test
    void fileDownloadCreatesThenAppendsForResume() throws IOException {
        Path file = Files.createTempFile("soulseek-download-", ".bin");
        try (Fixture first = new Fixture()) {
            Files.write(file, new byte[] {9, 9, 9});
            first.transfer.data = new byte[] {1, 2};
            first.waiter.startRequest =
                    CompletableFuture.completedFuture(new TransferRequest(TransferDirection.UPLOAD, 26, "file", 2));
            assertInstanceOf(
                    TransferOutcome.Succeeded.class,
                    first.client
                            .transfers()
                            .download(DownloadRequest.toFile("alice", "file", file.toString())
                                    .size(2L)
                                    .token(26)
                                    .options(options())
                                    .build()));
            assertArrayEquals(new byte[] {1, 2}, Files.readAllBytes(file));
        }

        try (Fixture second = new Fixture()) {
            second.transfer.data = new byte[] {3, 4};
            second.waiter.startRequest =
                    CompletableFuture.completedFuture(new TransferRequest(TransferDirection.UPLOAD, 27, "file", 4));
            assertInstanceOf(
                    TransferOutcome.Succeeded.class,
                    second.client
                            .transfers()
                            .download(DownloadRequest.toFile("alice", "file", file.toString())
                                    .size(4L)
                                    .startOffset(2)
                                    .token(27)
                                    .options(options())
                                    .build()));
            assertArrayEquals(new byte[] {1, 2, 3, 4}, Files.readAllBytes(file));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Appending ignores position — O_APPEND puts every write at end-of-file —
     * so resuming a file whose length is not the requested offset would land
     * the bytes at the wrong place with no error. The C# source fails loudly
     * on the same mismatch.
     */
    @Test
    @DisplayName("a resume whose offset does not match the local file fails loudly, not silently")
    void resumeWithMismatchedLocalLengthFailsLoudly() throws IOException {
        Path file = Files.createTempFile("soulseek-download-", ".bin");
        try (Fixture fixture = new Fixture()) {
            Files.write(file, new byte[] {1, 2, 3});
            fixture.transfer.data = new byte[] {9, 9};
            fixture.waiter.startRequest =
                    CompletableFuture.completedFuture(new TransferRequest(TransferDirection.UPLOAD, 30, "file", 4));

            TransferOutcome outcome = fixture.client
                    .transfers()
                    .download(DownloadRequest.toFile("alice", "file", file.toString())
                            .size(4L)
                            .startOffset(2)
                            .token(30)
                            .options(options())
                            .build());

            assertInstanceOf(TransferOutcome.Failed.class, outcome);
            assertArrayEquals(new byte[] {1, 2, 3}, Files.readAllBytes(file), "the local file is untouched");
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    void outputDisposalOptionIsHonored() {
        try (Fixture fixture = new Fixture()) {
            fixture.transfer.data = new byte[] {1};
            fixture.waiter.startRequest =
                    CompletableFuture.completedFuture(new TransferRequest(TransferDirection.UPLOAD, 28, "file", 1));
            CloseTrackingOutputStream disposable = new CloseTrackingOutputStream();
            fixture.client
                    .transfers()
                    .download(DownloadRequest.toStream("alice", "file", () -> disposable)
                            .size(1L)
                            .token(28)
                            .options(options().withDisposalOptions(null, true))
                            .build());
            assertTrue(disposable.flushed.get());
            assertTrue(disposable.closed.get());

            fixture.transfer.data = new byte[] {2};
            CloseTrackingOutputStream retained = new CloseTrackingOutputStream();
            fixture.client
                    .transfers()
                    .download(DownloadRequest.toStream("alice", "other", () -> retained)
                            .size(1L)
                            .token(29)
                            .options(options().withDisposalOptions(null, false))
                            .build());
            assertFalse(retained.closed.get());
        }
    }

    @Test
    void cleanupFailuresAreWarningsAndDoNotChangeSuccess() {
        DiagnosticProbe diagnostic = new DiagnosticProbe();
        try (Fixture fixture = new Fixture(diagnostic.proxy)) {
            fixture.transfer.data = new byte[] {1};
            fixture.waiter.startRequest =
                    CompletableFuture.completedFuture(new TransferRequest(TransferDirection.UPLOAD, 32, "file", 1));
            fixture.waiter.cancelFailure = new IllegalStateException("cancel failed");
            fixture.transfer.closeFailure = new IllegalStateException("close failed");
            FailingCleanupOutputStream output = new FailingCleanupOutputStream();

            TransferOutcome outcome = fixture.client
                    .transfers()
                    .download(DownloadRequest.toStream("alice", "folder/file", () -> output)
                            .size(1L)
                            .token(32)
                            .options(options().withDisposalOptions(null, true))
                            .build());

            assertInstanceOf(TransferOutcome.Succeeded.class, outcome);
            assertTrue(diagnostic.warnings.stream().anyMatch(warning -> warning.contains("Failed to cancel wait")));
            assertTrue(diagnostic.warnings.stream()
                    .anyMatch(warning -> warning.contains("Failed to dispose transfer connection")));
            assertTrue(diagnostic.warnings.stream()
                    .anyMatch(warning -> warning.contains("Failed to determine final position")));
            assertTrue(diagnostic.warnings.stream()
                    .anyMatch(warning -> warning.contains("Failed to finalize output stream")));
        }
    }

    /**
     * Every snapshot a transfer published, in the order it published them.
     *
     * <p>The engine used to broadcast these and a test subscribed to the
     * broadcast. Nothing subscribed to it in production once the downloads
     * facet stopped correlating it back by token, so it went; what a transfer
     * tells its own caller is the per-transfer callback pair, and that is where
     * these assertions read from now.
     */
    private static final class Timeline {
        private final List<Transfer> snapshots = new ArrayList<>();
        private final List<Long> progress = new ArrayList<>();

        private TransferOptions on(TransferOptions base) {
            return new TransferOptions(
                    change -> {
                        snapshots.add(change.transfer());
                        if (base.getStateChanged() != null) {
                            base.getStateChanged().onStateChanged(change);
                        }
                    },
                    update -> {
                        progress.add(update.transfer().getBytesTransferred());
                        if (base.getProgressUpdated() != null) {
                            base.getProgressUpdated().onProgressUpdated(update);
                        }
                    },
                    base.getSlotReleased(),
                    base.getReporter(),
                    base.getMaximumLingerTime(),
                    base.isSeekInputStreamAutomatically(),
                    base.isSeekOutputStreamAutomatically(),
                    base.isDisposeInputStreamOnCompletion(),
                    base.isDisposeOutputStreamOnCompletion());
        }

        private List<TransferState> states() {
            return snapshots.stream().map(Transfer::getState).toList();
        }

        private Transfer last() {
            return snapshots.get(snapshots.size() - 1);
        }

        /** The snapshot taken as the transfer reached a terminal state. */
        private Transfer terminal() {
            return snapshots.stream()
                    .filter(snapshot -> snapshot.getState().contains(TransferState.COMPLETED))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("the transfer never reached a terminal state"));
        }
    }

    /** Returns what a failed outcome says went wrong. */
    private static Throwable causeOf(TransferOutcome outcome) {
        return assertInstanceOf(TransferOutcome.Failed.class, outcome).cause();
    }

    private static java.util.function.Supplier<java.io.OutputStream> outputFactory() {
        return ByteArrayOutputStream::new;
    }

    private static TransferOptions options() {
        return new TransferOptions().withDisposalOptions(null, false);
    }

    private static TransferInternal transfer(TransferDirection direction, String username, String filename, int token) {
        return new TransferInternal(direction, username, filename, token);
    }

    /**
     * Runs a blocking client call on a virtual thread so the test can interact
     * with it while it is in flight.
     *
     * <p>The API used to hand back a future; now the caller decides whether to
     * be concurrent, and a test that wants to observe a call mid-flight is
     * exactly such a caller. The assertions around it are unchanged.
     */
    /**
     * A place in a peer's queue is not a transfer, and must not cost a slot.
     *
     * <p>The reason a fifteen-track album used to arrive one track at a time.
     * Both ceilings are set to one here and the download is left waiting for the
     * peer, which is where it spends almost all of its life; if waiting took a
     * slot, the other fourteen tracks could not even be asked for until this one
     * had finished. The slot is taken when the peer says it is ready, and not
     * before.
     */
    @Test
    void aDownloadWaitingInAPeersQueueHoldsNoTransferSlot() {
        try (Fixture fixture = new Fixture()) {
            fixture.client.transfers().downloadConcurrency(1, 1);
            fixture.waiter.startRequest = new CompletableFuture<>();

            CompletableFuture<TransferOutcome> download = inBackground(() -> fixture.client
                    .transfers()
                    .download(DownloadRequest.toStream("alice", "file", outputFactory())
                            .size(1L)
                            .token(34)
                            .options(options())
                            .build()));

            awaitAsk(fixture);
            assertEquals(
                    1,
                    fixture.client.transfers().globalDownloadSemaphore().availablePermits(),
                    "waiting in a peer's queue must not take an overall slot");
            assertEquals(
                    1,
                    fixture.client.transfers().downloadSemaphoreFor("alice").availablePermits(),
                    "waiting in a peer's queue must not take that peer's slot");

            fixture.transfer.data = new byte[] {1};
            fixture.waiter.startRequest.complete(new TransferRequest(TransferDirection.UPLOAD, 102, "file", 1));
            assertInstanceOf(TransferOutcome.Succeeded.class, download.join());

            assertEquals(
                    1,
                    fixture.client.transfers().downloadSemaphoreFor("alice").availablePermits(),
                    "the slot is given back when the transfer ends");
        }
    }

    /** Waits until the download has asked the peer to queue the file. */
    private static void awaitAsk(Fixture fixture) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            if (fixture.message.messages.stream().anyMatch(QueueDownloadRequest.class::isInstance)) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("the peer was never asked to queue the file");
    }

    private static <T> CompletableFuture<T> inBackground(java.util.function.Supplier<T> call) {
        return CompletableFuture.supplyAsync(call, Executors.newVirtualThreadPerTaskExecutor());
    }

    /** Void-returning variant of {@link #inBackground}. */
    private static CompletableFuture<Void> inBackground(Runnable call) {
        return CompletableFuture.runAsync(call, Executors.newVirtualThreadPerTaskExecutor());
    }

    /**
     * Returns the failure a blocking call produced.
     *
     * <p>Took a future before the API became blocking; the calls now throw
     * directly, so it takes the call itself.
     */
    private static Throwable failureOf(org.junit.jupiter.api.function.Executable body) {
        try {
            body.execute();
        } catch (java.util.concurrent.CompletionException wrapped) {
            return wrapped.getCause() == null ? wrapped : wrapped.getCause();
        } catch (Throwable failure) {
            return failure;
        }
        throw new AssertionError("Expected operation to fail");
    }

    private static Throwable unwrapCompletionFailure(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof CancellationException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static Object defaultValue(Class<?> type) {
        if (type == CompletableFuture.class) {
            return CompletableFuture.completedFuture(null);
        }
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == double.class) {
            return 0D;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static <T> T cast(Object value) {
        return (T) value;
    }

    private static final class Fixture implements AutoCloseable {
        private final MessageConnectionProbe server = new MessageConnectionProbe();
        private final MessageConnectionProbe message = new MessageConnectionProbe();
        private final TransferConnectionProbe transfer = new TransferConnectionProbe();
        private final WaiterProbe waiter = new WaiterProbe();
        private final PeerManagerProbe peerManager = new PeerManagerProbe(message.proxy, transfer.proxy);
        private final SoulseekEngine client;

        private Fixture() {
            this(null);
        }

        private Fixture(DiagnosticSink diagnostic) {
            client = new SoulseekEngine(
                    9999,
                    null,
                    server.proxy,
                    null,
                    peerManager.proxy,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    waiter.proxy,
                    null,
                    diagnostic,
                    null,
                    null,
                    null);
            client.setStateForTest(SoulseekClientState.CONNECTED.or(SoulseekClientState.LOGGED_IN));
        }

        @Override
        public void close() {
            client.close();
        }
    }

    private static final class WaiterProbe {

        /**
         * The peer's offer, and the only thing a download now waits for.
         *
         * <p>Completed by default rather than pending, because under
         * {@code QueueUpload} every download reaches this wait — there is no
         * longer an acknowledgement that can allow a transfer outright and skip
         * it. A test that wants to observe the wait replaces this with a pending
         * future; one that wants a refusal fails it, which is what
         * {@code UploadDenied} and {@code UploadFailed} do in production.
         */
        private CompletableFuture<TransferRequest> startRequest =
                CompletableFuture.completedFuture(new TransferRequest(TransferDirection.UPLOAD, 0, "file", 0));

        private final List<WaitKey> cancelled = new ArrayList<>();
        private RuntimeException cancelFailure;
        private final Waiter proxy = (Waiter)
                Proxy.newProxyInstance(Waiter.class.getClassLoader(), new Class<?>[] {Waiter.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().startsWith("register") && arguments != null) {
                for (Object argument : arguments) {
                    if (argument == TransferRequest.class) {
                        return (Wait<Object>) () -> Outcomes.raise(startRequest);
                    }
                    if (argument == UserAddressResponse.class) {
                        return (Wait<Object>) () -> new UserAddressResponse("alice", ENDPOINT);
                    }
                }
                return (Wait<Object>) () -> null;
            }
            if (method.getName().equals("cancel")) {
                if (cancelFailure != null) {
                    throw cancelFailure;
                }
                cancelled.add((WaitKey) arguments[0]);
                return null;
            }
            if (method.getName().equals("getDefaultTimeout")) {
                return 5_000;
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class PeerManagerProbe {
        private final MessageConnection message;
        private final Connection transfer;
        private CompletableFuture<Connection> awaitResult;
        private int awaitCalls;
        private int outgoingTransferCalls;
        private int outgoingToken;
        private final PeerConnectionManager proxy = (PeerConnectionManager) Proxy.newProxyInstance(
                PeerConnectionManager.class.getClassLoader(),
                new Class<?>[] {PeerConnectionManager.class},
                this::invoke);

        private PeerManagerProbe(MessageConnection message, Connection transfer) {
            this.message = message;
            this.transfer = transfer;
            awaitResult = CompletableFuture.completedFuture(transfer);
        }

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("getOrAddMessageConnection")
                    && arguments != null
                    && arguments.length == 3
                    && arguments[0] instanceof String) {
                return message;
            }
            if (method.getName().equals("getTransferConnection")
                    && arguments != null
                    && arguments.length == 4
                    && arguments[0] instanceof String) {
                outgoingTransferCalls++;
                outgoingToken = (Integer) arguments[2];
                return transfer;
            }
            if (method.getName().equals("awaitTransferConnection")) {
                awaitCalls++;
                return Outcomes.raise(awaitResult);
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class MessageConnectionProbe {
        // Copy-on-write because a test that watches for the ask while the
        // download runs on another thread reads this while the run writes it.
        private final List<OutgoingMessage> messages = new java.util.concurrent.CopyOnWriteArrayList<>();
        private final MessageConnection proxy = (MessageConnection) Proxy.newProxyInstance(
                MessageConnection.class.getClassLoader(), new Class<?>[] {MessageConnection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("write")
                    && arguments != null
                    && arguments.length == 2
                    && arguments[0] instanceof OutgoingMessage message) {
                messages.add(message);
                return null;
            }
            if (method.getName().equals("getState")) {
                return ConnectionState.CONNECTED;
            }
            if (method.getName().equals("getId")) {
                return UUID.randomUUID();
            }
            if (method.getName().equals("getIpEndpoint")) {
                return ENDPOINT;
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class TransferConnectionProbe {
        private byte[] data = new byte[0];
        private byte[] offsetBytes;
        private long readLength;
        private int maximumActualPerIteration = Integer.MAX_VALUE;
        private boolean blockRead;
        private Runnable readHook;
        private Exception disconnectOnRead;
        private RuntimeException closeFailure;
        private ConnectionEventListener<ConnectionDataEvent> dataReadListener;
        private ConnectionEventListener<ConnectionDisconnectedEvent> disconnectedListener;
        private final Connection proxy = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) throws IOException {
            if (method.getName().equals("write")
                    && arguments != null
                    && arguments.length == 2
                    && arguments[0] instanceof byte[] bytes) {
                offsetBytes = bytes.clone();
                return null;
            }
            if (method.getName().equals("read")
                    && arguments != null
                    && arguments.length == 5
                    && arguments[0] instanceof Long length) {
                readLength = length;
                if (readHook != null) {
                    readHook.run();
                }
                if (disconnectOnRead != null) {
                    disconnectedListener.handle(
                            proxy, new ConnectionDisconnectedEvent("connection lost", disconnectOnRead));
                    // Parks like a read from a peer that has stopped sending.
                    new CompletableFuture<Void>().join();
                }
                if (blockRead) {
                    new CompletableFuture<Void>().join();
                }
                OutputStream output = (OutputStream) arguments[1];
                ConnectionGovernor governor = (ConnectionGovernor) arguments[2];
                ConnectionReporter reporter = (ConnectionReporter) arguments[3];
                CancellationSignal token = (CancellationSignal) arguments[4];
                int sourceOffset = 0;
                long transferred = 0;
                while (transferred < length) {
                    int attempted = (int) Math.min(Integer.MAX_VALUE, length - transferred);
                    int granted = governor == null ? attempted : governor.grant(attempted, token);
                    int actual = Math.min(Math.min(granted, maximumActualPerIteration), data.length - sourceOffset);
                    if (actual <= 0) {
                        throw new IOException("source ended early");
                    }
                    output.write(data, sourceOffset, actual);
                    sourceOffset += actual;
                    transferred += actual;
                    if (reporter != null) {
                        reporter.report(attempted, granted, actual);
                    }
                    if (dataReadListener != null) {
                        dataReadListener.handle(proxy, new ConnectionDataEvent(transferred, length));
                    }
                }
                return null;
            }
            if (method.getName().equals("addDataReadListener")) {
                dataReadListener = cast(arguments[0]);
                return null;
            }
            if (method.getName().equals("removeDataReadListener")) {
                if (dataReadListener == arguments[0]) {
                    dataReadListener = null;
                }
                return null;
            }
            if (method.getName().equals("addDisconnectedListener")) {
                disconnectedListener = cast(arguments[0]);
                return null;
            }
            if (method.getName().equals("removeDisconnectedListener")) {
                if (disconnectedListener == arguments[0]) {
                    disconnectedListener = null;
                }
                return null;
            }
            if (method.getName().equals("close") && closeFailure != null) {
                throw closeFailure;
            }
            if (method.getName().equals("getState")) {
                return ConnectionState.CONNECTED;
            }
            if (method.getName().equals("getId")) {
                return UUID.randomUUID();
            }
            if (method.getName().equals("getIpEndpoint")) {
                return ENDPOINT;
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class PositionableBuffer extends OutputStream implements PositionableOutputStream {
        private byte[] bytes = new byte[16];
        private int length;
        private int position;

        private PositionableBuffer(byte[] initial) {
            writeBytes(initial);
        }

        @Override
        public long getPosition() {
            return position;
        }

        @Override
        public void setPosition(long value) {
            position = Math.toIntExact(value);
            ensure(position);
            length = Math.max(length, position);
        }

        @Override
        public void write(int value) {
            ensure(position + 1);
            bytes[position++] = (byte) value;
            length = Math.max(length, position);
        }

        @Override
        public void write(byte[] source, int offset, int count) {
            ensure(position + count);
            System.arraycopy(source, offset, bytes, position, count);
            position += count;
            length = Math.max(length, position);
        }

        private void writeBytes(byte[] source) {
            write(source, 0, source.length);
        }

        private void ensure(int required) {
            if (required <= bytes.length) {
                return;
            }
            byte[] expanded = new byte[Math.max(required, bytes.length * 2)];
            System.arraycopy(bytes, 0, expanded, 0, length);
            bytes = expanded;
        }

        private byte[] toByteArray() {
            byte[] result = new byte[length];
            System.arraycopy(bytes, 0, result, 0, length);
            return result;
        }
    }

    private static final class CloseTrackingOutputStream extends ByteArrayOutputStream {
        private final AtomicBoolean flushed = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();

        @Override
        public void flush() throws IOException {
            flushed.set(true);
            super.flush();
        }

        @Override
        public void close() throws IOException {
            closed.set(true);
            super.close();
        }
    }

    private static final class FailingCleanupOutputStream extends OutputStream implements PositionableOutputStream {
        private long position;
        private int positionReads;

        @Override
        public long getPosition() throws IOException {
            positionReads++;
            if (positionReads > 1) {
                throw new IOException("position failed");
            }
            return position;
        }

        @Override
        public void setPosition(long value) {
            position = value;
        }

        @Override
        public void write(int value) {
            position++;
        }

        @Override
        public void write(byte[] bytes, int offset, int length) {
            position += length;
        }

        @Override
        public void flush() throws IOException {
            throw new IOException("flush failed");
        }
    }

    private static final class DiagnosticProbe {
        private final List<String> warnings = new ArrayList<>();
        private final DiagnosticSink proxy = (DiagnosticSink) Proxy.newProxyInstance(
                DiagnosticSink.class.getClassLoader(), new Class<?>[] {DiagnosticSink.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("warning")) {
                warnings.add((String) arguments[0]);
            }
            return defaultValue(method.getReturnType());
        }
    }
}
