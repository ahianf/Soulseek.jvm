// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.common.IWaiter;
import dev.slsk.common.WaitKey;
import dev.slsk.diagnostics.IDiagnosticFactory;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.DuplicateTokenException;
import dev.slsk.exceptions.DuplicateTransferException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.exceptions.TransferReportedFailedException;
import dev.slsk.exceptions.TransferSizeMismatchException;
import dev.slsk.messaging.messages.OutgoingMessage;
import dev.slsk.messaging.messages.TransferRequest;
import dev.slsk.messaging.messages.TransferResponse;
import dev.slsk.messaging.messages.UserAddressResponse;
import dev.slsk.network.IMessageConnection;
import dev.slsk.network.IPeerConnectionManager;
import dev.slsk.network.tcp.Connection;
import dev.slsk.network.tcp.ConnectionDataEventArgs;
import dev.slsk.network.tcp.ConnectionDisconnectedEventArgs;
import dev.slsk.network.tcp.ConnectionEventListener;
import dev.slsk.network.tcp.ConnectionGovernor;
import dev.slsk.network.tcp.ConnectionReporter;
import dev.slsk.network.tcp.ConnectionState;
import dev.slsk.options.PositionableOutputStream;
import dev.slsk.options.TransferOptions;
import dev.slsk.transfer.TransferInternal;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class SoulseekClientDownloadTest {
    private static final InetSocketAddress ENDPOINT = new InetSocketAddress(InetAddress.getLoopbackAddress(), 46012);

    @Test
    void validatesArgumentsRangeFactoryAndLoginState() {
        try (Fixture fixture = new Fixture()) {
            for (String bad : new String[] {null, "", " ", "\t"}) {
                assertThrows(
                        IllegalArgumentException.class,
                        () -> fixture.client.downloadAsync(bad, "file", outputFactory()));
                assertThrows(
                        IllegalArgumentException.class,
                        () -> fixture.client.downloadAsync("alice", bad, outputFactory()));
                assertThrows(IllegalArgumentException.class, () -> fixture.client.downloadAsync("alice", "file", bad));
            }
            assertThrows(
                    IllegalArgumentException.class,
                    () -> fixture.client.downloadAsync("alice", "file", outputFactory(), -1L));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> fixture.client.downloadAsync("alice", "file", outputFactory(), 1L, -1));
            assertThrows(
                    NullPointerException.class,
                    () -> fixture.client.downloadAsync("alice", "file", outputFactory(), null, 1));
            assertThrows(
                    NullPointerException.class,
                    () -> fixture.client.downloadAsync(
                            "alice",
                            "file",
                            (dev.slsk.options.DownloadStreamFactory) null,
                            null,
                            0,
                            null,
                            null,
                            CancellationToken.none()));

            fixture.client.setStateForTest(SoulseekClientStates.DISCONNECTED);
            assertThrows(
                    IllegalStateException.class, () -> fixture.client.downloadAsync("alice", "file", outputFactory()));
        }
    }

    @Test
    void rejectsDuplicateTokensTransfersAndUniqueKeys() {
        try (Fixture fixture = new Fixture()) {
            fixture.client.getDownloadDictionary().put(1, transfer(TransferDirection.DOWNLOAD, "other", "other", 1));
            assertThrows(
                    DuplicateTokenException.class,
                    () -> fixture.client.downloadAsync("alice", "file", outputFactory(), 1L, 0, 1));

            fixture.client.getDownloadDictionary().clear();
            fixture.client.getUploadsInternal().put(2, transfer(TransferDirection.UPLOAD, "other", "other", 2));
            assertThrows(
                    DuplicateTokenException.class,
                    () -> fixture.client.downloadAsync("alice", "file", outputFactory(), 1L, 0, 2));

            fixture.client.getUploadsInternal().clear();
            fixture.client.getDownloadDictionary().put(3, transfer(TransferDirection.DOWNLOAD, "alice", "file", 3));
            assertThrows(
                    DuplicateTransferException.class,
                    () -> fixture.client.downloadAsync("alice", "file", outputFactory(), 1L, 0, 4));

            fixture.client.getDownloadDictionary().clear();
            fixture.client.getUniqueKeys().put("Download:alice:file", true);
            assertThrows(
                    DuplicateTransferException.class,
                    () -> fixture.client.downloadAsync("alice", "file", outputFactory(), 1L, 0, 4));
        }
    }

    @Test
    void immediateDownloadUsesRemoteSizeProtocolAndStateOrder() {
        try (Fixture fixture = new Fixture()) {
            byte[] bytes = new byte[] {1, 2, 3, 4};
            fixture.transfer.data = bytes;
            fixture.waiter.response = CompletableFuture.completedFuture(new TransferResponse(11, bytes.length));
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            List<TransferStates> states = new ArrayList<>();
            List<Long> progress = new ArrayList<>();
            fixture.client.addTransferStateChangedListener(
                    (sender, eventArgs) -> states.add(eventArgs.getTransfer().getState()));
            fixture.client.addTransferProgressUpdatedListener(
                    (sender, eventArgs) -> progress.add(eventArgs.getTransfer().getBytesTransferred()));

            Transfer result = fixture.client
                    .downloadAsync(
                            "alice",
                            "remote\\file",
                            () -> CompletableFuture.completedFuture(output),
                            null,
                            0,
                            11,
                            options(),
                            CancellationToken.none())
                    .join();

            assertEquals(bytes.length, result.getSize());
            assertEquals(bytes.length, result.getBytesTransferred());
            assertArrayEquals(bytes, output.toByteArray());
            assertArrayEquals(new byte[8], fixture.transfer.offsetBytes);
            TransferRequest request = assertInstanceOf(TransferRequest.class, fixture.message.messages.get(0));
            assertEquals(TransferDirection.DOWNLOAD, request.getDirection());
            assertEquals(0, request.getFileSize());
            assertEquals("remote\\file", request.getFilename());
            assertEquals(
                    List.of(
                            TransferStates.QUEUED.or(TransferStates.LOCALLY),
                            TransferStates.REQUESTED,
                            TransferStates.QUEUED.or(TransferStates.REMOTELY),
                            TransferStates.INITIALIZING,
                            TransferStates.IN_PROGRESS,
                            TransferStates.COMPLETED.or(TransferStates.SUCCEEDED)),
                    states);
            assertTrue(progress.contains(0L));
            assertTrue(progress.contains((long) bytes.length));
            assertFalse(fixture.client.getDownloadDictionary().containsKey(11));
            assertFalse(fixture.client.getUniqueKeys().containsKey("Download:alice:remote\\file"));
        }
    }

    @Test
    void expectedSizeMismatchIsPreservedAndAborted() {
        try (Fixture fixture = new Fixture()) {
            fixture.waiter.response = CompletableFuture.completedFuture(new TransferResponse(12, 5));
            List<Transfer> terminal = new ArrayList<>();
            fixture.client.addTransferStateChangedListener((sender, eventArgs) -> {
                if (eventArgs.getTransfer().getState().hasFlag(TransferStates.COMPLETED)) {
                    terminal.add(eventArgs.getTransfer());
                }
            });

            Throwable failure = failureOf(fixture.client.downloadAsync(
                    "alice", "file", outputFactory(), 4L, 0, 12, options(), CancellationToken.none()));

            TransferSizeMismatchException mismatch = assertInstanceOf(TransferSizeMismatchException.class, failure);
            assertEquals(4, mismatch.getLocalSize());
            assertEquals(5, mismatch.getRemoteSize());
            assertTrue(terminal.get(0).getState().hasFlag(TransferStates.ABORTED));
            assertSame(mismatch, terminal.get(0).getException());
        }
    }

    @Test
    void queuedSizeMismatchIsPreservedAndAborted() {
        try (Fixture fixture = new Fixture()) {
            fixture.waiter.response = CompletableFuture.completedFuture(new TransferResponse(30, "Queued"));
            fixture.waiter.startRequest =
                    CompletableFuture.completedFuture(new TransferRequest(TransferDirection.UPLOAD, 130, "file", 6));
            List<Transfer> terminal = new ArrayList<>();
            fixture.client.addTransferStateChangedListener((sender, eventArgs) -> {
                if (eventArgs.getTransfer().getState().hasFlag(TransferStates.COMPLETED)) {
                    terminal.add(eventArgs.getTransfer());
                }
            });

            Throwable failure = failureOf(fixture.client.downloadAsync(
                    "alice", "file", outputFactory(), 5L, 0, 30, options(), CancellationToken.none()));

            TransferSizeMismatchException mismatch = assertInstanceOf(TransferSizeMismatchException.class, failure);
            assertEquals(5, mismatch.getLocalSize());
            assertEquals(6, mismatch.getRemoteSize());
            assertTrue(terminal.get(0).getState().hasFlag(TransferStates.ABORTED));
            assertSame(mismatch, terminal.get(0).getException());
        }
    }

    @Test
    void nonQueueDenialIsRejected() {
        try (Fixture fixture = new Fixture()) {
            fixture.waiter.response = CompletableFuture.completedFuture(new TransferResponse(13, "not shared"));

            Throwable failure = failureOf(fixture.client.downloadAsync(
                    "alice", "file", outputFactory(), 1L, 0, 13, options(), CancellationToken.none()));

            assertInstanceOf(TransferRejectedException.class, failure);
        }
    }

    @Test
    void queuedDownloadRespondsAndUsesIncomingConnection() {
        try (Fixture fixture = new Fixture()) {
            byte[] bytes = new byte[] {7, 8, 9};
            fixture.transfer.data = bytes;
            fixture.waiter.response = CompletableFuture.completedFuture(new TransferResponse(14, "Queued..."));
            fixture.waiter.startRequest = CompletableFuture.completedFuture(
                    new TransferRequest(TransferDirection.UPLOAD, 99, "file", bytes.length));
            ByteArrayOutputStream output = new ByteArrayOutputStream();

            Transfer result = fixture.client
                    .downloadAsync(
                            "alice",
                            "file",
                            () -> CompletableFuture.completedFuture(output),
                            null,
                            0,
                            14,
                            options(),
                            CancellationToken.none())
                    .join();

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
            fixture.waiter.response = CompletableFuture.completedFuture(new TransferResponse(15, "queued"));
            fixture.waiter.startRequest =
                    CompletableFuture.completedFuture(new TransferRequest(TransferDirection.UPLOAD, 100, "file", 1));
            fixture.peerManager.awaitResult =
                    CompletableFuture.failedFuture(new ConnectionException("peer did not connect"));

            Transfer result = fixture.client
                    .downloadAsync("alice", "file", outputFactory(), 1L, 0, 15, options(), CancellationToken.none())
                    .join();

            assertTrue(result.getState().hasFlag(TransferStates.SUCCEEDED));
            assertEquals(1, fixture.peerManager.awaitCalls);
            assertEquals(1, fixture.peerManager.outgoingTransferCalls);
            assertEquals(100, fixture.peerManager.outgoingToken);
        }
    }

    @Test
    void enqueueReturnsOnRemoteQueueBeforePeerStartsTransfer() {
        try (Fixture fixture = new Fixture()) {
            fixture.waiter.response = CompletableFuture.completedFuture(new TransferResponse(16, "Queued"));
            fixture.waiter.startRequest = new CompletableFuture<>();

            CompletableFuture<Transfer> download = fixture.client
                    .enqueueDownloadAsync(
                            "alice", "file", outputFactory(), null, 0, 16, options(), CancellationToken.none())
                    .join();

            assertFalse(download.isDone());
            fixture.transfer.data = new byte[] {1, 2};
            fixture.waiter.startRequest.complete(new TransferRequest(TransferDirection.UPLOAD, 101, "file", 2));
            assertTrue(download.join().getState().hasFlag(TransferStates.SUCCEEDED));
        }
    }

    @Test
    void enqueuePropagatesFailureBeforeRemoteQueueing() {
        try (Fixture fixture = new Fixture()) {
            TransferRejectedException rejection = new TransferRejectedException("rejected");
            fixture.waiter.response = CompletableFuture.failedFuture(rejection);

            Throwable failure = failureOf(fixture.client.enqueueDownloadAsync(
                    "alice", "file", outputFactory(), 1L, 0, 17, options(), CancellationToken.none()));

            assertSame(rejection, failure);
        }
    }

    @Test
    void resumeSeeksOutputWritesOffsetAndReadsRemainingBytes() {
        try (Fixture fixture = new Fixture()) {
            fixture.transfer.data = new byte[] {3, 4, 5};
            fixture.waiter.response = CompletableFuture.completedFuture(new TransferResponse(18, 5));
            PositionableBuffer output = new PositionableBuffer(new byte[] {1, 2});

            Transfer result = fixture.client
                    .downloadAsync(
                            "alice",
                            "file",
                            () -> CompletableFuture.completedFuture(output),
                            5L,
                            2,
                            18,
                            options(),
                            CancellationToken.none())
                    .join();

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
            fixture.waiter.response = CompletableFuture.completedFuture(new TransferResponse(19, 2));
            Throwable failure = failureOf(fixture.client.downloadAsync(
                    "alice", "file", outputFactory(), 2L, 1, 19, options(), CancellationToken.none()));
            assertInstanceOf(SoulseekClientException.class, failure);

            TransferOptions noSeek = new TransferOptions(null, null, null, null, null, null, 3_000, true, false);
            fixture.transfer.data = new byte[] {2};
            Transfer result = fixture.client
                    .downloadAsync("alice", "other", outputFactory(), 2L, 1, 20, noSeek, CancellationToken.none())
                    .join();
            assertEquals(
                    1, result.getBytesTransferred(), "final progress follows stream position when seek is bypassed");
        }
    }

    @Test
    void governorReporterAndProgressCallbacksAreInvoked() {
        try (Fixture fixture = new Fixture()) {
            fixture.transfer.data = new byte[] {1, 2, 3, 4, 5};
            fixture.transfer.maximumActualPerIteration = 2;
            fixture.waiter.response = CompletableFuture.completedFuture(new TransferResponse(21, 5));
            List<Integer> grants = new ArrayList<>();
            List<List<Integer>> reports = new ArrayList<>();
            List<Long> progress = new ArrayList<>();
            TransferOptions options = new TransferOptions(
                    (transfer, requested, token) -> {
                        grants.add(requested);
                        return CompletableFuture.completedFuture(requested);
                    },
                    null,
                    update -> progress.add(update.transfer().getBytesTransferred()),
                    null,
                    null,
                    (transfer, attempted, granted, actual) -> reports.add(List.of(attempted, granted, actual)));

            fixture.client
                    .downloadAsync("alice", "file", outputFactory(), 5L, 0, 21, options, CancellationToken.none())
                    .join();

            assertFalse(grants.isEmpty());
            assertFalse(reports.isEmpty());
            assertTrue(reports.stream().anyMatch(report -> report.get(1) > report.get(2)));
            assertTrue(progress.contains(5L));
        }
    }

    @Test
    void remoteFailureRaceIsWrappedAndRecorded() {
        try (Fixture fixture = new Fixture()) {
            fixture.waiter.response = CompletableFuture.completedFuture(new TransferResponse(22, 1));
            fixture.transfer.readHook = () -> fixture.client
                    .getDownloadDictionary()
                    .get(22)
                    .getRemoteTaskCompletionSource()
                    .completeExceptionally(new TransferReportedFailedException("remote failed"));
            fixture.transfer.blockRead = true;
            List<Transfer> terminal = new ArrayList<>();
            fixture.client.addTransferStateChangedListener((sender, eventArgs) -> {
                if (eventArgs.getTransfer().getState().hasFlag(TransferStates.COMPLETED)) {
                    terminal.add(eventArgs.getTransfer());
                }
            });

            Throwable failure = failureOf(fixture.client.downloadAsync(
                    "alice", "file", outputFactory(), 1L, 0, 22, options(), CancellationToken.none()));

            SoulseekClientException mapped = assertInstanceOf(SoulseekClientException.class, failure);
            assertInstanceOf(TransferReportedFailedException.class, mapped.getCause());
            assertSame(mapped.getCause(), terminal.get(0).getException());
            assertTrue(terminal.get(0).getState().hasFlag(TransferStates.ERRORED));
        }
    }

    @Test
    void remoteDenialDuringReadIsPreservedAndRejected() {
        try (Fixture fixture = new Fixture()) {
            fixture.waiter.response = CompletableFuture.completedFuture(new TransferResponse(31, 1));
            TransferRejectedException rejection = new TransferRejectedException("download denied");
            fixture.transfer.readHook = () -> fixture.client
                    .getDownloadDictionary()
                    .get(31)
                    .getRemoteTaskCompletionSource()
                    .completeExceptionally(rejection);
            fixture.transfer.blockRead = true;
            List<Transfer> terminal = new ArrayList<>();
            fixture.client.addTransferStateChangedListener((sender, eventArgs) -> {
                if (eventArgs.getTransfer().getState().hasFlag(TransferStates.COMPLETED)) {
                    terminal.add(eventArgs.getTransfer());
                }
            });

            Throwable failure = failureOf(fixture.client.downloadAsync(
                    "alice", "file", outputFactory(), 1L, 0, 31, options(), CancellationToken.none()));

            assertSame(rejection, failure);
            assertSame(rejection, terminal.get(0).getException());
            assertTrue(terminal.get(0).getState().hasFlag(TransferStates.REJECTED));
        }
    }

    @Test
    void unexpectedDisconnectIsWrappedAsConnectionFailure() {
        try (Fixture fixture = new Fixture()) {
            IOException socketFailure = new IOException("socket failed");
            fixture.waiter.response = CompletableFuture.completedFuture(new TransferResponse(23, 1));
            fixture.transfer.disconnectOnRead = socketFailure;

            Throwable failure = failureOf(fixture.client.downloadAsync(
                    "alice", "file", outputFactory(), 1L, 0, 23, options(), CancellationToken.none()));

            SoulseekClientException mapped = assertInstanceOf(SoulseekClientException.class, failure);
            ConnectionException connection = assertInstanceOf(ConnectionException.class, mapped.getCause());
            assertSame(socketFailure, connection.getCause());
        }
    }

    @Test
    void timeoutAndCancellationArePreserved() {
        try (Fixture timeoutFixture = new Fixture();
                Fixture cancellationFixture = new Fixture()) {
            TimeoutException timeout = new TimeoutException("timed out");
            timeoutFixture.waiter.response = CompletableFuture.failedFuture(timeout);
            assertSame(
                    timeout,
                    failureOf(timeoutFixture.client.downloadAsync(
                            "alice", "file", outputFactory(), 1L, 0, 24, options(), CancellationToken.none())));

            CancellationException cancellation = new CancellationException("cancelled");
            cancellationFixture.waiter.response = CompletableFuture.failedFuture(cancellation);
            assertSame(
                    cancellation,
                    failureOf(cancellationFixture.client.downloadAsync(
                            "alice", "file", outputFactory(), 1L, 0, 25, options(), CancellationToken.none())));
        }
    }

    @Test
    void fileDownloadCreatesThenAppendsForResume() throws IOException {
        Path file = Files.createTempFile("soulseek-download-", ".bin");
        try (Fixture first = new Fixture()) {
            Files.write(file, new byte[] {9, 9, 9});
            first.transfer.data = new byte[] {1, 2};
            first.waiter.response = CompletableFuture.completedFuture(new TransferResponse(26, 2));
            first.client
                    .downloadAsync("alice", "file", file.toString(), 2L, 0, 26, options(), CancellationToken.none())
                    .join();
            assertArrayEquals(new byte[] {1, 2}, Files.readAllBytes(file));
        }

        try (Fixture second = new Fixture()) {
            second.transfer.data = new byte[] {3, 4};
            second.waiter.response = CompletableFuture.completedFuture(new TransferResponse(27, 4));
            second.client
                    .downloadAsync("alice", "file", file.toString(), 4L, 2, 27, options(), CancellationToken.none())
                    .join();
            assertArrayEquals(new byte[] {1, 2, 3, 4}, Files.readAllBytes(file));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    void outputDisposalOptionIsHonored() {
        try (Fixture fixture = new Fixture()) {
            fixture.transfer.data = new byte[] {1};
            fixture.waiter.response = CompletableFuture.completedFuture(new TransferResponse(28, 1));
            CloseTrackingOutputStream disposable = new CloseTrackingOutputStream();
            fixture.client
                    .downloadAsync(
                            "alice",
                            "file",
                            () -> CompletableFuture.completedFuture(disposable),
                            1L,
                            0,
                            28,
                            options().withDisposalOptions(null, true),
                            CancellationToken.none())
                    .join();
            assertTrue(disposable.flushed.get());
            assertTrue(disposable.closed.get());

            fixture.transfer.data = new byte[] {2};
            CloseTrackingOutputStream retained = new CloseTrackingOutputStream();
            fixture.client
                    .downloadAsync(
                            "alice",
                            "other",
                            () -> CompletableFuture.completedFuture(retained),
                            1L,
                            0,
                            29,
                            options().withDisposalOptions(null, false),
                            CancellationToken.none())
                    .join();
            assertFalse(retained.closed.get());
        }
    }

    @Test
    void cleanupFailuresAreWarningsAndDoNotChangeSuccess() {
        DiagnosticProbe diagnostic = new DiagnosticProbe();
        try (Fixture fixture = new Fixture(diagnostic.proxy)) {
            fixture.transfer.data = new byte[] {1};
            fixture.waiter.response = CompletableFuture.completedFuture(new TransferResponse(32, 1));
            fixture.waiter.cancelFailure = new IllegalStateException("cancel failed");
            fixture.transfer.closeFailure = new IllegalStateException("close failed");
            FailingCleanupOutputStream output = new FailingCleanupOutputStream();

            Transfer transfer = fixture.client
                    .downloadAsync(
                            "alice",
                            "folder/file",
                            () -> CompletableFuture.completedFuture(output),
                            1L,
                            0,
                            32,
                            options().withDisposalOptions(null, true),
                            CancellationToken.none())
                    .join();

            assertTrue(transfer.getState().hasFlag(TransferStates.SUCCEEDED));
            assertTrue(diagnostic.warnings.stream().anyMatch(warning -> warning.contains("Failed to cancel wait")));
            assertTrue(diagnostic.warnings.stream()
                    .anyMatch(warning -> warning.contains("Failed to dispose transfer connection")));
            assertTrue(diagnostic.warnings.stream()
                    .anyMatch(warning -> warning.contains("Failed to determine final position")));
            assertTrue(diagnostic.warnings.stream()
                    .anyMatch(warning -> warning.contains("Failed to finalize output stream")));
        }
    }

    private static dev.slsk.options.DownloadStreamFactory outputFactory() {
        return () -> CompletableFuture.completedFuture(new ByteArrayOutputStream());
    }

    private static TransferOptions options() {
        return new TransferOptions().withDisposalOptions(null, false);
    }

    private static TransferInternal transfer(TransferDirection direction, String username, String filename, int token) {
        return new TransferInternal(direction, username, filename, token);
    }

    private static Throwable failureOf(CompletableFuture<?> future) {
        try {
            future.join();
            throw new AssertionError("Expected the future to fail");
        } catch (CancellationException failure) {
            return unwrapCompletionFailure(failure);
        } catch (CompletionException failure) {
            return unwrapCompletionFailure(failure);
        }
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
        private final SoulseekClient client;

        private Fixture() {
            this(null);
        }

        private Fixture(IDiagnosticFactory diagnostic) {
            client = new SoulseekClient(
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
            client.setStateForTest(SoulseekClientStates.CONNECTED.or(SoulseekClientStates.LOGGED_IN));
        }

        @Override
        public void close() {
            client.close();
        }
    }

    private static final class WaiterProbe {
        private CompletableFuture<TransferResponse> response =
                CompletableFuture.completedFuture(new TransferResponse(0, 0));
        private CompletableFuture<TransferRequest> startRequest = new CompletableFuture<>();
        private final List<WaitKey> cancelled = new ArrayList<>();
        private RuntimeException cancelFailure;
        private final IWaiter proxy = (IWaiter)
                Proxy.newProxyInstance(IWaiter.class.getClassLoader(), new Class<?>[] {IWaiter.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("waitAsync") && arguments != null) {
                for (Object argument : arguments) {
                    if (argument == TransferResponse.class) {
                        return response;
                    }
                    if (argument == UserAddressResponse.class) {
                        return CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT));
                    }
                }
                return CompletableFuture.completedFuture(null);
            }
            if (method.getName().equals("waitIndefinitelyAsync") && arguments != null) {
                for (Object argument : arguments) {
                    if (argument == TransferRequest.class) {
                        return startRequest;
                    }
                }
                return CompletableFuture.completedFuture(null);
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
        private final IMessageConnection message;
        private final Connection transfer;
        private CompletableFuture<Connection> awaitResult;
        private int awaitCalls;
        private int outgoingTransferCalls;
        private int outgoingToken;
        private final IPeerConnectionManager proxy = (IPeerConnectionManager) Proxy.newProxyInstance(
                IPeerConnectionManager.class.getClassLoader(),
                new Class<?>[] {IPeerConnectionManager.class},
                this::invoke);

        private PeerManagerProbe(IMessageConnection message, Connection transfer) {
            this.message = message;
            this.transfer = transfer;
            awaitResult = CompletableFuture.completedFuture(transfer);
        }

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("getOrAddMessageConnectionAsync")
                    && arguments != null
                    && arguments.length == 3
                    && arguments[0] instanceof String) {
                return CompletableFuture.completedFuture(message);
            }
            if (method.getName().equals("getTransferConnectionAsync")
                    && arguments != null
                    && arguments.length == 4
                    && arguments[0] instanceof String) {
                outgoingTransferCalls++;
                outgoingToken = (Integer) arguments[2];
                return CompletableFuture.completedFuture(transfer);
            }
            if (method.getName().equals("awaitTransferConnectionAsync")) {
                awaitCalls++;
                return awaitResult;
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class MessageConnectionProbe {
        private final List<OutgoingMessage> messages = new ArrayList<>();
        private final IMessageConnection proxy = (IMessageConnection) Proxy.newProxyInstance(
                IMessageConnection.class.getClassLoader(), new Class<?>[] {IMessageConnection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("writeAsync")
                    && arguments != null
                    && arguments.length == 2
                    && arguments[0] instanceof OutgoingMessage message) {
                messages.add(message);
                return CompletableFuture.completedFuture(null);
            }
            if (method.getName().equals("getState")) {
                return ConnectionState.CONNECTED;
            }
            if (method.getName().equals("getId")) {
                return UUID.randomUUID();
            }
            if (method.getName().equals("getIpEndPoint")) {
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
        private ConnectionEventListener<ConnectionDataEventArgs> dataReadListener;
        private ConnectionEventListener<ConnectionDisconnectedEventArgs> disconnectedListener;
        private final Connection proxy = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) throws IOException {
            if (method.getName().equals("writeAsync")
                    && arguments != null
                    && arguments.length == 2
                    && arguments[0] instanceof byte[] bytes) {
                offsetBytes = bytes.clone();
                return CompletableFuture.completedFuture(null);
            }
            if (method.getName().equals("readAsync")
                    && arguments != null
                    && arguments.length == 5
                    && arguments[0] instanceof Long length) {
                readLength = length;
                if (readHook != null) {
                    readHook.run();
                }
                if (disconnectOnRead != null) {
                    disconnectedListener.handle(
                            proxy, new ConnectionDisconnectedEventArgs("connection lost", disconnectOnRead));
                    return new CompletableFuture<>();
                }
                if (blockRead) {
                    return new CompletableFuture<>();
                }
                OutputStream output = (OutputStream) arguments[1];
                ConnectionGovernor governor = (ConnectionGovernor) arguments[2];
                ConnectionReporter reporter = (ConnectionReporter) arguments[3];
                CancellationToken token = (CancellationToken) arguments[4];
                int sourceOffset = 0;
                long transferred = 0;
                while (transferred < length) {
                    int attempted = (int) Math.min(Integer.MAX_VALUE, length - transferred);
                    int granted = governor == null
                            ? attempted
                            : governor.grantAsync(attempted, token).join();
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
                        dataReadListener.handle(proxy, new ConnectionDataEventArgs(transferred, length));
                    }
                }
                return CompletableFuture.completedFuture(null);
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
            if (method.getName().equals("getIpEndPoint")) {
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
        private final IDiagnosticFactory proxy = (IDiagnosticFactory) Proxy.newProxyInstance(
                IDiagnosticFactory.class.getClassLoader(), new Class<?>[] {IDiagnosticFactory.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("warning")) {
                warnings.add((String) arguments[0]);
            }
            return defaultValue(method.getReturnType());
        }
    }
}
