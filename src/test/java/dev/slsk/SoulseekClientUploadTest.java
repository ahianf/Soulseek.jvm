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
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.ConnectionReadException;
import dev.slsk.exceptions.DuplicateTokenException;
import dev.slsk.exceptions.DuplicateTransferException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.exceptions.TransferException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.messaging.messages.OutgoingMessage;
import dev.slsk.messaging.messages.TransferRequest;
import dev.slsk.messaging.messages.TransferResponse;
import dev.slsk.messaging.messages.UploadDenied;
import dev.slsk.messaging.messages.UploadFailed;
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
import dev.slsk.options.TransferOptions;
import dev.slsk.transfer.TransferInternal;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SoulseekClientUploadTest {
    private static final InetSocketAddress ENDPOINT = new InetSocketAddress(InetAddress.getLoopbackAddress(), 46011);

    @Test
    void validatesArgumentsSizeFactoryFileAndLoginState() {
        try (Fixture fixture = new Fixture()) {
            for (String bad : new String[] {null, "", " ", "\t"}) {
                assertThrows(
                        IllegalArgumentException.class,
                        () -> fixture.client.uploadAsync(bad, "file", 0, offset -> completedStream(new byte[0])));
                assertThrows(
                        IllegalArgumentException.class,
                        () -> fixture.client.uploadAsync("alice", bad, 0, offset -> completedStream(new byte[0])));
                assertThrows(IllegalArgumentException.class, () -> fixture.client.uploadAsync("alice", "file", bad));
            }
            assertThrows(
                    IllegalArgumentException.class,
                    () -> fixture.client.uploadAsync("alice", "file", -1, offset -> completedStream(new byte[0])));
            assertThrows(NullPointerException.class, () -> fixture.client.uploadAsync("alice", "file", 0, null));
            assertThrows(
                    UncheckedIOException.class,
                    () -> fixture.client.uploadAsync("alice", "file", "/missing/upload-file"));

            fixture.client.setStateForTest(SoulseekClientStates.DISCONNECTED);
            assertThrows(
                    IllegalStateException.class,
                    () -> fixture.client.uploadAsync("alice", "file", 0, offset -> completedStream(new byte[0])));
        }
    }

    @Test
    void permitsZeroSizeUploadAndUsesGivenCancellationToken() {
        try (Fixture fixture = new Fixture()) {
            CancellationTokenSource source = new CancellationTokenSource();
            fixture.transfer.size = 0;

            Transfer result = fixture.client
                    .uploadAsync(
                            "alice",
                            "empty",
                            0,
                            offset -> completedStream(new byte[0]),
                            41,
                            options(20),
                            source.getToken())
                    .join();

            assertEquals(0, result.getSize());
            assertSame(source.getToken(), fixture.message.lastToken);
            assertSame(source.getToken(), fixture.peerManager.transferToken);
            assertTrue(result.getState().hasFlag(TransferStates.SUCCEEDED));
        }
    }

    @Test
    void uploadsLocalFileContentsAndSize() throws IOException {
        Path file = Files.createTempFile("soulseek-upload-", ".bin");
        byte[] bytes = new byte[] {5, 4, 3, 2, 1};
        Files.write(file, bytes);
        try (Fixture fixture = new Fixture()) {
            Transfer result = fixture.client
                    .uploadAsync("alice", "remote", file.toString(), 7, options(20), CancellationToken.none())
                    .join();

            assertEquals(bytes.length, result.getSize());
            assertArrayEquals(bytes, fixture.transfer.written.toByteArray());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    void rejectsTokensUsedByUploadsOrDownloads() {
        try (Fixture fixture = new Fixture()) {
            fixture.client.getUploadsInternal().put(8, transfer(TransferDirection.UPLOAD, "other", "other", 8));
            assertThrows(
                    DuplicateTokenException.class,
                    () -> fixture.client.uploadAsync("alice", "file", 1, offset -> completedStream(new byte[] {1}), 8));

            fixture.client.getUploadsInternal().clear();
            fixture.client.getDownloadDictionary().put(9, transfer(TransferDirection.DOWNLOAD, "other", "other", 9));
            assertThrows(
                    DuplicateTokenException.class,
                    () -> fixture.client.uploadAsync("alice", "file", 1, offset -> completedStream(new byte[] {1}), 9));
        }
    }

    @Test
    void rejectsMatchingActiveUploadAndUniqueKey() {
        try (Fixture fixture = new Fixture()) {
            fixture.client.getUploadsInternal().put(8, transfer(TransferDirection.UPLOAD, "alice", "file", 8));
            assertThrows(
                    DuplicateTransferException.class,
                    () -> fixture.client.uploadAsync("alice", "file", 1, offset -> completedStream(new byte[] {1}), 9));

            fixture.client.getUploadsInternal().clear();
            fixture.client.getUniqueKeys().put("Upload:alice:file", true);
            assertThrows(
                    DuplicateTransferException.class,
                    () -> fixture.client.uploadAsync("alice", "file", 1, offset -> completedStream(new byte[] {1}), 9));
        }
    }

    @Test
    void allowsOnlyUsernameOrFilenameToMatch() {
        try (Fixture fixture = new Fixture()) {
            fixture.client.getUploadsInternal().put(8, transfer(TransferDirection.UPLOAD, "alice", "other", 8));
            fixture.client.getUploadsInternal().put(9, transfer(TransferDirection.UPLOAD, "other", "file", 9));

            Transfer result = fixture.client
                    .uploadAsync(
                            "alice",
                            "file",
                            1,
                            offset -> completedStream(new byte[] {1}),
                            10,
                            options(20),
                            CancellationToken.none())
                    .join();

            assertTrue(result.getState().hasFlag(TransferStates.SUCCEEDED));
        }
    }

    @Test
    void enqueueReturnsAfterLocalQueueingBeforeSlotAcquisition() {
        try (Fixture fixture = new Fixture()) {
            CompletableFuture<Void> slot = new CompletableFuture<>();
            TransferOptions options = options(20, (transfer, token) -> slot, null, null, null, null);

            CompletableFuture<Transfer> upload = fixture.client
                    .enqueueUploadAsync(
                            "alice",
                            "file",
                            1,
                            offset -> completedStream(new byte[] {1}),
                            12,
                            options,
                            CancellationToken.none())
                    .join();

            assertFalse(upload.isDone());
            assertTrue(fixture.client.getUploadsInternal().containsKey(12));
            slot.complete(null);
            assertTrue(upload.join().getState().hasFlag(TransferStates.SUCCEEDED));
        }
    }

    @Test
    void successfulUploadUsesProtocolOrderAndRaisesExpectedStates() {
        try (Fixture fixture = new Fixture()) {
            byte[] bytes = new byte[] {1, 2, 3, 4};
            List<TransferStates> optionStates = new ArrayList<>();
            List<TransferStates> eventStates = new ArrayList<>();
            List<Long> progress = new ArrayList<>();
            fixture.client.addTransferStateChangedListener((sender, eventArgs) ->
                    eventStates.add(eventArgs.getTransfer().getState()));
            fixture.client.addTransferProgressUpdatedListener(
                    (sender, eventArgs) -> progress.add(eventArgs.getTransfer().getBytesTransferred()));
            TransferOptions options = options(
                    20,
                    null,
                    null,
                    null,
                    change -> optionStates.add(change.transfer().getState()),
                    null);

            Transfer result = fixture.client
                    .uploadAsync(
                            "alice",
                            "remote\\file",
                            bytes.length,
                            offset -> completedStream(bytes),
                            22,
                            options,
                            CancellationToken.none())
                    .join();

            List<TransferStates> expected = List.of(
                    TransferStates.QUEUED.or(TransferStates.LOCALLY),
                    TransferStates.REQUESTED,
                    TransferStates.INITIALIZING,
                    TransferStates.IN_PROGRESS,
                    TransferStates.COMPLETED.or(TransferStates.SUCCEEDED));
            assertEquals(expected, optionStates);
            assertEquals(expected, eventStates);
            assertEquals(bytes.length, result.getBytesTransferred());
            assertArrayEquals(bytes, fixture.transfer.written.toByteArray());
            assertEquals(bytes.length, fixture.transfer.writeLength);
            TransferRequest request = assertInstanceOf(TransferRequest.class, fixture.message.messages.get(0));
            assertEquals(TransferDirection.UPLOAD, request.getDirection());
            assertEquals(22, request.getToken());
            assertEquals("remote\\file", request.getFilename());
            assertEquals(bytes.length, request.getFileSize());
            assertTrue(progress.contains(0L));
            assertTrue(progress.contains((long) bytes.length));
            assertFalse(fixture.client.getUploadsInternal().containsKey(22));
            assertFalse(fixture.client.getUniqueKeys().containsKey("Upload:alice:remote\\file"));
        }
    }

    @Test
    void seeksToResumeOffsetAndWritesRemainingLength() {
        try (Fixture fixture = new Fixture()) {
            fixture.transfer.offset = 2;
            byte[] bytes = new byte[] {1, 2, 3, 4, 5};

            Transfer result = fixture.client
                    .uploadAsync(
                            "alice",
                            "file",
                            bytes.length,
                            offset -> completedStream(bytes),
                            31,
                            options(20),
                            CancellationToken.none())
                    .join();

            assertEquals(2, result.getStartOffset());
            assertEquals(3, fixture.transfer.writeLength);
            assertArrayEquals(new byte[] {3, 4, 5}, fixture.transfer.written.toByteArray());
            assertEquals(5, result.getBytesTransferred());
        }
    }

    @Test
    void skipsWriteWhenOffsetEqualsSize() {
        try (Fixture fixture = new Fixture()) {
            fixture.transfer.offset = 3;

            Transfer result = fixture.client
                    .uploadAsync(
                            "alice",
                            "file",
                            3,
                            offset -> completedStream(new byte[] {1, 2, 3}),
                            32,
                            options(20),
                            CancellationToken.none())
                    .join();

            assertEquals(0, fixture.transfer.writeCalls);
            assertEquals(3, result.getBytesTransferred());
        }
    }

    @Test
    void rejectsOffsetBeyondSizeAndNonSeekableResume() {
        try (Fixture fixture = new Fixture()) {
            fixture.transfer.offset = 4;
            Throwable tooLong = failureOf(fixture.client.uploadAsync(
                    "alice",
                    "file",
                    3,
                    offset -> completedStream(new byte[] {1, 2, 3}),
                    33,
                    options(20),
                    CancellationToken.none()));
            assertInstanceOf(SoulseekClientException.class, tooLong);
            assertInstanceOf(TransferException.class, tooLong.getCause());

            fixture.transfer.offset = 1;
            Throwable notSeekable = failureOf(fixture.client.uploadAsync(
                    "alice",
                    "other",
                    2,
                    offset -> CompletableFuture.completedFuture(new InputStream() {
                        @Override
                        public int read() {
                            return -1;
                        }
                    }),
                    34,
                    options(20),
                    CancellationToken.none()));
            assertInstanceOf(SoulseekClientException.class, notSeekable);
        }
    }

    @Test
    void canDisableAutomaticSeeking() {
        try (Fixture fixture = new Fixture()) {
            fixture.transfer.offset = 1;
            TransferOptions options = new TransferOptions(null, null, null, null, null, null, 20, false);

            Transfer result = fixture.client
                    .uploadAsync(
                            "alice",
                            "file",
                            3,
                            offset -> completedStream(new byte[] {9, 8, 7}),
                            35,
                            options,
                            CancellationToken.none())
                    .join();

            assertArrayEquals(new byte[] {9, 8}, fixture.transfer.written.toByteArray());
            assertEquals(3, result.getBytesTransferred());
        }
    }

    @Test
    void invokesGovernorThenReporterAndReturnsUnusedTokens() {
        try (Fixture fixture = new Fixture()) {
            List<Integer> governorRequests = new ArrayList<>();
            List<List<Integer>> reports = new ArrayList<>();
            TransferOptions options = options(
                    20,
                    null,
                    (transfer, requested, token) -> {
                        governorRequests.add(requested);
                        return CompletableFuture.completedFuture(requested);
                    },
                    (transfer, attempted, granted, actual) -> reports.add(List.of(attempted, granted, actual)),
                    null,
                    null);
            fixture.transfer.maximumActualPerIteration = 2;

            fixture.client
                    .uploadAsync(
                            "alice",
                            "file",
                            5,
                            offset -> completedStream(new byte[] {1, 2, 3, 4, 5}),
                            36,
                            options,
                            CancellationToken.none())
                    .join();

            assertFalse(governorRequests.isEmpty());
            assertFalse(reports.isEmpty());
            assertTrue(reports.stream().anyMatch(report -> report.get(1) > report.get(2)));
        }
    }

    @Test
    void rejectionSetsFinalStatePreservesExceptionAndNotifiesPeer() {
        try (Fixture fixture = new Fixture()) {
            fixture.waiter.transferResponse = CompletableFuture.completedFuture(new TransferResponse(40, "not shared"));
            List<Transfer> terminal = new ArrayList<>();
            fixture.client.addTransferStateChangedListener((sender, eventArgs) -> {
                if (eventArgs.getTransfer().getState().hasFlag(TransferStates.COMPLETED)) {
                    terminal.add(eventArgs.getTransfer());
                }
            });

            Throwable failure = failureOf(fixture.client.uploadAsync(
                    "alice",
                    "file",
                    1,
                    offset -> completedStream(new byte[] {1}),
                    40,
                    options(20),
                    CancellationToken.none()));

            assertInstanceOf(TransferRejectedException.class, failure);
            assertEquals(1, terminal.size());
            assertTrue(terminal.get(0).getState().hasFlag(TransferStates.REJECTED));
            assertSame(failure, terminal.get(0).getException());
            assertInstanceOf(UploadFailed.class, fixture.message.messages.get(fixture.message.messages.size() - 1));
        }
    }

    @Test
    void cancellationSetsFinalStateWritesDeniedAndReleasesSlot() {
        try (Fixture fixture = new Fixture()) {
            CancellationException cancellation = new CancellationException("cancelled");
            AtomicInteger released = new AtomicInteger();
            List<Transfer> terminal = new ArrayList<>();
            TransferOptions options = options(
                    20,
                    (transfer, token) -> CompletableFuture.failedFuture(cancellation),
                    null,
                    null,
                    change -> {
                        if (change.transfer().getState().hasFlag(TransferStates.COMPLETED)) {
                            terminal.add(change.transfer());
                        }
                    },
                    transfer -> released.incrementAndGet());

            Throwable failure = failureOf(fixture.client.uploadAsync(
                    "alice",
                    "file",
                    1,
                    offset -> completedStream(new byte[] {1}),
                    41,
                    options,
                    CancellationToken.none()));

            assertSame(cancellation, failure);
            assertTrue(terminal.get(0).getState().hasFlag(TransferStates.CANCELLED));
            assertEquals(0, released.get(), "a slot that was not acquired is not released");
            assertInstanceOf(UploadDenied.class, fixture.message.messages.get(fixture.message.messages.size() - 1));
        }
    }

    @Test
    void timeoutIsPreservedAndProducesTimedOutState() {
        try (Fixture fixture = new Fixture()) {
            TimeoutException timeout = new TimeoutException("timed out");
            fixture.waiter.transferResponse = CompletableFuture.failedFuture(timeout);
            List<Transfer> terminal = new ArrayList<>();
            TransferOptions options = options(
                    20,
                    null,
                    null,
                    null,
                    change -> {
                        if (change.transfer().getState().hasFlag(TransferStates.COMPLETED)) {
                            terminal.add(change.transfer());
                        }
                    },
                    null);

            Throwable failure = failureOf(fixture.client.uploadAsync(
                    "alice",
                    "file",
                    1,
                    offset -> completedStream(new byte[] {1}),
                    42,
                    options,
                    CancellationToken.none()));

            assertSame(timeout, failure);
            assertTrue(terminal.get(0).getState().hasFlag(TransferStates.TIMED_OUT));
        }
    }

    @Test
    void slotFailureIsWrappedAndAcquiredSlotIsReleased() {
        try (Fixture fixture = new Fixture()) {
            RuntimeException slotFailure = new RuntimeException("slot failed");
            Throwable failure = failureOf(fixture.client.uploadAsync(
                    "alice",
                    "file",
                    1,
                    offset -> completedStream(new byte[] {1}),
                    43,
                    options(
                            20,
                            (transfer, token) -> CompletableFuture.failedFuture(slotFailure),
                            null,
                            null,
                            null,
                            null),
                    CancellationToken.none()));
            SoulseekClientException mapped = assertInstanceOf(SoulseekClientException.class, failure);
            assertInstanceOf(TransferException.class, mapped.getCause());

            AtomicInteger released = new AtomicInteger();
            fixture.client
                    .uploadAsync(
                            "alice",
                            "other",
                            1,
                            offset -> completedStream(new byte[] {1}),
                            44,
                            options(20, null, null, null, null, transfer -> {
                                released.incrementAndGet();
                                throw new RuntimeException("ignored");
                            }),
                            CancellationToken.none())
                    .join();
            assertEquals(1, released.get());
        }
    }

    @Test
    void streamDisposalOptionIsHonored() {
        try (Fixture fixture = new Fixture()) {
            CloseTrackingInputStream disposable = new CloseTrackingInputStream(new byte[] {1});
            fixture.client
                    .uploadAsync(
                            "alice",
                            "file",
                            1,
                            offset -> CompletableFuture.completedFuture(disposable),
                            45,
                            options(20).withDisposalOptions(true),
                            CancellationToken.none())
                    .join();
            assertTrue(disposable.closed.get());

            CloseTrackingInputStream retained = new CloseTrackingInputStream(new byte[] {2});
            fixture.client
                    .uploadAsync(
                            "alice",
                            "other",
                            1,
                            offset -> CompletableFuture.completedFuture(retained),
                            46,
                            options(20).withDisposalOptions(false),
                            CancellationToken.none())
                    .join();
            assertFalse(retained.closed.get());
        }
    }

    @Test
    void malformedOffsetAndWriteFailureAreWrappedAndCleanedUp() {
        try (Fixture fixture = new Fixture()) {
            fixture.transfer.offsetBytes = new byte[] {1, 2};
            Throwable malformed = failureOf(fixture.client.uploadAsync(
                    "alice",
                    "file",
                    1,
                    offset -> completedStream(new byte[] {1}),
                    47,
                    options(20),
                    CancellationToken.none()));
            assertInstanceOf(SoulseekClientException.class, malformed);
            assertFalse(fixture.client.getUploadsInternal().containsKey(47));

            fixture.transfer.offsetBytes = null;
            fixture.transfer.writeFailure = new IOException("write failed");
            Throwable write = failureOf(fixture.client.uploadAsync(
                    "alice",
                    "other",
                    1,
                    offset -> completedStream(new byte[] {1}),
                    48,
                    options(20),
                    CancellationToken.none()));
            assertInstanceOf(SoulseekClientException.class, write);
            assertSame(fixture.transfer.writeFailure, write.getCause());
            assertFalse(fixture.client.getUniqueKeys().containsKey("Upload:alice:other"));
        }
    }

    @Test
    void unexpectedTransferDisconnectIsWrappedAndRecorded() {
        try (Fixture fixture = new Fixture()) {
            IOException socketFailure = new IOException("socket failed");
            fixture.transfer.disconnectOnWrite = socketFailure;
            List<Transfer> terminal = new ArrayList<>();
            fixture.client.addTransferStateChangedListener((sender, eventArgs) -> {
                if (eventArgs.getTransfer().getState().hasFlag(TransferStates.COMPLETED)) {
                    terminal.add(eventArgs.getTransfer());
                }
            });

            Throwable failure = failureOf(fixture.client.uploadAsync(
                    "alice",
                    "file",
                    1,
                    offset -> completedStream(new byte[] {1}),
                    49,
                    options(20),
                    CancellationToken.none()));

            SoulseekClientException mapped = assertInstanceOf(SoulseekClientException.class, failure);
            ConnectionException connection = assertInstanceOf(ConnectionException.class, mapped.getCause());
            assertSame(socketFailure, connection.getCause());
            assertSame(connection, terminal.get(0).getException());
            assertTrue(terminal.get(0).getState().hasFlag(TransferStates.ERRORED));
        }
    }

    private static CompletableFuture<InputStream> completedStream(byte[] bytes) {
        return CompletableFuture.completedFuture(new ByteArrayInputStream(bytes));
    }

    private static TransferInternal transfer(TransferDirection direction, String username, String filename, int token) {
        return new TransferInternal(direction, username, filename, token);
    }

    private static TransferOptions options(int linger) {
        return options(linger, null, null, null, null, null);
    }

    private static TransferOptions options(
            int linger,
            dev.slsk.options.TransferSlotAwaiter slotAwaiter,
            dev.slsk.options.TransferGovernor governor,
            dev.slsk.options.TransferReporter reporter,
            dev.slsk.options.TransferStateChangedCallback stateChanged,
            dev.slsk.options.TransferSlotReleasedCallback slotReleased) {
        return new TransferOptions(governor, stateChanged, null, slotAwaiter, slotReleased, reporter, linger);
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
        private final SoulseekClient client = new SoulseekClient(
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
                null,
                null,
                null,
                null);

        private Fixture() {
            client.setStateForTest(SoulseekClientStates.CONNECTED.or(SoulseekClientStates.LOGGED_IN));
        }

        @Override
        public void close() {
            client.close();
        }
    }

    private static final class WaiterProbe {
        private CompletableFuture<TransferResponse> transferResponse =
                CompletableFuture.completedFuture(new TransferResponse(0));
        private final List<WaitKey> keys = new ArrayList<>();
        private final IWaiter proxy = (IWaiter)
                Proxy.newProxyInstance(IWaiter.class.getClassLoader(), new Class<?>[] {IWaiter.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("waitAsync")) {
                if (arguments != null && arguments.length > 0 && arguments[0] instanceof WaitKey key) {
                    keys.add(key);
                }
                if (arguments != null) {
                    for (Object argument : arguments) {
                        if (argument == TransferResponse.class) {
                            return transferResponse;
                        }
                        if (argument == UserAddressResponse.class) {
                            return CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT));
                        }
                    }
                }
                return CompletableFuture.completedFuture(null);
            }
            if (method.getName().equals("waitIndefinitelyAsync")) {
                return CompletableFuture.completedFuture(null);
            }
            if (method.getName().equals("getDefaultTimeout")) {
                return 5_000;
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class PeerManagerProbe {
        private final IMessageConnection messageConnection;
        private final Connection transferConnection;
        private CancellationToken transferToken;
        private final IPeerConnectionManager proxy = (IPeerConnectionManager) Proxy.newProxyInstance(
                IPeerConnectionManager.class.getClassLoader(),
                new Class<?>[] {IPeerConnectionManager.class},
                this::invoke);

        private PeerManagerProbe(IMessageConnection messageConnection, Connection transferConnection) {
            this.messageConnection = messageConnection;
            this.transferConnection = transferConnection;
        }

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("getOrAddMessageConnectionAsync")
                    && arguments != null
                    && arguments.length == 3
                    && arguments[0] instanceof String) {
                return CompletableFuture.completedFuture(messageConnection);
            }
            if (method.getName().equals("getTransferConnectionAsync")
                    && arguments != null
                    && arguments.length == 4
                    && arguments[0] instanceof String) {
                transferToken = (CancellationToken) arguments[3];
                return CompletableFuture.completedFuture(transferConnection);
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class MessageConnectionProbe {
        private final List<OutgoingMessage> messages = new ArrayList<>();
        private CancellationToken lastToken;
        private final IMessageConnection proxy = (IMessageConnection) Proxy.newProxyInstance(
                IMessageConnection.class.getClassLoader(), new Class<?>[] {IMessageConnection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("writeAsync")
                    && arguments != null
                    && arguments.length == 2
                    && arguments[0] instanceof OutgoingMessage message) {
                messages.add(message);
                lastToken = (CancellationToken) arguments[1];
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
        private long offset;
        private byte[] offsetBytes;
        private long size;
        private long writeLength;
        private int writeCalls;
        private int maximumActualPerIteration = Integer.MAX_VALUE;
        private Throwable writeFailure;
        private Exception disconnectOnWrite;
        private final ByteArrayOutputStream written = new ByteArrayOutputStream();
        private ConnectionEventListener<ConnectionDataEventArgs> dataWrittenListener;
        private ConnectionEventListener<ConnectionDisconnectedEventArgs> disconnectedListener;
        private final Connection proxy = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) throws IOException {
            if (method.getName().equals("readAsync")
                    && arguments != null
                    && arguments.length == 2
                    && arguments[0] instanceof Long length) {
                if (length == 8) {
                    byte[] bytes = offsetBytes == null
                            ? ByteBuffer.allocate(8)
                                    .order(ByteOrder.LITTLE_ENDIAN)
                                    .putLong(offset)
                                    .array()
                            : offsetBytes;
                    return CompletableFuture.completedFuture(bytes);
                }
                return CompletableFuture.failedFuture(new ConnectionReadException("peer disconnected"));
            }
            if (method.getName().equals("writeAsync")
                    && arguments != null
                    && arguments.length == 5
                    && arguments[0] instanceof Long length) {
                writeCalls++;
                writeLength = length;
                if (disconnectOnWrite != null) {
                    disconnectedListener.handle(
                            proxy, new ConnectionDisconnectedEventArgs("connection lost", disconnectOnWrite));
                    return new CompletableFuture<>();
                }
                if (writeFailure != null) {
                    return CompletableFuture.failedFuture(writeFailure);
                }
                InputStream stream = (InputStream) arguments[1];
                ConnectionGovernor governor = (ConnectionGovernor) arguments[2];
                ConnectionReporter reporter = (ConnectionReporter) arguments[3];
                CancellationToken token = (CancellationToken) arguments[4];
                long transferred = 0;
                while (transferred < length) {
                    int attempted = (int) Math.min(Integer.MAX_VALUE, length - transferred);
                    int granted = governor == null
                            ? attempted
                            : governor.grantAsync(attempted, token).join();
                    int target = Math.min(granted, maximumActualPerIteration);
                    byte[] buffer = stream.readNBytes(target);
                    written.write(buffer);
                    transferred += buffer.length;
                    if (reporter != null) {
                        reporter.report(attempted, granted, buffer.length);
                    }
                    if (buffer.length == 0) {
                        throw new IOException("stream ended early");
                    }
                    if (dataWrittenListener != null) {
                        dataWrittenListener.handle(proxy, new ConnectionDataEventArgs(transferred, length));
                    }
                }
                size = transferred;
                return CompletableFuture.completedFuture(null);
            }
            if (method.getName().equals("addDataWrittenListener")) {
                dataWrittenListener = cast(arguments[0]);
                return null;
            }
            if (method.getName().equals("removeDataWrittenListener")) {
                if (dataWrittenListener == arguments[0]) {
                    dataWrittenListener = null;
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

    private static final class CloseTrackingInputStream extends ByteArrayInputStream {
        private final AtomicBoolean closed = new AtomicBoolean();

        private CloseTrackingInputStream(byte[] bytes) {
            super(bytes);
        }

        @Override
        public void close() throws IOException {
            closed.set(true);
            super.close();
        }
    }
}
