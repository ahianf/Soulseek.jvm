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

import dev.slsk.CancellationController;
import dev.slsk.CancellationSignal;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.ConnectionReadException;
import dev.slsk.exceptions.DuplicateTokenException;
import dev.slsk.exceptions.DuplicateTransferException;
import dev.slsk.exceptions.NoResponseException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.exceptions.TransferException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.internal.EngineEvents.Kind;
import dev.slsk.internal.common.Blocking;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.messaging.messages.OutgoingMessage;
import dev.slsk.internal.messaging.messages.TransferRequest;
import dev.slsk.internal.messaging.messages.TransferResponse;
import dev.slsk.internal.messaging.messages.UploadDenied;
import dev.slsk.internal.messaging.messages.UploadFailed;
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
import dev.slsk.internal.options.TransferOptions;
import dev.slsk.internal.transfer.TransferInternal;
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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class EngineUploadTest {
    private static final InetSocketAddress ENDPOINT = new InetSocketAddress(InetAddress.getLoopbackAddress(), 46011);

    @Test
    void validatesArgumentsSizeFactoryFileAndLoginState() {
        try (Fixture fixture = new Fixture()) {
            for (String bad : new String[] {null, "", " ", "\t"}) {
                assertThrows(
                        IllegalArgumentException.class,
                        () -> Blocking.await(fixture.client
                                .transfers()
                                .upload(UploadRequest.fromStream(bad, "file", 0, offset -> completedStream(new byte[0]))
                                        .build())));
                assertThrows(
                        IllegalArgumentException.class,
                        () -> Blocking.await(fixture.client
                                .transfers()
                                .upload(UploadRequest.fromStream(
                                                "alice", bad, 0, offset -> completedStream(new byte[0]))
                                        .build())));
                assertThrows(
                        IllegalArgumentException.class,
                        () -> Blocking.await(fixture.client
                                .transfers()
                                .upload(UploadRequest.fromFile("alice", "file", bad)
                                        .build())));
            }
            assertThrows(
                    IllegalArgumentException.class,
                    () -> Blocking.await(fixture.client
                            .transfers()
                            .upload(UploadRequest.fromStream(
                                            "alice", "file", -1, offset -> completedStream(new byte[0]))
                                    .build())));
            assertThrows(
                    NullPointerException.class,
                    () -> Blocking.await(fixture.client
                            .transfers()
                            .upload(UploadRequest.fromStream("alice", "file", 0, null)
                                    .build())));
            assertThrows(
                    UncheckedIOException.class,
                    () -> Blocking.await(fixture.client
                            .transfers()
                            .upload(UploadRequest.fromFile("alice", "file", "/missing/upload-file")
                                    .build())));

            fixture.client.setStateForTest(SoulseekClientState.DISCONNECTED);
            assertThrows(
                    IllegalStateException.class,
                    () -> Blocking.await(fixture.client
                            .transfers()
                            .upload(UploadRequest.fromStream("alice", "file", 0, offset -> completedStream(new byte[0]))
                                    .build())));
        }
    }

    @Test
    void permitsZeroSizeUploadAndUsesGivenCancellationSignal() {
        try (Fixture fixture = new Fixture()) {
            CancellationController source = new CancellationController();
            fixture.transfer.size = 0;

            Transfer result = Blocking.await(fixture.client
                    .transfers()
                    .upload(UploadRequest.fromStream("alice", "empty", 0, offset -> completedStream(new byte[0]))
                            .token(41)
                            .options(options(20))
                            .cancellation(source.getSignal())
                            .build()));

            assertEquals(0, result.getSize());
            assertSame(source.getSignal(), fixture.message.lastToken);
            assertSame(source.getSignal(), fixture.peerManager.transferToken);
            assertTrue(result.getState().contains(TransferState.SUCCEEDED));
        }
    }

    @Test
    void uploadsLocalFileContentsAndSize() throws IOException {
        Path file = Files.createTempFile("soulseek-upload-", ".bin");
        byte[] bytes = new byte[] {5, 4, 3, 2, 1};
        Files.write(file, bytes);
        try (Fixture fixture = new Fixture()) {
            Transfer result = Blocking.await(fixture.client
                    .transfers()
                    .upload(UploadRequest.fromFile("alice", "remote", file.toString())
                            .token(7)
                            .options(options(20))
                            .build()));

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
                    () -> Blocking.await(fixture.client
                            .transfers()
                            .upload(UploadRequest.fromStream(
                                            "alice", "file", 1, offset -> completedStream(new byte[] {1}))
                                    .token(8)
                                    .build())));

            fixture.client.getUploadsInternal().clear();
            fixture.client.getDownloadDictionary().put(9, transfer(TransferDirection.DOWNLOAD, "other", "other", 9));
            assertThrows(
                    DuplicateTokenException.class,
                    () -> Blocking.await(fixture.client
                            .transfers()
                            .upload(UploadRequest.fromStream(
                                            "alice", "file", 1, offset -> completedStream(new byte[] {1}))
                                    .token(9)
                                    .build())));
        }
    }

    @Test
    void rejectsMatchingActiveUploadAndUniqueKey() {
        try (Fixture fixture = new Fixture()) {
            fixture.client.getUploadsInternal().put(8, transfer(TransferDirection.UPLOAD, "alice", "file", 8));
            assertThrows(
                    DuplicateTransferException.class,
                    () -> Blocking.await(fixture.client
                            .transfers()
                            .upload(UploadRequest.fromStream(
                                            "alice", "file", 1, offset -> completedStream(new byte[] {1}))
                                    .token(9)
                                    .build())));

            fixture.client.getUploadsInternal().clear();
            fixture.client.getUniqueKeys().put("Upload:alice:file", true);
            assertThrows(
                    DuplicateTransferException.class,
                    () -> Blocking.await(fixture.client
                            .transfers()
                            .upload(UploadRequest.fromStream(
                                            "alice", "file", 1, offset -> completedStream(new byte[] {1}))
                                    .token(9)
                                    .build())));
        }
    }

    @Test
    void allowsOnlyUsernameOrFilenameToMatch() {
        try (Fixture fixture = new Fixture()) {
            fixture.client.getUploadsInternal().put(8, transfer(TransferDirection.UPLOAD, "alice", "other", 8));
            fixture.client.getUploadsInternal().put(9, transfer(TransferDirection.UPLOAD, "other", "file", 9));

            Transfer result = Blocking.await(fixture.client
                    .transfers()
                    .upload(UploadRequest.fromStream("alice", "file", 1, offset -> completedStream(new byte[] {1}))
                            .token(10)
                            .options(options(20))
                            .build()));

            assertTrue(result.getState().contains(TransferState.SUCCEEDED));
        }
    }

    @Test
    void enqueueReturnsAfterLocalQueueingBeforeSlotAcquisition() {
        try (Fixture fixture = new Fixture()) {
            CompletableFuture<Void> slot = new CompletableFuture<>();
            TransferOptions options = options(20, null, null, null);

            TransferHandle upload = new TransferHandle(Blocking.await(fixture.client
                    .transfers()
                    .enqueueUpload(
                            UploadRequest.fromStream("alice", "file", 1, offset -> completedStream(new byte[] {1}))
                                    .token(12)
                                    .options(options)
                                    .build())));

            assertFalse(upload.isDone());
            assertTrue(fixture.client.getUploadsInternal().containsKey(12));
            slot.complete(null);
            assertTrue(upload.await().getState().contains(TransferState.SUCCEEDED));
        }
    }

    @Test
    void successfulUploadUsesProtocolOrderAndRaisesExpectedStates() {
        try (Fixture fixture = new Fixture()) {
            byte[] bytes = new byte[] {1, 2, 3, 4};
            List<TransferState> optionStates = new ArrayList<>();
            List<TransferState> eventStates = new ArrayList<>();
            List<Long> progress = new ArrayList<>();
            fixture.client
                    .events()
                    .on(
                            Kind.TRANSFER_STATE_CHANGED,
                            (dev.slsk.internal.events.TransferStateChangedEvent eventData) ->
                                    eventStates.add(eventData.getTransfer().getState()));
            fixture.client
                    .events()
                    .on(
                            Kind.TRANSFER_PROGRESS_UPDATED,
                            (dev.slsk.internal.events.TransferProgressUpdatedEvent eventData) ->
                                    progress.add(eventData.getTransfer().getBytesTransferred()));
            TransferOptions options = options(
                    20, null, change -> optionStates.add(change.transfer().getState()), null);

            Transfer result = Blocking.await(fixture.client
                    .transfers()
                    .upload(UploadRequest.fromStream(
                                    "alice", "remote\\file", bytes.length, offset -> completedStream(bytes))
                            .token(22)
                            .options(options)
                            .build()));

            List<TransferState> expected = List.of(
                    TransferState.QUEUED.or(TransferState.LOCALLY),
                    TransferState.REQUESTED,
                    TransferState.INITIALIZING,
                    TransferState.IN_PROGRESS,
                    TransferState.COMPLETED.or(TransferState.SUCCEEDED));
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

            Transfer result = Blocking.await(fixture.client
                    .transfers()
                    .upload(UploadRequest.fromStream("alice", "file", bytes.length, offset -> completedStream(bytes))
                            .token(31)
                            .options(options(20))
                            .build()));

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

            Transfer result = Blocking.await(fixture.client
                    .transfers()
                    .upload(UploadRequest.fromStream(
                                    "alice", "file", 3, offset -> completedStream(new byte[] {1, 2, 3}))
                            .token(32)
                            .options(options(20))
                            .build()));

            assertEquals(0, fixture.transfer.writeCalls);
            assertEquals(3, result.getBytesTransferred());
        }
    }

    @Test
    void rejectsOffsetBeyondSizeAndNonSeekableResume() {
        try (Fixture fixture = new Fixture()) {
            fixture.transfer.offset = 4;
            Throwable tooLong = failureOf(() -> Blocking.await(fixture.client
                    .transfers()
                    .upload(UploadRequest.fromStream(
                                    "alice", "file", 3, offset -> completedStream(new byte[] {1, 2, 3}))
                            .token(33)
                            .options(options(20))
                            .build())));
            assertInstanceOf(SoulseekClientException.class, tooLong);
            assertInstanceOf(TransferException.class, tooLong.getCause());

            fixture.transfer.offset = 1;
            Throwable notSeekable = failureOf(() -> Blocking.await(fixture.client
                    .transfers()
                    .upload(UploadRequest.fromStream("alice", "other", 2, offset -> (new InputStream() {
                                @Override
                                public int read() {
                                    return -1;
                                }
                            }))
                            .token(34)
                            .options(options(20))
                            .build())));
            assertInstanceOf(SoulseekClientException.class, notSeekable);
        }
    }

    @Test
    void canDisableAutomaticSeeking() {
        try (Fixture fixture = new Fixture()) {
            fixture.transfer.offset = 1;
            TransferOptions options = new TransferOptions(null, null, null, null, 20, false);

            Transfer result = Blocking.await(fixture.client
                    .transfers()
                    .upload(UploadRequest.fromStream(
                                    "alice", "file", 3, offset -> completedStream(new byte[] {9, 8, 7}))
                            .token(35)
                            .options(options)
                            .build()));

            assertArrayEquals(new byte[] {9, 8}, fixture.transfer.written.toByteArray());
            assertEquals(3, result.getBytesTransferred());
        }
    }

    @Test
    void reportsAttemptedGrantedAndActualAndReturnsUnusedTokens() {
        try (Fixture fixture = new Fixture()) {
            List<List<Integer>> reports = new ArrayList<>();
            TransferOptions options = options(
                    20,
                    (transfer, attempted, granted, actual) -> reports.add(List.of(attempted, granted, actual)),
                    null,
                    null);
            fixture.transfer.maximumActualPerIteration = 2;

            Blocking.await(fixture.client
                    .transfers()
                    .upload(UploadRequest.fromStream(
                                    "alice", "file", 5, offset -> completedStream(new byte[] {1, 2, 3, 4, 5}))
                            .token(36)
                            .options(options)
                            .build()));

            assertFalse(reports.isEmpty());
            assertTrue(reports.stream().anyMatch(report -> report.get(1) > report.get(2)));
        }
    }

    @Test
    void rejectionSetsFinalStatePreservesExceptionAndNotifiesPeer() {
        try (Fixture fixture = new Fixture()) {
            fixture.waiter.transferResponse = CompletableFuture.completedFuture(new TransferResponse(40, "not shared"));
            List<Transfer> terminal = new ArrayList<>();
            fixture.client
                    .events()
                    .on(Kind.TRANSFER_STATE_CHANGED, (dev.slsk.internal.events.TransferStateChangedEvent eventData) -> {
                        if (eventData.getTransfer().getState().contains(TransferState.COMPLETED)) {
                            terminal.add(eventData.getTransfer());
                        }
                    });

            Throwable failure = failureOf(() -> Blocking.await(fixture.client
                    .transfers()
                    .upload(UploadRequest.fromStream("alice", "file", 1, offset -> completedStream(new byte[] {1}))
                            .token(40)
                            .options(options(20))
                            .build())));

            assertInstanceOf(TransferRejectedException.class, failure);
            assertEquals(1, terminal.size());
            assertTrue(terminal.get(0).getState().contains(TransferState.REJECTED));
            assertSame(failure, terminal.get(0).getException());
            assertInstanceOf(UploadFailed.class, fixture.message.messages.get(fixture.message.messages.size() - 1));
        }
    }

    /**
     * Cancellation used to be provoked by a slot awaiter that failed. There is
     * no pluggable awaiter now — the upload policy is the slot gate — so it is
     * provoked the way a caller provokes it, through the signal.
     */
    @Test
    void cancellationSetsFinalStateWritesDeniedAndReleasesSlot() {
        try (Fixture fixture = new Fixture()) {
            dev.slsk.CancellationController cancelled = new dev.slsk.CancellationController();
            cancelled.cancel();
            AtomicInteger released = new AtomicInteger();
            List<Transfer> terminal = new ArrayList<>();
            TransferOptions options = options(
                    20,
                    null,
                    change -> {
                        if (change.transfer().getState().contains(TransferState.COMPLETED)) {
                            terminal.add(change.transfer());
                        }
                    },
                    transfer -> released.incrementAndGet());

            Throwable failure = failureOf(() -> Blocking.await(fixture.client
                    .transfers()
                    .upload(UploadRequest.fromStream("alice", "file", 1, offset -> completedStream(new byte[] {1}))
                            .token(41)
                            .options(options)
                            .cancellation(cancelled.getSignal())
                            .build())));

            // Cancellation surfaces as CancellationException; the exact
            // instance was observable only while the future was the return
            // value. See the matching note in EngineDownloadTest.
            assertInstanceOf(CancellationException.class, failure);
            assertTrue(terminal.get(0).getState().contains(TransferState.CANCELLED));
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
                    change -> {
                        if (change.transfer().getState().contains(TransferState.COMPLETED)) {
                            terminal.add(change.transfer());
                        }
                    },
                    null);

            Throwable failure = failureOf(() -> Blocking.await(fixture.client
                    .transfers()
                    .upload(UploadRequest.fromStream("alice", "file", 1, offset -> completedStream(new byte[] {1}))
                            .token(42)
                            .options(options)
                            .build())));

            assertSame(
                    timeout,
                    assertInstanceOf(NoResponseException.class, failure).getCause());
            assertTrue(terminal.get(0).getState().contains(TransferState.TIMED_OUT));
        }
    }

    /**
     * The failing slot awaiter this used to inject is gone with the awaiter. A
     * source that will not open is the failure that remains, and the property
     * that mattered — a released slot is released even when the release
     * callback itself throws — is unchanged.
     */
    @Test
    void aSourceThatWillNotOpenIsWrappedAndTheSlotIsStillReleased() {
        try (Fixture fixture = new Fixture()) {
            Throwable failure = failureOf(() -> Blocking.await(fixture.client
                    .transfers()
                    .upload(UploadRequest.fromStream("alice", "file", 1, offset -> {
                                throw new java.io.UncheckedIOException(new java.io.IOException("source is gone"));
                            })
                            .token(43)
                            .options(options(20))
                            .build())));
            assertInstanceOf(SoulseekClientException.class, failure);

            AtomicInteger released = new AtomicInteger();
            Blocking.await(fixture.client
                    .transfers()
                    .upload(UploadRequest.fromStream("alice", "other", 1, offset -> completedStream(new byte[] {1}))
                            .token(44)
                            .options(options(20, null, null, transfer -> {
                                released.incrementAndGet();
                                throw new RuntimeException("ignored");
                            }))
                            .build()));
            assertEquals(1, released.get());
        }
    }

    @Test
    void streamDisposalOptionIsHonored() {
        try (Fixture fixture = new Fixture()) {
            CloseTrackingInputStream disposable = new CloseTrackingInputStream(new byte[] {1});
            Blocking.await(fixture.client
                    .transfers()
                    .upload(UploadRequest.fromStream("alice", "file", 1, offset -> disposable)
                            .token(45)
                            .options(options(20).withDisposalOptions(true))
                            .build()));
            assertTrue(disposable.closed.get());

            CloseTrackingInputStream retained = new CloseTrackingInputStream(new byte[] {2});
            Blocking.await(fixture.client
                    .transfers()
                    .upload(UploadRequest.fromStream("alice", "other", 1, offset -> retained)
                            .token(46)
                            .options(options(20).withDisposalOptions(false))
                            .build()));
            assertFalse(retained.closed.get());
        }
    }

    @Test
    void malformedOffsetAndWriteFailureAreWrappedAndCleanedUp() {
        try (Fixture fixture = new Fixture()) {
            fixture.transfer.offsetBytes = new byte[] {1, 2};
            Throwable malformed = failureOf(() -> Blocking.await(fixture.client
                    .transfers()
                    .upload(UploadRequest.fromStream("alice", "file", 1, offset -> completedStream(new byte[] {1}))
                            .token(47)
                            .options(options(20))
                            .build())));
            assertInstanceOf(SoulseekClientException.class, malformed);
            assertFalse(fixture.client.getUploadsInternal().containsKey(47));

            fixture.transfer.offsetBytes = null;
            fixture.transfer.writeFailure = new IOException("write failed");
            Throwable write = failureOf(() -> Blocking.await(fixture.client
                    .transfers()
                    .upload(UploadRequest.fromStream("alice", "other", 1, offset -> completedStream(new byte[] {1}))
                            .token(48)
                            .options(options(20))
                            .build())));
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
            fixture.client
                    .events()
                    .on(Kind.TRANSFER_STATE_CHANGED, (dev.slsk.internal.events.TransferStateChangedEvent eventData) -> {
                        if (eventData.getTransfer().getState().contains(TransferState.COMPLETED)) {
                            terminal.add(eventData.getTransfer());
                        }
                    });

            Throwable failure = failureOf(() -> Blocking.await(fixture.client
                    .transfers()
                    .upload(UploadRequest.fromStream("alice", "file", 1, offset -> completedStream(new byte[] {1}))
                            .token(49)
                            .options(options(20))
                            .build())));

            SoulseekClientException mapped = assertInstanceOf(SoulseekClientException.class, failure);
            ConnectionException connection = assertInstanceOf(ConnectionException.class, mapped.getCause());
            assertSame(socketFailure, connection.getCause());
            assertSame(connection, terminal.get(0).getException());
            assertTrue(terminal.get(0).getState().contains(TransferState.ERRORED));
        }
    }

    private static InputStream completedStream(byte[] bytes) {
        return new ByteArrayInputStream(bytes);
    }

    private static TransferInternal transfer(TransferDirection direction, String username, String filename, int token) {
        return new TransferInternal(direction, username, filename, token);
    }

    private static TransferOptions options(int linger) {
        return options(linger, null, null, null);
    }

    private static TransferOptions options(
            int linger,
            dev.slsk.internal.options.TransferReporter reporter,
            dev.slsk.internal.options.TransferStateChangedCallback stateChanged,
            dev.slsk.internal.options.TransferSlotReleasedCallback slotReleased) {
        return new TransferOptions(stateChanged, null, slotReleased, reporter, linger);
    }

    /**
     * Runs a blocking client call on a virtual thread so the test can interact
     * with it while it is in flight.
     *
     * <p>The API used to hand back a future; now the caller decides whether to
     * be concurrent, and a test that wants to observe a call mid-flight is
     * exactly such a caller. The assertions around it are unchanged.
     */
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
        private final SoulseekEngine client = new SoulseekEngine(
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
            client.setStateForTest(SoulseekClientState.CONNECTED.or(SoulseekClientState.LOGGED_IN));
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
        private final Waiter proxy = (Waiter)
                Proxy.newProxyInstance(Waiter.class.getClassLoader(), new Class<?>[] {Waiter.class}, this::invoke);

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
        private final MessageConnection messageConnection;
        private final Connection transferConnection;
        private CancellationSignal transferToken;
        private final PeerConnectionManager proxy = (PeerConnectionManager) Proxy.newProxyInstance(
                PeerConnectionManager.class.getClassLoader(),
                new Class<?>[] {PeerConnectionManager.class},
                this::invoke);

        private PeerManagerProbe(MessageConnection messageConnection, Connection transferConnection) {
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
                transferToken = (CancellationSignal) arguments[3];
                return CompletableFuture.completedFuture(transferConnection);
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class MessageConnectionProbe {
        private final List<OutgoingMessage> messages = new ArrayList<>();
        private CancellationSignal lastToken;
        private final MessageConnection proxy = (MessageConnection) Proxy.newProxyInstance(
                MessageConnection.class.getClassLoader(), new Class<?>[] {MessageConnection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("writeAsync")
                    && arguments != null
                    && arguments.length == 2
                    && arguments[0] instanceof OutgoingMessage message) {
                messages.add(message);
                lastToken = (CancellationSignal) arguments[1];
                return CompletableFuture.completedFuture(null);
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
        private long offset;
        private byte[] offsetBytes;
        private long size;
        private long writeLength;
        private int writeCalls;
        private int maximumActualPerIteration = Integer.MAX_VALUE;
        private Throwable writeFailure;
        private Exception disconnectOnWrite;
        private final ByteArrayOutputStream written = new ByteArrayOutputStream();
        private ConnectionEventListener<ConnectionDataEvent> dataWrittenListener;
        private ConnectionEventListener<ConnectionDisconnectedEvent> disconnectedListener;
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
                            proxy, new ConnectionDisconnectedEvent("connection lost", disconnectOnWrite));
                    return new CompletableFuture<>();
                }
                if (writeFailure != null) {
                    return CompletableFuture.failedFuture(writeFailure);
                }
                InputStream stream = (InputStream) arguments[1];
                ConnectionGovernor governor = (ConnectionGovernor) arguments[2];
                ConnectionReporter reporter = (ConnectionReporter) arguments[3];
                CancellationSignal token = (CancellationSignal) arguments[4];
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
                        dataWrittenListener.handle(proxy, new ConnectionDataEvent(transferred, length));
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
            if (method.getName().equals("getIpEndpoint")) {
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
