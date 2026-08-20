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

import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.ConnectionReadException;
import dev.slsk.exceptions.DuplicateTokenException;
import dev.slsk.exceptions.DuplicateTransferException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.exceptions.TransferException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.exceptions.TransferStreamException;
import dev.slsk.internal.common.Outcomes;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.concurrent.CancellationController;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.connection.SoulseekClientState;
import dev.slsk.internal.messaging.messages.OutgoingMessage;
import dev.slsk.internal.messaging.messages.TransferRequest;
import dev.slsk.internal.messaging.messages.TransferResponse;
import dev.slsk.internal.messaging.messages.UploadDenied;
import dev.slsk.internal.messaging.messages.UploadFailed;
import dev.slsk.internal.messaging.messages.UserAddressResponse;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.PeerConnectionManager;
import dev.slsk.internal.network.tcp.ConnectionDataEvent;
import dev.slsk.internal.network.tcp.ConnectionDisconnectedEvent;
import dev.slsk.internal.network.tcp.ConnectionGovernor;
import dev.slsk.internal.network.tcp.ConnectionReporter;
import dev.slsk.internal.network.tcp.TransportConnection;
import dev.slsk.internal.network.tcp.TransportState;
import dev.slsk.internal.options.TransferOptions;
import dev.slsk.internal.options.TransferStateChange;
import dev.slsk.internal.transfer.Transfer;
import dev.slsk.internal.transfer.TransferDirection;
import dev.slsk.internal.transfer.TransferInternal;
import dev.slsk.internal.transfer.TransferPhase;
import dev.slsk.internal.transfer.TransferQueueLocation;
import dev.slsk.internal.transfer.TransferTermination;
import dev.slsk.internal.transfer.UploadSpecification;
import dev.slsk.transfer.TransferOutcome;
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
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class EngineUploadTest {
    private static final InetSocketAddress ENDPOINT = new InetSocketAddress(InetAddress.getLoopbackAddress(), 46011);

    @Test
    void validatesArgumentsSizeFactoryFileAndLoginState() {
        try (Fixture fixture = new Fixture()) {
            for (String bad : new String[] {null, "", " ", "\t"}) {
                assertThrows(
                        IllegalArgumentException.class,
                        () -> fixture.client
                                .transfers()
                                .upload(UploadSpecification.fromStream(
                                                bad, "file", 0, offset -> completedStream(new byte[0]))
                                        .build()));
                assertThrows(
                        IllegalArgumentException.class,
                        () -> fixture.client
                                .transfers()
                                .upload(UploadSpecification.fromStream(
                                                "alice", bad, 0, offset -> completedStream(new byte[0]))
                                        .build()));
                assertThrows(
                        IllegalArgumentException.class,
                        () -> fixture.client
                                .transfers()
                                .upload(UploadSpecification.fromFile("alice", "file", bad)
                                        .build()));
            }
            assertThrows(
                    IllegalArgumentException.class,
                    () -> fixture.client
                            .transfers()
                            .upload(UploadSpecification.fromStream(
                                            "alice", "file", -1, offset -> completedStream(new byte[0]))
                                    .build()));
            assertThrows(
                    NullPointerException.class,
                    () -> fixture.client
                            .transfers()
                            .upload(UploadSpecification.fromStream("alice", "file", 0, null)
                                    .build()));
            assertThrows(
                    UncheckedIOException.class,
                    () -> fixture.client
                            .transfers()
                            .upload(UploadSpecification.fromFile("alice", "file", "/missing/upload-file")
                                    .build()));

            fixture.client.setStateForTest(SoulseekClientState.DISCONNECTED);
            assertThrows(
                    IllegalStateException.class,
                    () -> fixture.client
                            .transfers()
                            .upload(UploadSpecification.fromStream(
                                            "alice", "file", 0, offset -> completedStream(new byte[0]))
                                    .build()));
        }
    }

    @Test
    void permitsZeroSizeUploadAndUsesGivenCancellationSignal() {
        try (Fixture fixture = new Fixture()) {
            CancellationController source = new CancellationController();
            fixture.transfer.size = 0;

            Timeline timeline = new Timeline();

            TransferOutcome outcome = fixture.client
                    .transfers()
                    .upload(UploadSpecification.fromStream("alice", "empty", 0, offset -> completedStream(new byte[0]))
                            .token(41)
                            .options(timeline.on(options(20)))
                            .cancellation(source.getSignal())
                            .build());

            assertEquals(0, timeline.last().size());
            assertSame(source.getSignal(), fixture.message.lastToken);
            assertSame(source.getSignal(), fixture.peerManager.transferToken);
            assertInstanceOf(TransferOutcome.Succeeded.class, outcome);
        }
    }

    @Test
    void uploadsLocalFileContentsAndSize() throws IOException {
        Path file = Files.createTempFile("soulseek-upload-", ".bin");
        byte[] bytes = new byte[] {5, 4, 3, 2, 1};
        Files.write(file, bytes);
        try (Fixture fixture = new Fixture()) {
            Timeline timeline = new Timeline();

            fixture.client
                    .transfers()
                    .upload(UploadSpecification.fromFile("alice", "remote", file.toString())
                            .token(7)
                            .options(timeline.on(options(20)))
                            .build());

            assertEquals(bytes.length, timeline.last().size());
            assertArrayEquals(bytes, fixture.transfer.written.toByteArray());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    void aServedUploadReportsItsSpeedToTheServer() throws InterruptedException {
        try (Fixture fixture = new Fixture()) {
            fixture.client.server().username("me");
            fixture.client.setShareCatalog(resolvingCatalog(new byte[] {5, 4, 3, 2, 1}));

            fixture.client.transfers().serve(dev.slsk.user.Username.of("alice"), "shared\\song.mp3");

            // The serve is dispatched onto its own virtual thread, so the
            // report is awaited rather than asserted immediately. The server
            // also sees the peer-address lookup the upload run makes, so the
            // report is picked out by type rather than by position.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (fixture.server.messages.stream()
                            .noneMatch(dev.slsk.internal.messaging.messages.UserStatisticsRequest.class::isInstance)
                    && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }

            List<OutgoingMessage> reported = List.copyOf(fixture.server.messages).stream()
                    .filter(message -> message instanceof dev.slsk.internal.messaging.messages.SendUploadSpeedCommand
                            || message instanceof dev.slsk.internal.messaging.messages.UserStatisticsRequest)
                    .toList();
            assertEquals(2, reported.size(), "a served upload reports its speed and asks for the new average");
            assertTrue(
                    assertInstanceOf(dev.slsk.internal.messaging.messages.SendUploadSpeedCommand.class, reported.get(0))
                                    .getSpeed()
                            > 0,
                    "the reported speed is the served upload's own average");
            assertEquals(
                    "me",
                    assertInstanceOf(dev.slsk.internal.messaging.messages.UserStatisticsRequest.class, reported.get(1))
                            .getUsername(),
                    "the refreshed average is asked for, so search responses advertise the server's number");
        }
    }

    /** A catalog that resolves every path to the given bytes. */
    private static dev.slsk.spi.ShareCatalog resolvingCatalog(byte[] bytes) {
        return new dev.slsk.spi.ShareCatalog() {
            @Override
            public dev.slsk.share.BrowseResponse browse(dev.slsk.user.Username requester) {
                return dev.slsk.share.BrowseResponse.empty();
            }

            @Override
            public List<dev.slsk.share.Directory> directory(dev.slsk.user.Username requester, String path) {
                return List.of();
            }

            @Override
            public List<dev.slsk.search.SearchFile> search(dev.slsk.user.Username requester, String terms, int limit) {
                return List.of();
            }

            @Override
            public java.util.Optional<dev.slsk.spi.ResolvedFile> resolve(
                    dev.slsk.user.Username requester, String path) {
                return java.util.Optional.of(new dev.slsk.spi.ResolvedFile() {
                    @Override
                    public long size() {
                        return bytes.length;
                    }

                    @Override
                    public java.nio.channels.ReadableByteChannel open(long offset) {
                        return java.nio.channels.Channels.newChannel(new ByteArrayInputStream(bytes));
                    }
                });
            }

            @Override
            public dev.slsk.share.ShareIndex index() {
                return dev.slsk.share.ShareIndex.empty();
            }
        };
    }

    @Test
    void rejectsTokensUsedByUploadsOrDownloads() {
        try (Fixture fixture = new Fixture()) {
            fixture.client.getUploadRegistry().put(8, transfer(TransferDirection.UPLOAD, "other", "other", 8));
            assertThrows(
                    DuplicateTokenException.class,
                    () -> fixture.client
                            .transfers()
                            .upload(UploadSpecification.fromStream(
                                            "alice", "file", 1, offset -> completedStream(new byte[] {1}))
                                    .token(8)
                                    .build()));

            fixture.client.getUploadRegistry().clear();
            fixture.client.getDownloadRegistry().put(9, transfer(TransferDirection.DOWNLOAD, "other", "other", 9));
            assertThrows(
                    DuplicateTokenException.class,
                    () -> fixture.client
                            .transfers()
                            .upload(UploadSpecification.fromStream(
                                            "alice", "file", 1, offset -> completedStream(new byte[] {1}))
                                    .token(9)
                                    .build()));
        }
    }

    @Test
    void rejectsMatchingActiveUploadAndUniqueKey() {
        try (Fixture fixture = new Fixture()) {
            fixture.client.getUploadRegistry().put(8, transfer(TransferDirection.UPLOAD, "alice", "file", 8));
            assertThrows(
                    DuplicateTransferException.class,
                    () -> fixture.client
                            .transfers()
                            .upload(UploadSpecification.fromStream(
                                            "alice", "file", 1, offset -> completedStream(new byte[] {1}))
                                    .token(9)
                                    .build()));

            fixture.client.getUploadRegistry().clear();
            fixture.client.getUniqueKeys().add("Upload:alice:file");
            assertThrows(
                    DuplicateTransferException.class,
                    () -> fixture.client
                            .transfers()
                            .upload(UploadSpecification.fromStream(
                                            "alice", "file", 1, offset -> completedStream(new byte[] {1}))
                                    .token(9)
                                    .build()));
        }
    }

    @Test
    void allowsOnlyUsernameOrFilenameToMatch() {
        try (Fixture fixture = new Fixture()) {
            fixture.client.getUploadRegistry().put(8, transfer(TransferDirection.UPLOAD, "alice", "other", 8));
            fixture.client.getUploadRegistry().put(9, transfer(TransferDirection.UPLOAD, "other", "file", 9));

            TransferOutcome outcome = fixture.client
                    .transfers()
                    .upload(UploadSpecification.fromStream(
                                    "alice", "file", 1, offset -> completedStream(new byte[] {1}))
                            .token(10)
                            .options(options(20))
                            .build());

            assertInstanceOf(TransferOutcome.Succeeded.class, outcome);
        }
    }

    /**
     * Replaces {@code enqueueReturnsAfterLocalQueueingBeforeSlotAcquisition},
     * deleted with the {@code enqueue*} overloads under D14.
     *
     * <p>That test had already stopped asserting what it was named for: the
     * pluggable slot awaiter it gated on went when the upload policy became the
     * only slot gate, and the future it completed was read by nothing. What is
     * left of it, and what still matters, is that an upload is in the registry
     * for the whole of its flight and out of it afterwards — which is what an
     * inbound peer message is dispatched against.
     */
    @Test
    void anUploadIsInTheRegistryForTheWholeOfItsFlight() throws InterruptedException {
        try (Fixture fixture = new Fixture()) {
            CountDownLatch requested = new CountDownLatch(1);
            fixture.waiter.transferResponse = new CompletableFuture<>();
            TransferOptions options = options(
                    20,
                    null,
                    change -> {
                        if (change.transfer().phase() == TransferPhase.REQUESTED) {
                            requested.countDown();
                        }
                    },
                    null);

            CompletableFuture<TransferOutcome> upload = inBackground(() -> fixture.client
                    .transfers()
                    .upload(UploadSpecification.fromStream(
                                    "alice", "file", 1, offset -> completedStream(new byte[] {1}))
                            .token(12)
                            .options(options)
                            .build()));

            assertTrue(requested.await(5, TimeUnit.SECONDS));
            assertTrue(fixture.client.getUploadRegistry().containsKey(12));
            assertFalse(upload.isDone());

            fixture.waiter.transferResponse.complete(new TransferResponse(12));
            assertInstanceOf(TransferOutcome.Succeeded.class, upload.join());
            assertFalse(fixture.client.getUploadRegistry().containsKey(12));
        }
    }

    @Test
    void successfulUploadUsesProtocolOrderAndRaisesExpectedStates() {
        try (Fixture fixture = new Fixture()) {
            byte[] bytes = new byte[] {1, 2, 3, 4};
            List<TransferPhase> optionStates = new ArrayList<>();
            Timeline timeline = new Timeline();
            TransferOptions options = timeline.on(options(
                    20, null, change -> optionStates.add(change.transfer().phase()), null));

            fixture.client
                    .transfers()
                    .upload(UploadSpecification.fromStream(
                                    "alice", "remote\\file", bytes.length, offset -> completedStream(bytes))
                            .token(22)
                            .options(options)
                            .build());

            List<TransferPhase> expected = List.of(
                    TransferPhase.QUEUED,
                    TransferPhase.REQUESTED,
                    TransferPhase.INITIALIZING,
                    TransferPhase.IN_PROGRESS,
                    TransferPhase.COMPLETED);
            assertEquals(expected, optionStates, "a caller's own callback still sees every transition");
            assertEquals(expected, timeline.states());
            assertEquals(
                    TransferQueueLocation.LOCAL, timeline.snapshots.getFirst().queueLocation());
            assertEquals(TransferTermination.SUCCEEDED, timeline.last().termination());
            assertEquals(bytes.length, timeline.last().bytesTransferred());
            assertArrayEquals(bytes, fixture.transfer.written.toByteArray());
            assertEquals(bytes.length, fixture.transfer.writeLength);
            TransferRequest request = assertInstanceOf(TransferRequest.class, fixture.message.messages.get(0));
            assertEquals(TransferDirection.UPLOAD, request.getDirection());
            assertEquals(22, request.getToken());
            assertEquals("remote\\file", request.getFilename());
            assertEquals(bytes.length, request.getFileSize());
            assertTrue(timeline.progress.contains(0L));
            assertTrue(timeline.progress.contains((long) bytes.length));
            assertFalse(fixture.client.getUploadRegistry().containsKey(22));
            assertFalse(fixture.client.getUniqueKeys().contains("Upload:alice:remote\\file"));
        }
    }

    @Test
    void seeksToResumeOffsetAndWritesRemainingLength() {
        try (Fixture fixture = new Fixture()) {
            fixture.transfer.offset = 2;
            byte[] bytes = new byte[] {1, 2, 3, 4, 5};

            Timeline timeline = new Timeline();

            fixture.client
                    .transfers()
                    .upload(UploadSpecification.fromStream(
                                    "alice", "file", bytes.length, offset -> completedStream(bytes))
                            .token(31)
                            .options(timeline.on(options(20)))
                            .build());

            assertEquals(2, timeline.last().startOffset());
            assertEquals(3, fixture.transfer.writeLength);
            assertArrayEquals(new byte[] {3, 4, 5}, fixture.transfer.written.toByteArray());
            assertEquals(5, timeline.last().bytesTransferred());
        }
    }

    @Test
    void skipsWriteWhenOffsetEqualsSize() {
        try (Fixture fixture = new Fixture()) {
            fixture.transfer.offset = 3;

            Timeline timeline = new Timeline();

            fixture.client
                    .transfers()
                    .upload(UploadSpecification.fromStream(
                                    "alice", "file", 3, offset -> completedStream(new byte[] {1, 2, 3}))
                            .token(32)
                            .options(timeline.on(options(20)))
                            .build());

            assertEquals(0, fixture.transfer.writeCalls);
            assertEquals(3, timeline.last().bytesTransferred());
        }
    }

    @Test
    void rejectsOffsetBeyondSizeAndNonSeekableResume() {
        try (Fixture fixture = new Fixture()) {
            fixture.transfer.offset = 4;
            TransferOutcome tooLong = fixture.client
                    .transfers()
                    .upload(UploadSpecification.fromStream(
                                    "alice", "file", 3, offset -> completedStream(new byte[] {1, 2, 3}))
                            .token(33)
                            .options(options(20))
                            .build());
            assertInstanceOf(TransferException.class, causeOf(tooLong));
            // Not retryable: the offset cannot shrink and the file cannot grow,
            // so every re-offer fails the same way. Classified retryable, one
            // peer resuming past the end of a file cost eight doomed attempts.
            assertFalse(
                    assertInstanceOf(TransferOutcome.Failed.class, tooLong).retryable(),
                    "an offset past the end of the file cannot be satisfied by trying again");

            fixture.transfer.offset = 1;
            TransferOutcome notSeekable = fixture.client
                    .transfers()
                    .upload(UploadSpecification.fromStream("alice", "other", 2, offset -> (new InputStream() {
                                @Override
                                public int read() {
                                    return -1;
                                }
                            }))
                            .token(34)
                            .options(options(20))
                            .build());
            assertInstanceOf(TransferStreamException.class, causeOf(notSeekable));
        }
    }

    @Test
    void canDisableAutomaticSeeking() {
        try (Fixture fixture = new Fixture()) {
            fixture.transfer.offset = 1;
            TransferOptions options = TransferOptions.builder()
                    .maximumLingerTime(Duration.ofMillis(20))
                    .seekInputStreamAutomatically(false)
                    .build();

            Timeline timeline = new Timeline();

            fixture.client
                    .transfers()
                    .upload(UploadSpecification.fromStream(
                                    "alice", "file", 3, offset -> completedStream(new byte[] {9, 8, 7}))
                            .token(35)
                            .options(timeline.on(options))
                            .build());

            assertArrayEquals(new byte[] {9, 8}, fixture.transfer.written.toByteArray());
            assertEquals(3, timeline.last().bytesTransferred());
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

            fixture.client
                    .transfers()
                    .upload(UploadSpecification.fromStream(
                                    "alice", "file", 5, offset -> completedStream(new byte[] {1, 2, 3, 4, 5}))
                            .token(36)
                            .options(options)
                            .build());

            assertFalse(reports.isEmpty());
            assertTrue(reports.stream().anyMatch(report -> report.get(1) > report.get(2)));
        }
    }

    @Test
    void rejectionSetsFinalStatePreservesExceptionAndNotifiesPeer() {
        try (Fixture fixture = new Fixture()) {
            fixture.waiter.transferResponse = CompletableFuture.completedFuture(new TransferResponse(40, "not shared"));
            Timeline timeline = new Timeline();

            TransferOutcome outcome = fixture.client
                    .transfers()
                    .upload(UploadSpecification.fromStream(
                                    "alice", "file", 1, offset -> completedStream(new byte[] {1}))
                            .token(40)
                            .options(timeline.on(options(20)))
                            .build());

            assertEquals(
                    "Transfer rejected: not shared",
                    assertInstanceOf(TransferOutcome.Rejected.class, outcome).rawMessage());
            assertEquals(TransferTermination.REJECTED, timeline.terminal().termination());
            assertInstanceOf(
                    TransferRejectedException.class, timeline.terminal().exception());
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
            dev.slsk.internal.concurrent.CancellationController cancelled =
                    new dev.slsk.internal.concurrent.CancellationController();
            cancelled.cancel();
            AtomicInteger released = new AtomicInteger();
            List<Transfer> terminal = new ArrayList<>();
            TransferOptions options = options(
                    20,
                    null,
                    change -> {
                        if (change.transfer().phase() == TransferPhase.COMPLETED) {
                            terminal.add(change.transfer());
                        }
                    },
                    transfer -> released.incrementAndGet());

            TransferOutcome outcome = fixture.client
                    .transfers()
                    .upload(UploadSpecification.fromStream(
                                    "alice", "file", 1, offset -> completedStream(new byte[] {1}))
                            .token(41)
                            .options(options)
                            .cancellation(cancelled.getSignal())
                            .build());

            assertInstanceOf(TransferOutcome.Cancelled.class, outcome);
            assertEquals(TransferTermination.CANCELLED, terminal.get(0).termination());
            assertEquals(0, released.get(), "a slot that was not acquired is not released");
            assertInstanceOf(UploadDenied.class, fixture.message.messages.get(fixture.message.messages.size() - 1));
        }
    }

    /**
     * The failure notification is a courtesy, and it is not allowed to cost the
     * slot.
     *
     * <p>It used to resolve the endpoint and establish a message connection
     * from inside the same {@code try} whose {@code finally} released the
     * permits, so an unreachable peer held the per-user semaphore, the upload
     * slot and a global upload permit for the whole indirect budget — on an
     * upload that had just failed because that peer was unreachable. A recorded
     * session paid ten seconds of it on each of forty attempts.
     *
     * <p>Both halves are pinned here: nothing is established to deliver the
     * message, and the slot is already released by the time it is attempted.
     */
    @Test
    void failureNotificationNeitherEstablishesAConnectionNorOutlivesTheSlot() {
        try (Fixture fixture = new Fixture()) {
            fixture.peerManager.messageConnectionCached = false;
            fixture.waiter.transferResponse =
                    CompletableFuture.failedFuture(new ConnectionException("peer is unreachable"));
            // Everything the run does for its own sake happens before the slot
            // goes back. Only what follows the release belongs to the
            // notification, so that is what the counts are measured against.
            AtomicInteger releases = new AtomicInteger();
            AtomicInteger establishesAtRelease = new AtomicInteger(-1);
            TransferOptions options = options(20, null, null, transfer -> {
                releases.incrementAndGet();
                establishesAtRelease.set(fixture.peerManager.establishes);
            });

            TransferOutcome outcome = fixture.client
                    .transfers()
                    .upload(UploadSpecification.fromStream(
                                    "alice", "file", 1, offset -> completedStream(new byte[] {1}))
                            .token(44)
                            .options(options)
                            .build());

            assertInstanceOf(TransferOutcome.Failed.class, outcome);
            assertEquals(1, releases.get(), "the slot was acquired and released");
            assertEquals(
                    establishesAtRelease.get(),
                    fixture.peerManager.establishes,
                    "a failure notification must not dial the peer");
            assertTrue(
                    fixture.message.messages.stream().noneMatch(message -> message instanceof UploadFailed),
                    "nothing is sent when no connection to the peer is already held");
        }
    }

    /**
     * The other side of the rule: a peer we can still reach is still told.
     * Skipping the dial-out must not become skipping the message.
     */
    @Test
    void failureNotificationStillGoesOutOverAConnectionAlreadyHeld() {
        try (Fixture fixture = new Fixture()) {
            fixture.waiter.transferResponse =
                    CompletableFuture.failedFuture(new ConnectionException("transfer connection died"));
            AtomicInteger establishesAtRelease = new AtomicInteger(-1);
            TransferOptions options =
                    options(20, null, null, transfer -> establishesAtRelease.set(fixture.peerManager.establishes));

            fixture.client
                    .transfers()
                    .upload(UploadSpecification.fromStream(
                                    "alice", "file", 1, offset -> completedStream(new byte[] {1}))
                            .token(45)
                            .options(options)
                            .build());

            assertEquals(
                    establishesAtRelease.get(), fixture.peerManager.establishes, "the cached connection is enough");
            assertInstanceOf(UploadFailed.class, fixture.message.messages.get(fixture.message.messages.size() - 1));
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
                        if (change.transfer().phase() == TransferPhase.COMPLETED) {
                            terminal.add(change.transfer());
                        }
                    },
                    null);

            TransferOutcome outcome = fixture.client
                    .transfers()
                    .upload(UploadSpecification.fromStream(
                                    "alice", "file", 1, offset -> completedStream(new byte[] {1}))
                            .token(42)
                            .options(options)
                            .build());

            // The deadline itself, rather than the NoResponseException the old
            // blocking wrapper renamed it to on the way out.
            assertSame(timeout, causeOf(outcome));
            assertEquals(TransferTermination.TIMED_OUT, terminal.get(0).termination());
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
            TransferOutcome outcome = fixture.client
                    .transfers()
                    .upload(UploadSpecification.fromStream("alice", "file", 1, offset -> {
                                throw new java.io.UncheckedIOException(new java.io.IOException("source is gone"));
                            })
                            .token(43)
                            .options(options(20))
                            .build());
            assertInstanceOf(java.io.UncheckedIOException.class, causeOf(outcome));

            AtomicInteger released = new AtomicInteger();
            fixture.client
                    .transfers()
                    .upload(UploadSpecification.fromStream(
                                    "alice", "other", 1, offset -> completedStream(new byte[] {1}))
                            .token(44)
                            .options(options(20, null, null, transfer -> {
                                released.incrementAndGet();
                                throw new RuntimeException("ignored");
                            }))
                            .build());
            assertEquals(1, released.get());
        }
    }

    @Test
    void streamCloseOptionIsHonored() {
        try (Fixture fixture = new Fixture()) {
            CloseTrackingInputStream closeTracking = new CloseTrackingInputStream(new byte[] {1});
            fixture.client
                    .transfers()
                    .upload(UploadSpecification.fromStream("alice", "file", 1, offset -> closeTracking)
                            .token(45)
                            .options(options(20).withCloseOptions(true))
                            .build());
            assertTrue(closeTracking.closed.get());

            CloseTrackingInputStream retained = new CloseTrackingInputStream(new byte[] {2});
            fixture.client
                    .transfers()
                    .upload(UploadSpecification.fromStream("alice", "other", 1, offset -> retained)
                            .token(46)
                            .options(options(20).withCloseOptions(false))
                            .build());
            assertFalse(retained.closed.get());
        }
    }

    @Test
    void malformedOffsetAndWriteFailureAreWrappedAndCleanedUp() {
        try (Fixture fixture = new Fixture()) {
            fixture.transfer.offsetBytes = new byte[] {1, 2};
            TransferOutcome malformed = fixture.client
                    .transfers()
                    .upload(UploadSpecification.fromStream(
                                    "alice", "file", 1, offset -> completedStream(new byte[] {1}))
                            .token(47)
                            .options(options(20))
                            .build());
            assertInstanceOf(MessageReadException.class, causeOf(malformed));
            assertFalse(fixture.client.getUploadRegistry().containsKey(47));

            fixture.transfer.offsetBytes = null;
            // What the transport raises for a failed streaming write: the
            // classified domain exception, not the raw IOException under it.
            fixture.transfer.writeFailure =
                    new dev.slsk.exceptions.ConnectionWriteException("write failed", new IOException("broken pipe"));
            TransferOutcome write = fixture.client
                    .transfers()
                    .upload(UploadSpecification.fromStream(
                                    "alice", "other", 1, offset -> completedStream(new byte[] {1}))
                            .token(48)
                            .options(options(20))
                            .build());
            assertSame(fixture.transfer.writeFailure, causeOf(write));
            assertFalse(fixture.client.getUniqueKeys().contains("Upload:alice:other"));
        }
    }

    @Test
    void unexpectedTransferDisconnectIsWrappedAndRecorded() {
        try (Fixture fixture = new Fixture()) {
            IOException socketFailure = new IOException("socket failed");
            fixture.transfer.disconnectOnWrite = socketFailure;
            Timeline timeline = new Timeline();

            TransferOutcome outcome = fixture.client
                    .transfers()
                    .upload(UploadSpecification.fromStream(
                                    "alice", "file", 1, offset -> completedStream(new byte[] {1}))
                            .token(49)
                            .options(timeline.on(options(20)))
                            .build());

            ConnectionException connection = assertInstanceOf(ConnectionException.class, causeOf(outcome));
            assertSame(socketFailure, connection.getCause());
            assertSame(connection, timeline.terminal().exception());
            assertEquals(TransferTermination.ERRORED, timeline.terminal().termination());
        }
    }

    /**
     * Every snapshot a transfer published; see the twin in
     * {@code EngineDownloadTest}.
     */
    private static final class Timeline {
        private final List<Transfer> snapshots = new ArrayList<>();
        private final List<Long> progress = new ArrayList<>();

        private TransferOptions on(TransferOptions base) {
            return TransferOptions.builder(base)
                    .stateChanged(change -> {
                        snapshots.add(change.transfer());
                        if (base.stateChanged() != null) {
                            base.stateChanged().accept(change);
                        }
                    })
                    .progressUpdated(update -> {
                        progress.add(update.transfer().bytesTransferred());
                        if (base.progressUpdated() != null) {
                            base.progressUpdated().accept(update);
                        }
                    })
                    .build();
        }

        private List<TransferPhase> states() {
            return snapshots.stream().map(Transfer::phase).toList();
        }

        private Transfer last() {
            return snapshots.get(snapshots.size() - 1);
        }

        /** The snapshot taken as the transfer reached a terminal state. */
        private Transfer terminal() {
            return snapshots.stream()
                    .filter(snapshot -> snapshot.phase() == TransferPhase.COMPLETED)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("the transfer never reached a terminal state"));
        }
    }

    /** Returns what a failed outcome says went wrong. */
    private static Throwable causeOf(TransferOutcome outcome) {
        return assertInstanceOf(TransferOutcome.Failed.class, outcome).cause();
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
            Consumer<TransferStateChange> stateChanged,
            Consumer<Transfer> slotReleased) {
        return TransferOptions.builder()
                .stateChanged(stateChanged)
                .slotReleased(slotReleased)
                .reporter(reporter)
                .maximumLingerTime(Duration.ofMillis(linger))
                .build();
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
            client.setStateForTest(SoulseekClientState.LOGGED_IN);
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

        private Object invoke(Object ignored, Method method, Object[] arguments) throws Exception {
            if (method.getName().startsWith("register")) {
                if (arguments != null && arguments.length > 0 && arguments[0] instanceof WaitKey key) {
                    keys.add(key);
                }
                if (arguments != null) {
                    for (Object argument : arguments) {
                        if (argument == TransferResponse.class) {
                            return (Wait<Object>) () -> Outcomes.raise(transferResponse);
                        }
                        if (argument == UserAddressResponse.class) {
                            return (Wait<Object>) () -> new UserAddressResponse("alice", ENDPOINT);
                        }
                    }
                }
                return (Wait<Object>) () -> null;
            }
            if (method.getName().equals("getDefaultTimeout")) {
                return Duration.ofSeconds(5);
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class PeerManagerProbe {
        private final MessageConnection messageConnection;
        private final TransportConnection transferConnection;
        private CancellationSignal transferToken;
        /**
         * Whether this peer already has a message connection in the cache.
         *
         * <p>True is the ordinary case — the run reached the peer to offer the
         * file at all — and it is what keeps the notification assertions below
         * meaningful. A test sets it false to model the peer that went
         * unreachable, which is the case that must not provoke a dial-out.
         */
        private boolean messageConnectionCached = true;
        /** Counts establishes, which a failure notification must never cause. */
        private int establishes;

        private final PeerConnectionManager proxy = (PeerConnectionManager) Proxy.newProxyInstance(
                PeerConnectionManager.class.getClassLoader(),
                new Class<?>[] {PeerConnectionManager.class},
                this::invoke);

        private PeerManagerProbe(MessageConnection messageConnection, TransportConnection transferConnection) {
            this.messageConnection = messageConnection;
            this.transferConnection = transferConnection;
        }

        private Object invoke(Object ignored, Method method, Object[] arguments) throws Exception {
            if (method.getName().equals("getCachedMessageConnection")) {
                return messageConnectionCached ? messageConnection : null;
            }
            if (method.getName().equals("getOrAddMessageConnection")
                    && arguments != null
                    && arguments.length == 3
                    && arguments[0] instanceof String) {
                establishes++;
                return messageConnection;
            }
            if (method.getName().equals("getTransferConnection")
                    && arguments != null
                    && arguments.length == 4
                    && arguments[0] instanceof String) {
                transferToken = (CancellationSignal) arguments[3];
                return transferConnection;
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class MessageConnectionProbe {
        private final List<OutgoingMessage> messages = new ArrayList<>();
        private CancellationSignal lastToken;
        private final MessageConnection proxy = (MessageConnection) Proxy.newProxyInstance(
                MessageConnection.class.getClassLoader(), new Class<?>[] {MessageConnection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) throws Exception {
            if (method.getName().equals("write")
                    && arguments != null
                    && arguments.length == 2
                    && arguments[0] instanceof OutgoingMessage message) {
                messages.add(message);
                lastToken = (CancellationSignal) arguments[1];
                return null;
            }
            if (method.getName().equals("getState")) {
                return TransportState.CONNECTED;
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
        private java.util.function.Consumer<ConnectionDataEvent> dataWrittenListener;
        private java.util.function.Consumer<ConnectionDisconnectedEvent> disconnectedListener;
        private final TransportConnection proxy = (TransportConnection) Proxy.newProxyInstance(
                TransportConnection.class.getClassLoader(), new Class<?>[] {TransportConnection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) throws Exception {
            if (method.getName().equals("read")
                    && arguments != null
                    && arguments.length == 2
                    && arguments[0] instanceof Long length) {
                if (length == 8) {
                    return offsetBytes == null
                            ? ByteBuffer.allocate(8)
                                    .order(ByteOrder.LITTLE_ENDIAN)
                                    .putLong(offset)
                                    .array()
                            : offsetBytes;
                }
                throw new ConnectionReadException("peer disconnected");
            }
            if (method.getName().equals("write")
                    && arguments != null
                    && arguments.length == 5
                    && arguments[0] instanceof Long length) {
                writeCalls++;
                writeLength = length;
                if (disconnectOnWrite != null) {
                    disconnectedListener.accept(
                            new ConnectionDisconnectedEvent(proxy, "connection lost", disconnectOnWrite));
                    // Parks like a write to a peer that has stopped reading.
                    new CompletableFuture<Void>().join();
                }
                if (writeFailure != null) {
                    return Outcomes.raise(CompletableFuture.<Void>failedFuture(writeFailure));
                }
                ReadableByteChannel source = (ReadableByteChannel) arguments[1];
                ConnectionGovernor governor = (ConnectionGovernor) arguments[2];
                ConnectionReporter reporter = (ConnectionReporter) arguments[3];
                CancellationSignal token = (CancellationSignal) arguments[4];
                long transferred = 0;
                while (transferred < length) {
                    int attempted = (int) Math.min(Integer.MAX_VALUE, length - transferred);
                    int granted = governor == null ? attempted : governor.grant(attempted, token);
                    int target = Math.min(granted, maximumActualPerIteration);
                    ByteBuffer buffer = ByteBuffer.allocate(target);
                    int count = source.read(buffer);
                    if (count < 0) {
                        count = 0;
                    }
                    written.write(buffer.array(), 0, count);
                    transferred += count;
                    if (reporter != null) {
                        reporter.report(attempted, granted, count);
                    }
                    if (count == 0) {
                        throw new IOException("channel ended early");
                    }
                    if (dataWrittenListener != null) {
                        dataWrittenListener.accept(new ConnectionDataEvent(proxy, transferred, length));
                    }
                }
                size = transferred;
                return CompletableFuture.completedFuture(null);
            }
            if (method.getName().equals("subscribe")) {
                if (arguments[0] == TransportConnection.Kind.DATA_WRITTEN) {
                    java.util.function.Consumer<ConnectionDataEvent> registered = cast(arguments[1]);
                    dataWrittenListener = registered;
                    return (dev.slsk.Subscription) () -> {
                        if (dataWrittenListener == registered) {
                            dataWrittenListener = null;
                        }
                    };
                }
                if (arguments[0] == TransportConnection.Kind.DISCONNECTED) {
                    java.util.function.Consumer<ConnectionDisconnectedEvent> registered = cast(arguments[1]);
                    disconnectedListener = registered;
                    return (dev.slsk.Subscription) () -> {
                        if (disconnectedListener == registered) {
                            disconnectedListener = null;
                        }
                    };
                }
                return (dev.slsk.Subscription) () -> {};
            }
            if (method.getName().equals("getState")) {
                return TransportState.CONNECTED;
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
