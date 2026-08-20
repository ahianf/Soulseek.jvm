// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.NoResponseException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.exceptions.TransferNotFoundException;
import dev.slsk.exceptions.UserEndpointException;
import dev.slsk.exceptions.UserOfflineException;
import dev.slsk.internal.EngineEvents.Kind;
import dev.slsk.internal.common.Outcomes;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.concurrent.CancellationController;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.connection.SoulseekClientState;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.events.BrowseProgressUpdatedEvent;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.handlers.BrowseResponseConnection;
import dev.slsk.internal.messaging.messages.BrowseRequestMessage;
import dev.slsk.internal.messaging.messages.FolderContentsRequest;
import dev.slsk.internal.messaging.messages.OutgoingMessage;
import dev.slsk.internal.messaging.messages.PlaceInQueueRequest;
import dev.slsk.internal.messaging.messages.PlaceInQueueResponse;
import dev.slsk.internal.messaging.messages.UserAddressRequest;
import dev.slsk.internal.messaging.messages.UserAddressResponse;
import dev.slsk.internal.messaging.messages.UserInfoRequest;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.MessageDataEvent;
import dev.slsk.internal.network.MessageReceivedEvent;
import dev.slsk.internal.network.PeerConnectionManager;
import dev.slsk.internal.network.tcp.Connection;
import dev.slsk.internal.network.tcp.ConnectionDisconnectedEvent;
import dev.slsk.internal.options.BrowseOptions;
import dev.slsk.internal.share.BrowseResponseMessage;
import dev.slsk.internal.share.SharedDirectory;
import dev.slsk.internal.transfer.TransferDirection;
import dev.slsk.internal.transfer.TransferInternal;
import dev.slsk.internal.user.UserInfo;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class EnginePeerRequestTest {
    private static final InetSocketAddress ENDPOINT = new InetSocketAddress(InetAddress.getLoopbackAddress(), 46001);

    @Test
    void connectToUserResolvesEndpointAndGetsConnection() throws Exception {
        Fixture fixture = new Fixture();
        fixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));
        CancellationController source = new CancellationController();
        CancellationSignal token = source.getSignal();

        fixture.client.users().connectToUser("alice", token);

        assertEquals("alice", fixture.peerManager.username);
        assertEquals(ENDPOINT, fixture.peerManager.endpoint);
        assertSame(token, fixture.peerManager.token);
        assertEquals(0, fixture.peerManager.invalidations);
        assertEquals(
                "alice",
                assertInstanceOf(UserAddressRequest.class, fixture.server.message)
                        .getUsername());
        assertSame(token, fixture.server.token);
        fixture.close();
    }

    @Test
    void connectToUserInvalidatesOnRequestAndLogsOnlySuccess() throws Exception {
        Fixture fixture = new Fixture();
        fixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));
        fixture.peerManager.invalidationResult = false;

        fixture.client.users().connectToUser("alice", true);

        // Asserted by absence of the one line rather than by an empty list: the
        // endpoint lookup on the way through logs a cache miss of its own, and
        // what is under test is that a no-op invalidation stays quiet.
        String invalidated = "Invalidated message connection cache for alice";
        assertEquals(1, fixture.peerManager.invalidations);
        assertFalse(fixture.diagnostic.debugMessages.contains(invalidated));

        fixture.peerManager.invalidationResult = true;
        fixture.client.users().connectToUser("alice", true);
        assertEquals(2, fixture.peerManager.invalidations);
        assertTrue(fixture.diagnostic.debugMessages.contains(invalidated));
        fixture.close();
    }

    @Test
    void getUserInfoRegistersWaitThenUsesPeerConnection() throws Exception {
        Fixture fixture = new Fixture();
        UserInfo expected = new UserInfo("description", 3, 4, true, new byte[] {1, 2});
        fixture.waiter.results.put(UserInfo.class, CompletableFuture.completedFuture(expected));
        fixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));
        CancellationController source = new CancellationController();
        CancellationSignal token = source.getSignal();

        UserInfo actual = fixture.client.users().getUserInfo("alice", token);

        assertSame(expected, actual);
        assertEquals(
                List.of(
                        new WaitKey(MessageCode.Peer.INFO_RESPONSE, "alice"),
                        new WaitKey(MessageCode.Server.GET_PEER_ADDRESS, "alice")),
                fixture.waiter.keys);
        assertEquals("alice", fixture.peerManager.username);
        assertEquals(ENDPOINT, fixture.peerManager.endpoint);
        assertSame(token, fixture.peerManager.token);
        assertInstanceOf(UserInfoRequest.class, fixture.peer.message);
        assertSame(token, fixture.peer.token);
        fixture.waiter.tokens.forEach(recorded -> assertSame(token, recorded));
        fixture.close();
    }

    @Test
    void validatesArgumentsAndLoginState() {
        Fixture fixture = new Fixture();
        for (String bad : new String[] {null, "", " ", "\t"}) {
            assertThrows(
                    IllegalArgumentException.class, () -> fixture.client.users().connectToUser(bad));
            assertThrows(
                    IllegalArgumentException.class, () -> fixture.client.users().getUserInfo(bad));
        }
        fixture.client.setStateForTest(SoulseekClientState.DISCONNECTED);
        assertThrows(IllegalStateException.class, () -> fixture.client.users().connectToUser("alice"));
        assertThrows(IllegalStateException.class, () -> fixture.client.users().getUserInfo("alice"));
        fixture.close();
    }

    @Test
    void connectPreservesOfflineTimeoutAndCancellation() {
        Fixture fixture = new Fixture();
        UserOfflineException offline = new UserOfflineException("offline");
        fixture.waiter.results.put(UserAddressResponse.class, CompletableFuture.failedFuture(offline));
        assertSame(offline, failureOf(() -> fixture.client.users().connectToUser("alice")));

        TimeoutException timeout = new TimeoutException("timed out");
        fixture.server.result = CompletableFuture.failedFuture(timeout);
        fixture.waiter.results.put(UserAddressResponse.class, new CompletableFuture<>());
        assertSame(
                timeout,
                assertInstanceOf(
                                NoResponseException.class,
                                failureOf(() -> fixture.client.users().connectToUser("bob")))
                        .getCause());

        CancellationException cancellation = new CancellationException("cancelled");
        fixture.server.result = CompletableFuture.failedFuture(cancellation);
        assertSame(cancellation, failureOf(() -> fixture.client.users().connectToUser("carol")));
        fixture.close();
    }

    @Test
    void connectWrapsEndpointAndManagerFailuresAtSourceLayers() {
        Fixture fixture = new Fixture();
        RuntimeException endpointFailure = new RuntimeException("endpoint failed");
        fixture.server.synchronousFailure = endpointFailure;

        SoulseekClientException outer = assertInstanceOf(
                SoulseekClientException.class,
                failureOf(() -> fixture.client.users().connectToUser("alice")));
        UserEndpointException endpoint = assertInstanceOf(UserEndpointException.class, outer.getCause());
        assertSame(endpointFailure, endpoint.getCause());

        fixture.server.synchronousFailure = null;
        fixture.server.result = CompletableFuture.completedFuture(null);
        fixture.waiter.results.put(
                UserAddressResponse.class, CompletableFuture.completedFuture(new UserAddressResponse("bob", ENDPOINT)));
        RuntimeException managerFailure = new RuntimeException("manager failed");
        fixture.peerManager.synchronousFailure = managerFailure;

        SoulseekClientException managerMapped = assertInstanceOf(
                SoulseekClientException.class,
                failureOf(() -> fixture.client.users().connectToUser("bob")));
        assertSame(managerFailure, managerMapped.getCause());
        fixture.close();
    }

    @Test
    void getUserInfoMapsPeerFailuresAndPreservesSpecialCases() {
        Fixture fixture = new Fixture();
        UserInfo info = new UserInfo("description", 1, 2, false);
        fixture.waiter.results.put(UserInfo.class, CompletableFuture.completedFuture(info));
        fixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));

        TimeoutException timeout = new TimeoutException("timed out");
        fixture.peer.result = CompletableFuture.failedFuture(timeout);
        assertSame(
                timeout,
                assertInstanceOf(
                                NoResponseException.class,
                                failureOf(() -> fixture.client.users().getUserInfo("alice")))
                        .getCause());

        CancellationException cancellation = new CancellationException("cancelled");
        fixture.peer.result = CompletableFuture.failedFuture(cancellation);
        assertSame(cancellation, failureOf(() -> fixture.client.users().getUserInfo("bob")));

        RuntimeException expected = new RuntimeException("peer write failed");
        fixture.peer.result = CompletableFuture.failedFuture(expected);
        SoulseekClientException mapped = assertInstanceOf(
                SoulseekClientException.class,
                failureOf(() -> fixture.client.users().getUserInfo("carol")));
        assertSame(expected, mapped.getCause());

        UserOfflineException offline = new UserOfflineException("offline");
        fixture.peer.result = CompletableFuture.completedFuture(null);
        fixture.waiter.results.put(UserAddressResponse.class, CompletableFuture.failedFuture(offline));
        assertSame(offline, failureOf(() -> fixture.client.users().getUserInfo("dave")));
        fixture.close();
    }

    @Test
    void directoryContentsUsesTokenCorrelationAndReturnsSnapshot() throws Exception {
        Fixture fixture = new Fixture();
        fixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));
        List<SharedDirectory> source = new ArrayList<>(List.of(new SharedDirectory("shared")));
        fixture.waiter.results.put(List.class, CompletableFuture.completedFuture(source));
        CancellationController cancellationController = new CancellationController();
        CancellationSignal cancellationSignal = cancellationController.getSignal();

        List<SharedDirectory> result =
                fixture.client.users().getDirectoryContents("alice", "shared", 123, cancellationSignal);

        assertEquals(1, result.size());
        source.clear();
        assertEquals(1, result.size());
        assertThrows(UnsupportedOperationException.class, () -> result.add(new SharedDirectory("other")));
        assertEquals(new WaitKey(MessageCode.Peer.FOLDER_CONTENTS_RESPONSE, "alice", 123), fixture.waiter.keys.get(0));
        FolderContentsRequest request = assertInstanceOf(FolderContentsRequest.class, fixture.peer.message);
        assertEquals(123, request.getToken());
        assertEquals("shared", request.getDirectoryName());
        assertSame(cancellationSignal, fixture.peer.token);
        fixture.waiter.tokens.forEach(recorded -> assertSame(cancellationSignal, recorded));
        fixture.close();
    }

    @Test
    void queuePlaceRequiresActiveDownloadAndReturnsResponse() throws Exception {
        Fixture fixture = new Fixture();
        fixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));
        fixture.waiter.results.put(
                PlaceInQueueResponse.class, CompletableFuture.completedFuture(new PlaceInQueueResponse("file", 17)));
        fixture.client.setDownloadsForTest(
                new HashMap<>(Map.of(1, new TransferInternal(TransferDirection.DOWNLOAD, "alice", "file", 1))));
        CancellationController source = new CancellationController();
        CancellationSignal token = source.getSignal();

        int result = fixture.client.transfers().getDownloadPlaceInQueue("alice", "file", token);

        assertEquals(17, result);
        assertEquals(
                new WaitKey(MessageCode.Peer.PLACE_IN_QUEUE_RESPONSE, "alice", "file"), fixture.waiter.keys.get(0));
        assertEquals(
                "file",
                assertInstanceOf(PlaceInQueueRequest.class, fixture.peer.message)
                        .getFilename());
        assertSame(token, fixture.peer.token);
        fixture.close();
    }

    @Test
    void smallPeerQueriesValidateAndMapFailures() {
        Fixture fixture = new Fixture();
        for (String bad : new String[] {null, "", " ", "\t"}) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> fixture.client.users().getDirectoryContents(bad, "directory"));
            assertThrows(
                    IllegalArgumentException.class, () -> fixture.client.users().getDirectoryContents("alice", bad));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> fixture.client.transfers().getDownloadPlaceInQueue(bad, "file"));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> fixture.client.transfers().getDownloadPlaceInQueue("alice", bad));
        }
        assertThrows(
                TransferNotFoundException.class,
                () -> fixture.client.transfers().getDownloadPlaceInQueue("alice", "file"));
        fixture.client.setDownloadsForTest(new HashMap<>(Map.of(
                1,
                new TransferInternal(TransferDirection.DOWNLOAD, "other", "file", 1),
                2,
                new TransferInternal(TransferDirection.DOWNLOAD, "alice", "other", 2))));
        assertThrows(
                TransferNotFoundException.class,
                () -> fixture.client.transfers().getDownloadPlaceInQueue("alice", "file"));

        fixture.client.setStateForTest(SoulseekClientState.DISCONNECTED);
        assertThrows(
                IllegalStateException.class, () -> fixture.client.users().getDirectoryContents("alice", "directory"));
        assertThrows(
                IllegalStateException.class, () -> fixture.client.transfers().getDownloadPlaceInQueue("alice", "file"));
        fixture.close();
    }

    @Test
    void smallPeerQueriesPreserveSpecialFailuresAndWrapOthers() {
        Fixture directoryFixture = new Fixture();
        directoryFixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));
        directoryFixture.waiter.results.put(
                List.class, CompletableFuture.completedFuture(List.of(new SharedDirectory("shared"))));
        TimeoutException timeout = new TimeoutException("timed out");
        directoryFixture.peer.result = CompletableFuture.failedFuture(timeout);
        assertSame(
                timeout,
                assertInstanceOf(
                                NoResponseException.class,
                                failureOf(
                                        () -> directoryFixture.client.users().getDirectoryContents("alice", "shared")))
                        .getCause());

        CancellationException cancellation = new CancellationException("cancelled");
        directoryFixture.peer.result = CompletableFuture.failedFuture(cancellation);
        assertSame(
                cancellation, failureOf(() -> directoryFixture.client.users().getDirectoryContents("bob", "shared")));

        RuntimeException expected = new RuntimeException("peer failed");
        directoryFixture.peer.result = CompletableFuture.failedFuture(expected);
        SoulseekClientException mapped = assertInstanceOf(
                SoulseekClientException.class,
                failureOf(() -> directoryFixture.client.users().getDirectoryContents("carol", "shared")));
        assertSame(expected, mapped.getCause());
        directoryFixture.close();

        Fixture queueFixture = new Fixture();
        queueFixture.client.setDownloadsForTest(
                new HashMap<>(Map.of(1, new TransferInternal(TransferDirection.DOWNLOAD, "alice", "file", 1))));
        UserOfflineException offline = new UserOfflineException("offline");
        queueFixture.waiter.results.put(PlaceInQueueResponse.class, new CompletableFuture<>());
        queueFixture.waiter.results.put(UserAddressResponse.class, CompletableFuture.failedFuture(offline));
        assertSame(offline, failureOf(() -> queueFixture.client.transfers().getDownloadPlaceInQueue("alice", "file")));

        RuntimeException waitFailure = new RuntimeException("wait failed");
        queueFixture.waiter.results.put(PlaceInQueueResponse.class, CompletableFuture.failedFuture(waitFailure));
        queueFixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));
        SoulseekClientException waitMapped = assertInstanceOf(
                SoulseekClientException.class,
                failureOf(() -> queueFixture.client.transfers().getDownloadPlaceInQueue("alice", "file")));
        assertSame(waitFailure, waitMapped.getCause());
        queueFixture.close();
    }

    @Test
    void browseReturnsResponseAndReportsInitialAndFinalProgress() throws Exception {
        Fixture fixture = new Fixture();
        BrowseResponseMessage response = new BrowseResponseMessage(List.of(new SharedDirectory("shared")));
        fixture.waiter.results.put(BrowseResponseMessage.class, CompletableFuture.completedFuture(response));
        fixture.waiter.results.put(
                BrowseResponseConnection.class,
                CompletableFuture.completedFuture(new BrowseResponseConnection(
                        new MessageReceivedEvent(104, new byte[] {1, 2, 3, 4}), fixture.peer.proxy)));
        fixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));
        List<BrowseProgressUpdatedEvent> events = new ArrayList<>();
        List<dev.slsk.internal.options.BrowseProgress> callbacks = new ArrayList<>();
        fixture.client
                .events()
                .on(
                        Kind.BROWSE_PROGRESS_UPDATED,
                        (dev.slsk.internal.events.BrowseProgressUpdatedEvent eventData) -> events.add(eventData));
        CancellationController source = new CancellationController();
        CancellationSignal token = source.getSignal();

        BrowseResponseMessage actual = fixture.client
                .users()
                .browse(
                        "alice",
                        BrowseOptions.builder()
                                .responseTimeout(Duration.ofMillis(1234))
                                .progressUpdated(callbacks::add)
                                .build(),
                        token);

        assertSame(response, actual);
        assertInstanceOf(BrowseRequestMessage.class, fixture.peer.message);
        assertEquals(
                List.of(
                        new WaitKey(MessageCode.Peer.BROWSE_RESPONSE, "alice"),
                        new WaitKey(dev.slsk.internal.common.Constants.WaitKey.BROWSE_RESPONSE_CONNECTION, "alice"),
                        new WaitKey(MessageCode.Server.GET_PEER_ADDRESS, "alice")),
                fixture.waiter.keys);
        assertEquals(Duration.ofMillis(1234), fixture.waiter.lastTimeout);
        assertEquals(2, events.size());
        assertEquals(0.0, events.get(0).percentComplete());
        assertEquals(100.0, events.get(1).percentComplete());
        assertEquals(2, callbacks.size());
        assertEquals(0.0, callbacks.get(0).percentComplete());
        assertEquals(100.0, callbacks.get(1).percentComplete());
        fixture.waiter.tokens.forEach(recorded -> assertSame(token, recorded));
        assertSame(token, fixture.peer.token);
        fixture.close();
    }

    @Test
    void browseValidatesAndPreservesSourceFailureClasses() {
        Fixture fixture = new Fixture();
        for (String bad : new String[] {null, "", " ", "\t"}) {
            assertThrows(
                    IllegalArgumentException.class, () -> fixture.client.users().browse(bad));
        }
        fixture.client.setStateForTest(SoulseekClientState.DISCONNECTED);
        assertThrows(IllegalStateException.class, () -> fixture.client.users().browse("alice"));
        fixture.client.setStateForTest(SoulseekClientState.LOGGED_IN);

        fixture.waiter.results.put(BrowseResponseMessage.class, new CompletableFuture<>());
        TimeoutException timeout = new TimeoutException("header timed out");
        fixture.waiter.results.put(BrowseResponseConnection.class, CompletableFuture.failedFuture(timeout));
        fixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));
        assertSame(
                timeout,
                assertInstanceOf(
                                NoResponseException.class,
                                failureOf(() -> fixture.client.users().browse("alice")))
                        .getCause());

        UserOfflineException offline = new UserOfflineException("offline");
        fixture.waiter.results.put(BrowseResponseConnection.class, new CompletableFuture<>());
        fixture.waiter.results.put(UserAddressResponse.class, CompletableFuture.failedFuture(offline));
        assertSame(offline, failureOf(() -> fixture.client.users().browse("bob")));

        CancellationException cancellation = new CancellationException("cancelled");
        fixture.waiter.results.put(BrowseResponseMessage.class, CompletableFuture.failedFuture(cancellation));
        fixture.waiter.results.put(
                BrowseResponseConnection.class,
                CompletableFuture.completedFuture(
                        new BrowseResponseConnection(new MessageReceivedEvent(5, new byte[] {1}), fixture.peer.proxy)));
        fixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("carol", ENDPOINT)));
        assertSame(cancellation, failureOf(() -> fixture.client.users().browse("carol")));
        fixture.close();
    }

    @Test
    void browseFailsIndefiniteWaitOnSetupFailureAndDisconnect() throws Exception {
        Fixture setupFixture = new Fixture();
        setupFixture.waiter.results.put(BrowseResponseMessage.class, new CompletableFuture<>());
        setupFixture.waiter.results.put(BrowseResponseConnection.class, new CompletableFuture<>());
        setupFixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));
        RuntimeException expected = new RuntimeException("write failed");
        setupFixture.peer.synchronousFailure = expected;

        SoulseekClientException mapped = assertInstanceOf(
                SoulseekClientException.class,
                failureOf(() -> setupFixture.client.users().browse("alice")));
        assertSame(expected, mapped.getCause());
        assertEquals(new WaitKey(MessageCode.Peer.BROWSE_RESPONSE, "alice"), setupFixture.waiter.failedKey);
        setupFixture.close();

        Fixture disconnectFixture = new Fixture();
        disconnectFixture.waiter.results.put(BrowseResponseMessage.class, new CompletableFuture<>());
        disconnectFixture.waiter.results.put(
                BrowseResponseConnection.class,
                CompletableFuture.completedFuture(new BrowseResponseConnection(
                        new MessageReceivedEvent(5, new byte[] {1}), disconnectFixture.peer.proxy)));
        disconnectFixture.waiter.results.put(
                UserAddressResponse.class, CompletableFuture.completedFuture(new UserAddressResponse("bob", ENDPOINT)));

        CompletableFuture<BrowseResponseMessage> operation =
                inBackground(() -> disconnectFixture.client.users().browse("bob"));
        // browse runs on another thread now; it has to have wired its
        // disconnect listener before the peer can drop underneath it.
        waitForDisconnectListener(disconnectFixture.peer);
        disconnectFixture.peer.raiseDisconnected("gone");

        SoulseekClientException disconnected =
                assertInstanceOf(SoulseekClientException.class, failureOf(operation::join));
        ConnectionException cause = assertInstanceOf(ConnectionException.class, disconnected.getCause());
        assertEquals("Peer connection disconnected unexpectedly: gone", cause.getMessage());
        disconnectFixture.close();
    }

    /** Waits for the background caller to attach its disconnect listener. */
    private static void waitForDisconnectListener(ConnectionProbe peer) {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        while (!peer.hasDisconnectedListener() && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
    }

    /**
     * Runs a blocking client call on a virtual thread so the test can interact
     * with it while it is in flight.
     *
     * <p>The API used to hand back a future; now the caller decides whether to
     * be concurrent, and a test that wants to observe a call mid-flight is
     * exactly such a caller. The assertions around it are unchanged.
     */
    private static <T> CompletableFuture<T> inBackground(BlockingCall<T> call) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return call.get();
                    } catch (Exception checked) {
                        throw new CompletionException(checked);
                    }
                },
                Executors.newVirtualThreadPerTaskExecutor());
    }

    /** A blocking facade call under test; checked outcomes surface via the future. */
    @FunctionalInterface
    private interface BlockingCall<T> {
        T get() throws Exception;
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

    private static final class Fixture implements AutoCloseable {
        private final ConnectionProbe server = new ConnectionProbe();
        private final ConnectionProbe peer = new ConnectionProbe();
        private final WaiterProbe waiter = new WaiterProbe();
        private final PeerManagerProbe peerManager = new PeerManagerProbe(peer.proxy);
        private final DiagnosticProbe diagnostic = new DiagnosticProbe();
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
                diagnostic.proxy,
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

    private static final class ConnectionProbe {
        private OutgoingMessage message;
        private CancellationSignal token;
        private CompletableFuture<Void> result = CompletableFuture.completedFuture(null);
        private RuntimeException synchronousFailure;
        private volatile java.util.function.Consumer<ConnectionDisconnectedEvent> disconnectedListener;

        private boolean hasDisconnectedListener() {
            return disconnectedListener != null;
        }

        private java.util.function.Consumer<MessageDataEvent> messageDataListener;
        private final MessageConnection proxy = (MessageConnection) Proxy.newProxyInstance(
                MessageConnection.class.getClassLoader(), new Class<?>[] {MessageConnection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) throws Exception {
            if (method.getName().equals("write")
                    && arguments.length == 2
                    && arguments[0] instanceof OutgoingMessage outgoing) {
                message = outgoing;
                token = (CancellationSignal) arguments[1];
                if (synchronousFailure != null) {
                    throw synchronousFailure;
                }
                // join() keeps the configured outcome's shape: a cancellation
                // raw, everything else in a CompletionException, which is what
                // the blocking write raises now.
                Outcomes.raise(result);
                return null;
            }
            if (method.getName().equals("subscribe")) {
                if (arguments[0] == Connection.Kind.DISCONNECTED) {
                    java.util.function.Consumer<ConnectionDisconnectedEvent> registered = cast(arguments[1]);
                    disconnectedListener = registered;
                    return (dev.slsk.Subscription) () -> {
                        if (disconnectedListener == registered) {
                            disconnectedListener = null;
                        }
                    };
                }
                if (arguments[0] == MessageConnection.MessageKind.DATA_READ) {
                    java.util.function.Consumer<MessageDataEvent> registered = cast(arguments[1]);
                    messageDataListener = registered;
                    return (dev.slsk.Subscription) () -> {
                        if (messageDataListener == registered) {
                            messageDataListener = null;
                        }
                    };
                }
                return (dev.slsk.Subscription) () -> {};
            }
            return defaultValue(method.getReturnType());
        }

        private void raiseDisconnected(String message) {
            disconnectedListener.accept(new ConnectionDisconnectedEvent(proxy, message, null));
        }
    }

    private static final class WaiterProbe {
        private final Map<Class<?>, CompletableFuture<?>> results = new HashMap<>();
        private final List<WaitKey> keys = new ArrayList<>();
        private final List<CancellationSignal> tokens = new ArrayList<>();
        private Duration lastTimeout;
        private WaitKey failedKey;
        private final Waiter proxy = (Waiter)
                Proxy.newProxyInstance(Waiter.class.getClassLoader(), new Class<?>[] {Waiter.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) throws Exception {
            if (method.getName().equals("register") && arguments.length == 4) {
                keys.add((WaitKey) arguments[0]);
                Class<?> resultType = (Class<?>) arguments[1];
                if (arguments[2] != null) {
                    lastTimeout = (Duration) arguments[2];
                }
                tokens.add((CancellationSignal) arguments[3]);
                CompletableFuture<?> configured = results.getOrDefault(resultType, new CompletableFuture<>());
                return (Wait<Object>) () -> Outcomes.raise(configured);
            }
            if (method.getName().equals("registerIndefinitely") && arguments.length == 3) {
                keys.add((WaitKey) arguments[0]);
                Class<?> resultType = (Class<?>) arguments[1];
                tokens.add((CancellationSignal) arguments[2]);
                CompletableFuture<?> configured = results.getOrDefault(resultType, new CompletableFuture<>());
                return (Wait<Object>) () -> Outcomes.raise(configured);
            }
            if (method.getName().equals("fail")) {
                failedKey = (WaitKey) arguments[0];
                Throwable failure = (Throwable) arguments[1];
                CompletableFuture<?> browse = results.get(BrowseResponseMessage.class);
                if (browse != null && !browse.isDone()) {
                    browse.completeExceptionally(failure);
                }
                return null;
            }
            return defaultValue(method.getReturnType());
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T cast(Object value) {
        return (T) value;
    }

    private static final class PeerManagerProbe {
        private final MessageConnection connection;
        private String username;
        private InetSocketAddress endpoint;
        private CancellationSignal token;
        private int invalidations;
        private boolean invalidationResult;
        private RuntimeException synchronousFailure;
        private final PeerConnectionManager proxy = (PeerConnectionManager) Proxy.newProxyInstance(
                PeerConnectionManager.class.getClassLoader(),
                new Class<?>[] {PeerConnectionManager.class},
                this::invoke);

        private PeerManagerProbe(MessageConnection connection) {
            this.connection = connection;
        }

        private Object invoke(Object ignored, Method method, Object[] arguments) throws Exception {
            if (method.getName().equals("tryInvalidateMessageConnectionCache")) {
                invalidations++;
                return invalidationResult;
            }
            if (method.getName().equals("getOrAddMessageConnection") && arguments.length == 3) {
                if (synchronousFailure != null) {
                    throw synchronousFailure;
                }
                username = (String) arguments[0];
                endpoint = (InetSocketAddress) arguments[1];
                token = (CancellationSignal) arguments[2];
                return connection;
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class DiagnosticProbe {
        private final List<String> debugMessages = new ArrayList<>();
        private final DiagnosticSink proxy = (DiagnosticSink) Proxy.newProxyInstance(
                DiagnosticSink.class.getClassLoader(), new Class<?>[] {DiagnosticSink.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) throws Exception {
            if (method.getName().equals("debug") && arguments.length == 1) {
                debugMessages.add((String) arguments[0]);
            }
            return null;
        }
    }
}
