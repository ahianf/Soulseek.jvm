// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.common.WaitKey;
import dev.slsk.common.Waiter;
import dev.slsk.diagnostics.DiagnosticSink;
import dev.slsk.events.BrowseProgressUpdatedEvent;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.exceptions.TransferNotFoundException;
import dev.slsk.exceptions.UserEndpointException;
import dev.slsk.exceptions.UserOfflineException;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.handlers.BrowseResponseConnection;
import dev.slsk.messaging.messages.BrowseRequest;
import dev.slsk.messaging.messages.FolderContentsRequest;
import dev.slsk.messaging.messages.OutgoingMessage;
import dev.slsk.messaging.messages.PlaceInQueueRequest;
import dev.slsk.messaging.messages.PlaceInQueueResponse;
import dev.slsk.messaging.messages.UserAddressRequest;
import dev.slsk.messaging.messages.UserAddressResponse;
import dev.slsk.messaging.messages.UserInfoRequest;
import dev.slsk.network.MessageConnection;
import dev.slsk.network.MessageConnectionEventListener;
import dev.slsk.network.MessageDataEvent;
import dev.slsk.network.MessageReceivedEvent;
import dev.slsk.network.PeerConnectionManager;
import dev.slsk.network.tcp.ConnectionDisconnectedEvent;
import dev.slsk.network.tcp.ConnectionEventListener;
import dev.slsk.options.BrowseOptions;
import dev.slsk.transfer.TransferInternal;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class SoulseekClientPeerRequestTest {
    private static final InetSocketAddress ENDPOINT = new InetSocketAddress(InetAddress.getLoopbackAddress(), 46001);

    @Test
    void connectToUserResolvesEndpointAndGetsConnection() {
        Fixture fixture = new Fixture();
        fixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));
        CancellationController source = new CancellationController();
        CancellationSignal token = source.getSignal();

        fixture.client.connectToUserAsync("alice", token).join();

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
    void connectToUserInvalidatesOnRequestAndLogsOnlySuccess() {
        Fixture fixture = new Fixture();
        fixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));
        fixture.peerManager.invalidationResult = false;

        fixture.client.connectToUserAsync("alice", true).join();

        assertEquals(1, fixture.peerManager.invalidations);
        assertEquals(List.of(), fixture.diagnostic.debugMessages);

        fixture.peerManager.invalidationResult = true;
        fixture.client.connectToUserAsync("alice", true).join();
        assertEquals(2, fixture.peerManager.invalidations);
        assertEquals(List.of("Invalidated message connection cache for alice"), fixture.diagnostic.debugMessages);
        fixture.close();
    }

    @Test
    void getUserInfoRegistersWaitThenUsesPeerConnection() {
        Fixture fixture = new Fixture();
        UserInfo expected = new UserInfo("description", 3, 4, true, new byte[] {1, 2});
        fixture.waiter.results.put(UserInfo.class, CompletableFuture.completedFuture(expected));
        fixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));
        CancellationController source = new CancellationController();
        CancellationSignal token = source.getSignal();

        UserInfo actual = fixture.client.getUserInfoAsync("alice", token).join();

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
            assertThrows(IllegalArgumentException.class, () -> fixture.client.connectToUserAsync(bad));
            assertThrows(IllegalArgumentException.class, () -> fixture.client.getUserInfoAsync(bad));
        }
        fixture.client.setStateForTest(SoulseekClientStates.DISCONNECTED);
        assertThrows(IllegalStateException.class, () -> fixture.client.connectToUserAsync("alice"));
        assertThrows(IllegalStateException.class, () -> fixture.client.getUserInfoAsync("alice"));
        fixture.close();
    }

    @Test
    void connectPreservesOfflineTimeoutAndCancellation() {
        Fixture fixture = new Fixture();
        UserOfflineException offline = new UserOfflineException("offline");
        fixture.waiter.results.put(UserAddressResponse.class, CompletableFuture.failedFuture(offline));
        assertSame(offline, failureOf(fixture.client.connectToUserAsync("alice")));

        TimeoutException timeout = new TimeoutException("timed out");
        fixture.server.result = CompletableFuture.failedFuture(timeout);
        fixture.waiter.results.put(UserAddressResponse.class, new CompletableFuture<>());
        assertSame(timeout, failureOf(fixture.client.connectToUserAsync("bob")));

        CancellationException cancellation = new CancellationException("cancelled");
        fixture.server.result = CompletableFuture.failedFuture(cancellation);
        assertSame(cancellation, failureOf(fixture.client.connectToUserAsync("carol")));
        fixture.close();
    }

    @Test
    void connectWrapsEndpointAndManagerFailuresAtSourceLayers() {
        Fixture fixture = new Fixture();
        RuntimeException endpointFailure = new RuntimeException("endpoint failed");
        fixture.server.synchronousFailure = endpointFailure;

        SoulseekClientException outer =
                assertInstanceOf(SoulseekClientException.class, failureOf(fixture.client.connectToUserAsync("alice")));
        UserEndpointException endpoint = assertInstanceOf(UserEndpointException.class, outer.getCause());
        assertSame(endpointFailure, endpoint.getCause());

        fixture.server.synchronousFailure = null;
        fixture.server.result = CompletableFuture.completedFuture(null);
        fixture.waiter.results.put(
                UserAddressResponse.class, CompletableFuture.completedFuture(new UserAddressResponse("bob", ENDPOINT)));
        RuntimeException managerFailure = new RuntimeException("manager failed");
        fixture.peerManager.synchronousFailure = managerFailure;

        SoulseekClientException managerMapped =
                assertInstanceOf(SoulseekClientException.class, failureOf(fixture.client.connectToUserAsync("bob")));
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
        assertSame(timeout, failureOf(fixture.client.getUserInfoAsync("alice")));

        CancellationException cancellation = new CancellationException("cancelled");
        fixture.peer.result = CompletableFuture.failedFuture(cancellation);
        assertSame(cancellation, failureOf(fixture.client.getUserInfoAsync("bob")));

        RuntimeException expected = new RuntimeException("peer write failed");
        fixture.peer.result = CompletableFuture.failedFuture(expected);
        SoulseekClientException mapped =
                assertInstanceOf(SoulseekClientException.class, failureOf(fixture.client.getUserInfoAsync("carol")));
        assertSame(expected, mapped.getCause());

        UserOfflineException offline = new UserOfflineException("offline");
        fixture.peer.result = CompletableFuture.completedFuture(null);
        fixture.waiter.results.put(UserAddressResponse.class, CompletableFuture.failedFuture(offline));
        assertSame(offline, failureOf(fixture.client.getUserInfoAsync("dave")));
        fixture.close();
    }

    @Test
    void directoryContentsUsesTokenCorrelationAndReturnsSnapshot() {
        Fixture fixture = new Fixture();
        fixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));
        List<Directory> source = new ArrayList<>(List.of(new Directory("shared")));
        fixture.waiter.results.put(List.class, CompletableFuture.completedFuture(source));
        CancellationController cancellationController = new CancellationController();
        CancellationSignal cancellationSignal = cancellationController.getSignal();

        List<Directory> result = fixture.client
                .getDirectoryContentsAsync("alice", "shared", 123, cancellationSignal)
                .join();

        assertEquals(1, result.size());
        source.clear();
        assertEquals(1, result.size());
        assertThrows(UnsupportedOperationException.class, () -> result.add(new Directory("other")));
        assertEquals(new WaitKey(MessageCode.Peer.FOLDER_CONTENTS_RESPONSE, "alice", 123), fixture.waiter.keys.get(0));
        FolderContentsRequest request = assertInstanceOf(FolderContentsRequest.class, fixture.peer.message);
        assertEquals(123, request.getToken());
        assertEquals("shared", request.getDirectoryName());
        assertSame(cancellationSignal, fixture.peer.token);
        fixture.waiter.tokens.forEach(recorded -> assertSame(cancellationSignal, recorded));
        fixture.close();
    }

    @Test
    void queuePlaceRequiresActiveDownloadAndReturnsResponse() {
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

        int result = fixture.client
                .getDownloadPlaceInQueueAsync("alice", "file", token)
                .join();

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
                    IllegalArgumentException.class, () -> fixture.client.getDirectoryContentsAsync(bad, "directory"));
            assertThrows(IllegalArgumentException.class, () -> fixture.client.getDirectoryContentsAsync("alice", bad));
            assertThrows(
                    IllegalArgumentException.class, () -> fixture.client.getDownloadPlaceInQueueAsync(bad, "file"));
            assertThrows(
                    IllegalArgumentException.class, () -> fixture.client.getDownloadPlaceInQueueAsync("alice", bad));
        }
        assertThrows(
                TransferNotFoundException.class, () -> fixture.client.getDownloadPlaceInQueueAsync("alice", "file"));
        fixture.client.setDownloadsForTest(new HashMap<>(Map.of(
                1,
                new TransferInternal(TransferDirection.DOWNLOAD, "other", "file", 1),
                2,
                new TransferInternal(TransferDirection.DOWNLOAD, "alice", "other", 2))));
        assertThrows(
                TransferNotFoundException.class, () -> fixture.client.getDownloadPlaceInQueueAsync("alice", "file"));

        fixture.client.setStateForTest(SoulseekClientStates.DISCONNECTED);
        assertThrows(IllegalStateException.class, () -> fixture.client.getDirectoryContentsAsync("alice", "directory"));
        assertThrows(IllegalStateException.class, () -> fixture.client.getDownloadPlaceInQueueAsync("alice", "file"));
        fixture.close();
    }

    @Test
    void smallPeerQueriesPreserveSpecialFailuresAndWrapOthers() {
        Fixture directoryFixture = new Fixture();
        directoryFixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));
        directoryFixture.waiter.results.put(
                List.class, CompletableFuture.completedFuture(List.of(new Directory("shared"))));
        TimeoutException timeout = new TimeoutException("timed out");
        directoryFixture.peer.result = CompletableFuture.failedFuture(timeout);
        assertSame(timeout, failureOf(directoryFixture.client.getDirectoryContentsAsync("alice", "shared")));

        CancellationException cancellation = new CancellationException("cancelled");
        directoryFixture.peer.result = CompletableFuture.failedFuture(cancellation);
        assertSame(cancellation, failureOf(directoryFixture.client.getDirectoryContentsAsync("bob", "shared")));

        RuntimeException expected = new RuntimeException("peer failed");
        directoryFixture.peer.result = CompletableFuture.failedFuture(expected);
        SoulseekClientException mapped = assertInstanceOf(
                SoulseekClientException.class,
                failureOf(directoryFixture.client.getDirectoryContentsAsync("carol", "shared")));
        assertSame(expected, mapped.getCause());
        directoryFixture.close();

        Fixture queueFixture = new Fixture();
        queueFixture.client.setDownloadsForTest(
                new HashMap<>(Map.of(1, new TransferInternal(TransferDirection.DOWNLOAD, "alice", "file", 1))));
        UserOfflineException offline = new UserOfflineException("offline");
        queueFixture.waiter.results.put(PlaceInQueueResponse.class, new CompletableFuture<>());
        queueFixture.waiter.results.put(UserAddressResponse.class, CompletableFuture.failedFuture(offline));
        assertSame(offline, failureOf(queueFixture.client.getDownloadPlaceInQueueAsync("alice", "file")));

        RuntimeException waitFailure = new RuntimeException("wait failed");
        queueFixture.waiter.results.put(PlaceInQueueResponse.class, CompletableFuture.failedFuture(waitFailure));
        queueFixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));
        SoulseekClientException waitMapped = assertInstanceOf(
                SoulseekClientException.class,
                failureOf(queueFixture.client.getDownloadPlaceInQueueAsync("alice", "file")));
        assertSame(waitFailure, waitMapped.getCause());
        queueFixture.close();
    }

    @Test
    void browseReturnsResponseAndReportsInitialAndFinalProgress() {
        Fixture fixture = new Fixture();
        BrowseResponse response = new BrowseResponse(List.of(new Directory("shared")));
        fixture.waiter.results.put(BrowseResponse.class, CompletableFuture.completedFuture(response));
        fixture.waiter.results.put(
                BrowseResponseConnection.class,
                CompletableFuture.completedFuture(new BrowseResponseConnection(
                        new MessageReceivedEvent(104, new byte[] {1, 2, 3, 4}), fixture.peer.proxy)));
        fixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));
        List<BrowseProgressUpdatedEvent> events = new ArrayList<>();
        List<dev.slsk.options.BrowseProgress> callbacks = new ArrayList<>();
        fixture.client.addBrowseProgressUpdatedListener((sender, eventData) -> events.add(eventData));
        CancellationController source = new CancellationController();
        CancellationSignal token = source.getSignal();

        BrowseResponse actual = fixture.client
                .browseAsync("alice", new BrowseOptions(1234, callbacks::add), token)
                .join();

        assertSame(response, actual);
        assertInstanceOf(BrowseRequest.class, fixture.peer.message);
        assertEquals(
                List.of(
                        new WaitKey(MessageCode.Peer.BROWSE_RESPONSE, "alice"),
                        new WaitKey(dev.slsk.common.Constants.WaitKey.BROWSE_RESPONSE_CONNECTION, "alice"),
                        new WaitKey(MessageCode.Server.GET_PEER_ADDRESS, "alice")),
                fixture.waiter.keys);
        assertEquals(1234, fixture.waiter.lastTimeout);
        assertEquals(2, events.size());
        assertEquals(0.0, events.get(0).getPercentComplete());
        assertEquals(100.0, events.get(1).getPercentComplete());
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
            assertThrows(IllegalArgumentException.class, () -> fixture.client.browseAsync(bad));
        }
        fixture.client.setStateForTest(SoulseekClientStates.DISCONNECTED);
        assertThrows(IllegalStateException.class, () -> fixture.client.browseAsync("alice"));
        fixture.client.setStateForTest(SoulseekClientStates.CONNECTED.or(SoulseekClientStates.LOGGED_IN));

        fixture.waiter.results.put(BrowseResponse.class, new CompletableFuture<>());
        TimeoutException timeout = new TimeoutException("header timed out");
        fixture.waiter.results.put(BrowseResponseConnection.class, CompletableFuture.failedFuture(timeout));
        fixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));
        assertSame(timeout, failureOf(fixture.client.browseAsync("alice")));

        UserOfflineException offline = new UserOfflineException("offline");
        fixture.waiter.results.put(BrowseResponseConnection.class, new CompletableFuture<>());
        fixture.waiter.results.put(UserAddressResponse.class, CompletableFuture.failedFuture(offline));
        assertSame(offline, failureOf(fixture.client.browseAsync("bob")));

        CancellationException cancellation = new CancellationException("cancelled");
        fixture.waiter.results.put(BrowseResponse.class, CompletableFuture.failedFuture(cancellation));
        fixture.waiter.results.put(
                BrowseResponseConnection.class,
                CompletableFuture.completedFuture(
                        new BrowseResponseConnection(new MessageReceivedEvent(5, new byte[] {1}), fixture.peer.proxy)));
        fixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("carol", ENDPOINT)));
        assertSame(cancellation, failureOf(fixture.client.browseAsync("carol")));
        fixture.close();
    }

    @Test
    void browseFailsIndefiniteWaitOnSetupFailureAndDisconnect() {
        Fixture setupFixture = new Fixture();
        setupFixture.waiter.results.put(BrowseResponse.class, new CompletableFuture<>());
        setupFixture.waiter.results.put(BrowseResponseConnection.class, new CompletableFuture<>());
        setupFixture.waiter.results.put(
                UserAddressResponse.class,
                CompletableFuture.completedFuture(new UserAddressResponse("alice", ENDPOINT)));
        RuntimeException expected = new RuntimeException("write failed");
        setupFixture.peer.synchronousFailure = expected;

        SoulseekClientException mapped =
                assertInstanceOf(SoulseekClientException.class, failureOf(setupFixture.client.browseAsync("alice")));
        assertSame(expected, mapped.getCause());
        assertEquals(new WaitKey(MessageCode.Peer.BROWSE_RESPONSE, "alice"), setupFixture.waiter.failedKey);
        setupFixture.close();

        Fixture disconnectFixture = new Fixture();
        disconnectFixture.waiter.results.put(BrowseResponse.class, new CompletableFuture<>());
        disconnectFixture.waiter.results.put(
                BrowseResponseConnection.class,
                CompletableFuture.completedFuture(new BrowseResponseConnection(
                        new MessageReceivedEvent(5, new byte[] {1}), disconnectFixture.peer.proxy)));
        disconnectFixture.waiter.results.put(
                UserAddressResponse.class, CompletableFuture.completedFuture(new UserAddressResponse("bob", ENDPOINT)));

        CompletableFuture<BrowseResponse> operation = disconnectFixture.client.browseAsync("bob");
        disconnectFixture.peer.raiseDisconnected("gone");

        SoulseekClientException disconnected = assertInstanceOf(SoulseekClientException.class, failureOf(operation));
        ConnectionException cause = assertInstanceOf(ConnectionException.class, disconnected.getCause());
        assertEquals("Peer connection disconnected unexpectedly: gone", cause.getMessage());
        disconnectFixture.close();
    }

    private static Throwable failureOf(CompletableFuture<?> future) {
        try {
            future.join();
            throw new AssertionError("Expected operation to fail");
        } catch (CompletionException exception) {
            return exception.getCause();
        } catch (CancellationException exception) {
            return exception;
        }
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
        private final DefaultSoulseekClient client = new DefaultSoulseekClient(
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
            client.setStateForTest(SoulseekClientStates.CONNECTED.or(SoulseekClientStates.LOGGED_IN));
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
        private ConnectionEventListener<ConnectionDisconnectedEvent> disconnectedListener;
        private MessageConnectionEventListener<MessageDataEvent> messageDataListener;
        private final MessageConnection proxy = (MessageConnection) Proxy.newProxyInstance(
                MessageConnection.class.getClassLoader(), new Class<?>[] {MessageConnection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("writeAsync")
                    && arguments.length == 2
                    && arguments[0] instanceof OutgoingMessage outgoing) {
                message = outgoing;
                token = (CancellationSignal) arguments[1];
                if (synchronousFailure != null) {
                    throw synchronousFailure;
                }
                return result;
            }
            if (method.getName().equals("addDisconnectedListener")) {
                disconnectedListener = cast(arguments[0]);
                return null;
            }
            if (method.getName().equals("addMessageDataReadListener")) {
                messageDataListener = cast(arguments[0]);
                return null;
            }
            if (method.getName().equals("removeMessageDataReadListener")) {
                if (messageDataListener == arguments[0]) {
                    messageDataListener = null;
                }
                return null;
            }
            return defaultValue(method.getReturnType());
        }

        private void raiseDisconnected(String message) {
            disconnectedListener.handle(proxy, new ConnectionDisconnectedEvent(message));
        }
    }

    private static final class WaiterProbe {
        private final Map<Class<?>, CompletableFuture<?>> results = new HashMap<>();
        private final List<WaitKey> keys = new ArrayList<>();
        private final List<CancellationSignal> tokens = new ArrayList<>();
        private Integer lastTimeout;
        private WaitKey failedKey;
        private final Waiter proxy = (Waiter)
                Proxy.newProxyInstance(Waiter.class.getClassLoader(), new Class<?>[] {Waiter.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("waitAsync") && arguments.length == 4) {
                keys.add((WaitKey) arguments[0]);
                Class<?> resultType = (Class<?>) arguments[1];
                if (arguments[2] != null) {
                    lastTimeout = (Integer) arguments[2];
                }
                tokens.add((CancellationSignal) arguments[3]);
                return results.getOrDefault(resultType, new CompletableFuture<>());
            }
            if (method.getName().equals("waitIndefinitelyAsync") && arguments.length == 3) {
                keys.add((WaitKey) arguments[0]);
                Class<?> resultType = (Class<?>) arguments[1];
                tokens.add((CancellationSignal) arguments[2]);
                return results.getOrDefault(resultType, new CompletableFuture<>());
            }
            if (method.getName().equals("fail")) {
                failedKey = (WaitKey) arguments[0];
                Throwable failure = (Throwable) arguments[1];
                CompletableFuture<?> browse = results.get(BrowseResponse.class);
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

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("tryInvalidateMessageConnectionCache")) {
                invalidations++;
                return invalidationResult;
            }
            if (method.getName().equals("getOrAddMessageConnectionAsync") && arguments.length == 3) {
                if (synchronousFailure != null) {
                    throw synchronousFailure;
                }
                username = (String) arguments[0];
                endpoint = (InetSocketAddress) arguments[1];
                token = (CancellationSignal) arguments[2];
                return CompletableFuture.completedFuture(connection);
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class DiagnosticProbe {
        private final List<String> debugMessages = new ArrayList<>();
        private final DiagnosticSink proxy = (DiagnosticSink) Proxy.newProxyInstance(
                DiagnosticSink.class.getClassLoader(), new Class<?>[] {DiagnosticSink.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("debug") && arguments.length == 1) {
                debugMessages.add((String) arguments[0]);
            }
            return null;
        }
    }
}
