// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.handlers;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.BrowseResponse;
import dev.slsk.CancellationSignal;
import dev.slsk.Directory;
import dev.slsk.File;
import dev.slsk.RawBrowseResponse;
import dev.slsk.RawSearchResponse;
import dev.slsk.SearchQuery;
import dev.slsk.SearchResponse;
import dev.slsk.SearchScope;
import dev.slsk.SearchStates;
import dev.slsk.TransferDirection;
import dev.slsk.UserInfo;
import dev.slsk.common.Constants;
import dev.slsk.common.WaitKey;
import dev.slsk.common.Waiter;
import dev.slsk.diagnostics.DiagnosticSink;
import dev.slsk.events.DownloadDeniedEvent;
import dev.slsk.events.DownloadFailedEvent;
import dev.slsk.exceptions.DownloadEnqueueException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.exceptions.TransferReportedFailedException;
import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.messages.BrowseRequest;
import dev.slsk.messaging.messages.FolderContentsRequest;
import dev.slsk.messaging.messages.FolderContentsResponse;
import dev.slsk.messaging.messages.OutgoingMessage;
import dev.slsk.messaging.messages.PlaceInQueueRequest;
import dev.slsk.messaging.messages.PlaceInQueueResponse;
import dev.slsk.messaging.messages.QueueDownloadRequest;
import dev.slsk.messaging.messages.TransferRequest;
import dev.slsk.messaging.messages.TransferResponse;
import dev.slsk.messaging.messages.UploadDenied;
import dev.slsk.messaging.messages.UploadFailed;
import dev.slsk.network.MessageConnection;
import dev.slsk.network.MessageEvent;
import dev.slsk.network.MessageReceivedEvent;
import dev.slsk.options.BrowseResponseResolver;
import dev.slsk.options.DirectoryContentsResolver;
import dev.slsk.options.EnqueueDownloadCallback;
import dev.slsk.options.PlaceInQueueResolver;
import dev.slsk.options.SearchResponseResolver;
import dev.slsk.options.SoulseekClientOptions;
import dev.slsk.options.SoulseekClientOptionsPatch;
import dev.slsk.options.UserInfoResolver;
import dev.slsk.search.SearchInternal;
import dev.slsk.transfer.TransferInternal;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class PeerMessageHandlerTest {
    private static final String USERNAME = "peer";
    private static final String FILENAME = "music\\track.mp3";
    private static final int TOKEN = 0x10203040;
    private static final InetSocketAddress ENDPOINT = endpoint(44001);

    @Test
    void constructionRequiresClient() {
        assertThrows(NullPointerException.class, () -> new DefaultPeerMessageHandler(null));
    }

    @Test
    void correlatedResponsesCompleteExpectedWaits() {
        Fixture fixture = new Fixture(new SoulseekClientOptions());

        fixture.handler
                .handleMessageReadAsync(
                        fixture.connection.proxy,
                        new FolderContentsResponse(TOKEN, "dir", List.of(new Directory("dir"))).toByteArray())
                .join();
        fixture.handler
                .handleMessageReadAsync(fixture.connection.proxy, new UserInfo("description", 2, 3, true).toByteArray())
                .join();
        fixture.handler
                .handleMessageReadAsync(fixture.connection.proxy, new TransferResponse(TOKEN, 123L).toByteArray())
                .join();
        fixture.handler
                .handleMessageReadAsync(fixture.connection.proxy, new PlaceInQueueResponse(FILENAME, 7).toByteArray())
                .join();

        assertInstanceOf(
                List.class,
                fixture.waiter.completed.get(new WaitKey(MessageCode.Peer.FOLDER_CONTENTS_RESPONSE, USERNAME, TOKEN)));
        assertInstanceOf(
                UserInfo.class, fixture.waiter.completed.get(new WaitKey(MessageCode.Peer.INFO_RESPONSE, USERNAME)));
        assertInstanceOf(
                TransferResponse.class,
                fixture.waiter.completed.get(new WaitKey(MessageCode.Peer.TRANSFER_RESPONSE, USERNAME, TOKEN)));
        assertInstanceOf(
                PlaceInQueueResponse.class,
                fixture.waiter.completed.get(
                        new WaitKey(MessageCode.Peer.PLACE_IN_QUEUE_RESPONSE, USERNAME, FILENAME)));
    }

    @Test
    void browseResponseCompletesWaitAndInvalidPayloadFailsIt() {
        Fixture fixture = new Fixture(new SoulseekClientOptions());
        BrowseResponse response = new BrowseResponse(List.of(new Directory("shared")));

        fixture.handler
                .handleMessageReadAsync(fixture.connection.proxy, response.toByteArray())
                .join();

        WaitKey key = new WaitKey(MessageCode.Peer.BROWSE_RESPONSE, USERNAME);
        assertInstanceOf(BrowseResponse.class, fixture.waiter.completed.get(key));

        fixture.handler
                .handleMessageReadAsync(
                        fixture.connection.proxy,
                        new MessageBuilder()
                                .writeCode(MessageCode.Peer.BROWSE_RESPONSE)
                                .writeByte(1)
                                .build())
                .join();
        assertInstanceOf(MessageReadException.class, fixture.waiter.failures.get(key));
        assertTrue(fixture.diagnostic.containsWarning("Error handling peer message"));
    }

    @Test
    void activeSearchReceivesResponseAndInactiveSearchIsIgnored() {
        Fixture fixture = new Fixture(new SoulseekClientOptions());
        SearchInternal search = new SearchInternal(SearchQuery.fromText("query"), SearchScope.getNetwork(), TOKEN);
        search.setState(SearchStates.IN_PROGRESS);
        fixture.client.searches.put(TOKEN, search);
        SearchResponse response =
                new SearchResponse(USERNAME, TOKEN, true, 100, 0, List.of(new File(1, FILENAME, 123L, "mp3")));

        fixture.handler
                .handleMessageReadAsync(fixture.connection.proxy, response.toByteArray())
                .join();
        fixture.client.searches.clear();
        fixture.handler
                .handleMessageReadAsync(fixture.connection.proxy, response.toByteArray())
                .join();

        assertEquals(1, search.getResponseCount());
        assertEquals(1, search.getFileCount());
        search.close();
    }

    @Test
    void infoRequestWritesResolvedResponseAndFailureUsesDefault() {
        UserInfo info = new UserInfo("resolved", 2, 3, true);
        Fixture resolved = new Fixture(
                options(null, null, null, (username, endpoint) -> CompletableFuture.completedFuture(info), null, null));

        resolved.handler
                .handleMessageReadAsync(
                        resolved.connection.proxy, new dev.slsk.messaging.messages.UserInfoRequest().toByteArray())
                .join();
        assertArrayEquals(info.toByteArray(), resolved.connection.bytes.getFirst());

        Fixture failed = new Fixture(options(
                null,
                null,
                null,
                (username, endpoint) -> CompletableFuture.failedFuture(new RuntimeException("info resolver")),
                null,
                null));
        failed.handler
                .handleMessageReadAsync(
                        failed.connection.proxy, new dev.slsk.messaging.messages.UserInfoRequest().toByteArray())
                .join();
        assertEquals(1, failed.connection.bytes.size());
        assertTrue(failed.diagnostic.containsWarning("Failed to resolve user info response"));
    }

    @Test
    void searchRequestWritesNonemptyResponseAndSuppressesEmptyOrNull() {
        SearchResponse response =
                new SearchResponse("local", TOKEN, true, 100, 0, List.of(new File(1, FILENAME, 123L, "mp3")));
        Fixture fixture = new Fixture(options(
                (username, token, query) -> CompletableFuture.completedFuture(response), null, null, null, null, null));
        fixture.handler
                .handleMessageReadAsync(fixture.connection.proxy, peerSearchRequest(TOKEN, "query"))
                .join();
        assertArrayEquals(response.toByteArray(), fixture.connection.bytes.getFirst());

        fixture = new Fixture(options(
                (username, token, query) ->
                        CompletableFuture.completedFuture(new SearchResponse("local", token, true, 0, 0, List.of())),
                null,
                null,
                null,
                null,
                null));
        fixture.handler
                .handleMessageReadAsync(fixture.connection.proxy, peerSearchRequest(TOKEN, "empty"))
                .join();
        assertTrue(fixture.connection.bytes.isEmpty());

        fixture = new Fixture(options(
                (username, token, query) -> CompletableFuture.completedFuture(null), null, null, null, null, null));
        fixture.handler
                .handleMessageReadAsync(fixture.connection.proxy, peerSearchRequest(TOKEN, "null"))
                .join();
        assertTrue(fixture.connection.bytes.isEmpty());
    }

    @Test
    void rawSearchUsesExactLengthClosesStreamAndResolverFailureWarns() {
        ClosingInputStream stream = new ClosingInputStream(new byte[] {1, 2, 3});
        Fixture fixture = new Fixture(options(
                (username, token, query) -> CompletableFuture.completedFuture(new RawSearchResponse(3, stream)),
                null,
                null,
                null,
                null,
                null));

        fixture.handler
                .handleMessageReadAsync(fixture.connection.proxy, peerSearchRequest(TOKEN, "raw"))
                .join();

        assertEquals(3L, fixture.connection.rawLength);
        assertSame(stream, fixture.connection.rawStream);
        assertTrue(stream.closed);

        Fixture failed = new Fixture(options(
                (username, token, query) -> CompletableFuture.failedFuture(new RuntimeException("search resolver")),
                null,
                null,
                null,
                null,
                null));
        failed.handler
                .handleMessageReadAsync(failed.connection.proxy, peerSearchRequest(TOKEN, "failed"))
                .join();
        assertTrue(failed.diagnostic.containsWarning("Error resolving search response"));
    }

    @Test
    void browseRequestWritesResolvedAndRawResponsesAndFallsBackOnFailure() {
        BrowseResponse response = new BrowseResponse(List.of(new Directory("shared")));
        Fixture resolved = new Fixture(options(
                null, (username, endpoint) -> CompletableFuture.completedFuture(response), null, null, null, null));
        resolved.handler
                .handleMessageReadAsync(resolved.connection.proxy, new BrowseRequest().toByteArray())
                .join();
        assertArrayEquals(response.toByteArray(), resolved.connection.bytes.getFirst());

        ClosingInputStream stream = new ClosingInputStream(new byte[] {1, 2, 3});
        Fixture raw = new Fixture(options(
                null,
                (username, endpoint) -> CompletableFuture.completedFuture(new RawBrowseResponse(3, stream)),
                null,
                null,
                null,
                null));
        raw.handler
                .handleMessageReadAsync(raw.connection.proxy, new BrowseRequest().toByteArray())
                .join();
        assertEquals(3L, raw.connection.rawLength);
        assertSame(stream, raw.connection.rawStream);
        assertTrue(stream.closed);

        Fixture failed = new Fixture(options(
                null,
                (username, endpoint) -> CompletableFuture.failedFuture(new RuntimeException("browse resolver")),
                null,
                null,
                null,
                null));
        failed.handler
                .handleMessageReadAsync(failed.connection.proxy, new BrowseRequest().toByteArray())
                .join();
        assertEquals(1, failed.connection.bytes.size());
        assertTrue(failed.diagnostic.containsWarning("Failed to resolve browse response"));
    }

    @Test
    void folderRequestWritesResolvedContentsAndFailureOnlyWarns() {
        Directory directory = new Directory("shared", List.of(new File(1, FILENAME, 123L, "mp3")));
        Fixture resolved = new Fixture(options(
                null,
                null,
                (username, endpoint, token, name) -> CompletableFuture.completedFuture(List.of(directory)),
                null,
                null,
                null));
        resolved.handler
                .handleMessageReadAsync(
                        resolved.connection.proxy, new FolderContentsRequest(TOKEN, "shared").toByteArray())
                .join();
        FolderContentsResponse expected = new FolderContentsResponse(TOKEN, "shared", List.of(directory));
        assertArrayEquals(
                expected.toByteArray(), resolved.connection.outgoing.getFirst().toByteArray());

        Fixture failed = new Fixture(options(
                null,
                null,
                (username, endpoint, token, name) ->
                        CompletableFuture.failedFuture(new RuntimeException("directory resolver")),
                null,
                null,
                null));
        failed.handler
                .handleMessageReadAsync(
                        failed.connection.proxy, new FolderContentsRequest(TOKEN, "shared").toByteArray())
                .join();
        assertTrue(failed.connection.outgoing.isEmpty());
        assertTrue(failed.diagnostic.containsWarning("Failed to resolve directory contents response"));
    }

    @Test
    void queueDownloadSendsPlaceOrDenialWithSourceMessages() {
        Fixture queued = new Fixture(options(
                null,
                null,
                null,
                null,
                (username, endpoint, filename) -> CompletableFuture.completedFuture(null),
                (username, endpoint, filename) -> CompletableFuture.completedFuture(4)));
        queued.handler
                .handleMessageReadAsync(queued.connection.proxy, new QueueDownloadRequest(FILENAME).toByteArray())
                .join();
        assertArrayEquals(
                new PlaceInQueueResponse(FILENAME, 4).toByteArray(),
                queued.connection.outgoing.getFirst().toByteArray());

        Fixture rejected = new Fixture(options(
                null,
                null,
                null,
                null,
                (username, endpoint, filename) ->
                        CompletableFuture.failedFuture(new DownloadEnqueueException("No slot")),
                null));
        rejected.handler
                .handleMessageReadAsync(rejected.connection.proxy, new QueueDownloadRequest(FILENAME).toByteArray())
                .join();
        assertArrayEquals(
                new UploadDenied(FILENAME, "No slot").toByteArray(),
                rejected.connection.outgoing.getFirst().toByteArray());

        Fixture failed = new Fixture(options(
                null,
                null,
                null,
                null,
                (username, endpoint, filename) -> CompletableFuture.failedFuture(new RuntimeException("enqueue")),
                null));
        failed.handler
                .handleMessageReadAsync(failed.connection.proxy, new QueueDownloadRequest(FILENAME).toByteArray())
                .join();
        assertArrayEquals(
                new UploadDenied(FILENAME, "Enqueue failed due to internal error").toByteArray(),
                failed.connection.outgoing.getFirst().toByteArray());
        assertTrue(failed.diagnostic.containsWarning("Failed to invoke QueueDownload action"));
    }

    @Test
    void downloadTransferRequestSendsQueuedOrTwoRejectionMessages() {
        Fixture queued = new Fixture(options(
                null,
                null,
                null,
                null,
                (username, endpoint, filename) -> CompletableFuture.completedFuture(null),
                (username, endpoint, filename) -> CompletableFuture.completedFuture(null)));
        queued.handler
                .handleMessageReadAsync(
                        queued.connection.proxy,
                        new TransferRequest(TransferDirection.DOWNLOAD, TOKEN, FILENAME).toByteArray())
                .join();
        assertEquals(1, queued.connection.outgoing.size());
        assertArrayEquals(
                new TransferResponse(TOKEN, "Queued").toByteArray(),
                queued.connection.outgoing.getFirst().toByteArray());

        Fixture rejected = new Fixture(options(
                null,
                null,
                null,
                null,
                (username, endpoint, filename) ->
                        CompletableFuture.failedFuture(new DownloadEnqueueException("Rejected")),
                null));
        rejected.handler
                .handleMessageReadAsync(
                        rejected.connection.proxy,
                        new TransferRequest(TransferDirection.DOWNLOAD, TOKEN, FILENAME).toByteArray())
                .join();
        assertEquals(2, rejected.connection.outgoing.size());
        assertArrayEquals(
                new TransferResponse(TOKEN, "Rejected").toByteArray(),
                rejected.connection.outgoing.get(0).toByteArray());
        assertArrayEquals(
                new UploadDenied(FILENAME, "Rejected").toByteArray(),
                rejected.connection.outgoing.get(1).toByteArray());
    }

    @Test
    void uploadTransferRequestCompletesTrackedWaitAndCancelsUnknown() {
        Fixture tracked = new Fixture(new SoulseekClientOptions());
        tracked.client.downloads.put(1, new TransferInternal(TransferDirection.DOWNLOAD, USERNAME, FILENAME, TOKEN));
        tracked.handler
                .handleMessageReadAsync(
                        tracked.connection.proxy,
                        new TransferRequest(TransferDirection.UPLOAD, TOKEN, FILENAME).toByteArray())
                .join();
        assertInstanceOf(
                TransferRequest.class,
                tracked.waiter.completed.get(new WaitKey(MessageCode.Peer.TRANSFER_REQUEST, USERNAME, FILENAME)));
        assertTrue(tracked.connection.outgoing.isEmpty());

        Fixture unknown = new Fixture(new SoulseekClientOptions());
        unknown.handler
                .handleMessageReadAsync(
                        unknown.connection.proxy,
                        new TransferRequest(TransferDirection.UPLOAD, TOKEN, FILENAME).toByteArray())
                .join();
        assertArrayEquals(
                new TransferResponse(TOKEN, "Cancelled").toByteArray(),
                unknown.connection.outgoing.getFirst().toByteArray());
    }

    @Test
    void placeRequestWritesOnlyNonNullPlaceAndLogsResolverFailure() {
        Fixture placed = new Fixture(options(
                null, null, null, null, null, (username, endpoint, filename) -> CompletableFuture.completedFuture(9)));
        placed.handler
                .handleMessageReadAsync(placed.connection.proxy, new PlaceInQueueRequest(FILENAME).toByteArray())
                .join();
        assertArrayEquals(
                new PlaceInQueueResponse(FILENAME, 9).toByteArray(),
                placed.connection.outgoing.getFirst().toByteArray());

        Fixture absent = new Fixture(options(
                null,
                null,
                null,
                null,
                null,
                (username, endpoint, filename) -> CompletableFuture.completedFuture(null)));
        absent.handler
                .handleMessageReadAsync(absent.connection.proxy, new PlaceInQueueRequest(FILENAME).toByteArray())
                .join();
        assertTrue(absent.connection.outgoing.isEmpty());

        Fixture failed = new Fixture(options(
                null,
                null,
                null,
                null,
                null,
                (username, endpoint, filename) ->
                        CompletableFuture.failedFuture(new RuntimeException("place resolver"))));
        failed.handler
                .handleMessageReadAsync(failed.connection.proxy, new PlaceInQueueRequest(FILENAME).toByteArray())
                .join();
        assertTrue(failed.diagnostic.containsWarning("Failed to resolve place in queue"));
    }

    @Test
    void deniedAndFailedMessagesFailWaitsAndRaiseEvents() {
        Fixture fixture = new Fixture(new SoulseekClientOptions());
        List<DownloadDeniedEvent> deniedEvents = new ArrayList<>();
        List<DownloadFailedEvent> failedEvents = new ArrayList<>();
        fixture.handler.addDownloadDeniedListener((sender, eventData) -> deniedEvents.add(eventData));
        fixture.handler.addDownloadFailedListener((sender, eventData) -> failedEvents.add(eventData));

        fixture.handler
                .handleMessageReadAsync(fixture.connection.proxy, new UploadDenied(FILENAME, "No slot").toByteArray())
                .join();
        fixture.handler
                .handleMessageReadAsync(fixture.connection.proxy, new UploadFailed(FILENAME).toByteArray())
                .join();

        WaitKey key = new WaitKey(MessageCode.Peer.TRANSFER_REQUEST, USERNAME, FILENAME);
        assertInstanceOf(TransferReportedFailedException.class, fixture.waiter.failures.get(key));
        assertEquals("No slot", deniedEvents.getFirst().getMessage());
        assertEquals(FILENAME, deniedEvents.getFirst().getFilename());
        assertEquals(USERNAME, deniedEvents.getFirst().getUsername());
        assertEquals(FILENAME, failedEvents.getFirst().getFilename());

        Fixture deniedOnly = new Fixture(new SoulseekClientOptions());
        deniedOnly
                .handler
                .handleMessageReadAsync(
                        deniedOnly.connection.proxy, new UploadDenied(FILENAME, "No slot").toByteArray())
                .join();
        assertInstanceOf(TransferRejectedException.class, deniedOnly.waiter.failures.get(key));
    }

    @Test
    void receiptAndWrittenCallbacksCorrelateBrowseAndLogCodes() {
        Fixture fixture = new Fixture(new SoulseekClientOptions());
        byte[] response = new BrowseResponse(List.of()).toByteArray();
        MessageReceivedEvent received = new MessageReceivedEvent(response.length, Arrays.copyOfRange(response, 4, 8));

        fixture.handler.handleMessageReceived(fixture.connection.proxy, received);
        Object result =
                fixture.waiter.completed.get(new WaitKey(Constants.WaitKey.BROWSE_RESPONSE_CONNECTION, USERNAME));
        BrowseResponseConnection browse = assertInstanceOf(BrowseResponseConnection.class, result);
        assertSame(received, browse.eventData());
        assertSame(fixture.connection.proxy, browse.connection());

        fixture.handler.handleMessageReceived(
                fixture.connection.proxy,
                new MessageReceivedEvent(8, Arrays.copyOfRange(new BrowseRequest().toByteArray(), 4, 8)));
        fixture.handler.handleMessageWritten(
                fixture.connection.proxy, new MessageEvent(new BrowseRequest().toByteArray()));
        assertTrue(fixture.diagnostic.contains("Peer message sent: BROWSE_REQUEST"));
    }

    private static SoulseekClientOptions options(
            SearchResponseResolver search,
            BrowseResponseResolver browse,
            DirectoryContentsResolver directories,
            UserInfoResolver info,
            EnqueueDownloadCallback enqueue,
            PlaceInQueueResolver place) {
        SoulseekClientOptionsPatch patch = new SoulseekClientOptionsPatch(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                search,
                null,
                browse,
                directories,
                info,
                enqueue,
                place);
        return new SoulseekClientOptions().with(patch);
    }

    private static byte[] peerSearchRequest(int token, String query) {
        return new MessageBuilder()
                .writeCode(MessageCode.Peer.SEARCH_REQUEST)
                .writeInteger(token)
                .writeString(query)
                .build();
    }

    private static InetSocketAddress endpoint(int port) {
        try {
            return new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port);
        } catch (Exception exception) {
            throw new AssertionError(exception);
        }
    }

    private static final class Fixture {
        private final RecordingDiagnostic diagnostic = new RecordingDiagnostic();
        private final RecordingWaiter waiter = new RecordingWaiter();
        private final FakeClient client;
        private final ConnectionProbe connection = new ConnectionProbe();
        private final DefaultPeerMessageHandler handler;

        private Fixture(SoulseekClientOptions options) {
            client = new FakeClient(options, waiter);
            handler = new DefaultPeerMessageHandler(client, diagnostic);
        }
    }

    private static final class FakeClient implements PeerMessageHandlerClient {
        private final SoulseekClientOptions options;
        private final Waiter waiter;
        private final Map<Integer, SearchInternal> searches = new HashMap<>();
        private final Map<Integer, TransferInternal> downloads = new HashMap<>();

        private FakeClient(SoulseekClientOptions options, Waiter waiter) {
            this.options = options;
            this.waiter = waiter;
        }

        @Override
        public SoulseekClientOptions getOptions() {
            return options;
        }

        @Override
        public Waiter getWaiter() {
            return waiter;
        }

        @Override
        public Map<Integer, SearchInternal> getSearches() {
            return searches;
        }

        @Override
        public Map<Integer, TransferInternal> getDownloadDictionary() {
            return downloads;
        }
    }

    private static final class RecordingWaiter implements Waiter {
        private final Map<WaitKey, Object> completed = new HashMap<>();
        private final Map<WaitKey, Throwable> failures = new HashMap<>();

        @Override
        public int getDefaultTimeout() {
            return 5_000;
        }

        @Override
        public void cancel(WaitKey key) {}

        @Override
        public void cancelAll() {}

        @Override
        public void complete(WaitKey key) {
            completed.put(key, null);
        }

        @Override
        public <T> void complete(WaitKey key, T result) {
            completed.put(key, result);
        }

        @Override
        public boolean hasWait(WaitKey key) {
            return false;
        }

        @Override
        public void fail(WaitKey key, Throwable exception) {
            failures.put(key, exception);
        }

        @Override
        public void timeout(WaitKey key) {}

        @Override
        public CompletableFuture<Void> waitAsync(WaitKey key) {
            return new CompletableFuture<>();
        }

        @Override
        public CompletableFuture<Void> waitAsync(WaitKey key, Integer timeout) {
            return waitAsync(key);
        }

        @Override
        public CompletableFuture<Void> waitAsync(WaitKey key, Integer timeout, CancellationSignal cancellationSignal) {
            return waitAsync(key);
        }

        @Override
        public <T> CompletableFuture<T> waitAsync(WaitKey key, Class<T> resultType) {
            return new CompletableFuture<>();
        }

        @Override
        public <T> CompletableFuture<T> waitAsync(WaitKey key, Class<T> resultType, Integer timeout) {
            return waitAsync(key, resultType);
        }

        @Override
        public <T> CompletableFuture<T> waitAsync(
                WaitKey key, Class<T> resultType, Integer timeout, CancellationSignal cancellationSignal) {
            return waitAsync(key, resultType);
        }

        @Override
        public CompletableFuture<Void> waitIndefinitelyAsync(WaitKey key) {
            return waitAsync(key);
        }

        @Override
        public CompletableFuture<Void> waitIndefinitelyAsync(WaitKey key, CancellationSignal cancellationSignal) {
            return waitAsync(key);
        }

        @Override
        public <T> CompletableFuture<T> waitIndefinitelyAsync(WaitKey key, Class<T> resultType) {
            return waitAsync(key, resultType);
        }

        @Override
        public <T> CompletableFuture<T> waitIndefinitelyAsync(
                WaitKey key, Class<T> resultType, CancellationSignal cancellationSignal) {
            return waitAsync(key, resultType);
        }

        @Override
        public void close() {}
    }

    private static final class ConnectionProbe {
        private final List<byte[]> bytes = new ArrayList<>();
        private final List<OutgoingMessage> outgoing = new ArrayList<>();
        private long rawLength = -1;
        private InputStream rawStream;
        private final MessageConnection proxy = (MessageConnection) Proxy.newProxyInstance(
                MessageConnection.class.getClassLoader(), new Class<?>[] {MessageConnection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            return switch (method.getName()) {
                case "getUsername" -> USERNAME;
                case "getIpEndpoint" -> ENDPOINT;
                case "getId" -> UUID.fromString("00000000-0000-0000-0000-000000000001");
                case "writeAsync" -> {
                    if (arguments[0] instanceof byte[] value) {
                        bytes.add(Arrays.copyOf(value, value.length));
                    } else if (arguments[0] instanceof OutgoingMessage message) {
                        outgoing.add(message);
                    } else if (arguments[0] instanceof Long length) {
                        rawLength = length;
                        rawStream = (InputStream) arguments[1];
                    }
                    yield CompletableFuture.completedFuture(null);
                }
                case "toString" -> "ConnectionProbe";
                default -> defaultValue(method.getReturnType());
            };
        }
    }

    private static final class ClosingInputStream extends ByteArrayInputStream {
        private boolean closed;

        private ClosingInputStream(byte[] bytes) {
            super(bytes);
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }

    private static final class RecordingDiagnostic implements DiagnosticSink {
        private final List<String> messages = new ArrayList<>();
        private final List<String> warnings = new ArrayList<>();

        private boolean contains(String value) {
            return messages.stream().anyMatch(message -> message.toLowerCase().contains(value.toLowerCase()));
        }

        private boolean containsWarning(String value) {
            return warnings.stream().anyMatch(message -> message.toLowerCase().contains(value.toLowerCase()));
        }

        @Override
        public void trace(String message) {
            messages.add(message);
        }

        @Override
        public void trace(String message, Throwable exception) {
            messages.add(message);
        }

        @Override
        public void debug(String message) {
            messages.add(message);
        }

        @Override
        public void debug(String message, Throwable exception) {
            messages.add(message);
        }

        @Override
        public void info(String message) {
            messages.add(message);
        }

        @Override
        public void warning(String message) {
            messages.add(message);
            warnings.add(message);
        }

        @Override
        public void warning(String message, Throwable exception) {
            messages.add(message);
            warnings.add(message);
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
            return 0d;
        }
        if (type == float.class) {
            return 0f;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == char.class) {
            return '\0';
        }
        return null;
    }
}
