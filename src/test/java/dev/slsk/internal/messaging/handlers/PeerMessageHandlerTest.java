// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.handlers;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.exceptions.MessageReadException;
import dev.slsk.exceptions.TransferRejectedException;
import dev.slsk.exceptions.TransferReportedFailedException;
import dev.slsk.internal.common.Constants;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.events.DownloadDeniedEvent;
import dev.slsk.internal.events.DownloadFailedEvent;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.messages.BrowseRequest;
import dev.slsk.internal.messaging.messages.FolderContentsRequest;
import dev.slsk.internal.messaging.messages.FolderContentsResponse;
import dev.slsk.internal.messaging.messages.OutgoingMessage;
import dev.slsk.internal.messaging.messages.PlaceInQueueRequest;
import dev.slsk.internal.messaging.messages.PlaceInQueueResponse;
import dev.slsk.internal.messaging.messages.QueueDownloadRequest;
import dev.slsk.internal.messaging.messages.TransferRequest;
import dev.slsk.internal.messaging.messages.TransferResponse;
import dev.slsk.internal.messaging.messages.UploadDenied;
import dev.slsk.internal.messaging.messages.UploadFailed;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.MessageEvent;
import dev.slsk.internal.network.MessageReceivedEvent;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.internal.search.SearchInternal;
import dev.slsk.internal.search.SearchQuery;
import dev.slsk.internal.search.SearchResponse;
import dev.slsk.internal.search.SearchScope;
import dev.slsk.internal.search.SearchState;
import dev.slsk.internal.share.BrowseResponse;
import dev.slsk.internal.share.Catalogs;
import dev.slsk.internal.share.Directory;
import dev.slsk.internal.share.File;
import dev.slsk.internal.transfer.TransferDirection;
import dev.slsk.internal.transfer.TransferInternal;
import dev.slsk.internal.user.UserInfo;
import dev.slsk.search.FileAttributes;
import dev.slsk.search.SearchFile;
import dev.slsk.share.ShareIndex;
import dev.slsk.spi.ResolvedFile;
import dev.slsk.spi.ShareCatalog;
import dev.slsk.spi.UploadPolicy;
import dev.slsk.transfer.RejectionReason;
import dev.slsk.user.UserProfile;
import dev.slsk.user.Username;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PeerMessageHandlerTest {
    private static final String USERNAME = "peer";
    private static final String FILENAME = "music\\track.mp3";
    private static final int TOKEN = 0x10203040;
    private static final InetSocketAddress ENDPOINT = endpoint(44001);

    @Test
    void constructionRequiresItsPorts() {
        Fixture fixture = new Fixture(ShareCatalog.empty());
        assertThrows(
                NullPointerException.class,
                () -> new DefaultPeerMessageHandler(
                        null,
                        fixture.waiter,
                        () -> fixture.client.searches,
                        () -> fixture.client.downloads,
                        () -> "me",
                        fixture.client));
    }

    @Test
    void correlatedResponsesCompleteExpectedWaits() {
        Fixture fixture = new Fixture(new SoulseekClientOptions());

        fixture.handler.handleMessageRead(
                fixture.connection.proxy,
                new FolderContentsResponse(TOKEN, "dir", List.of(new Directory("dir"))).toByteArray());
        fixture.handler.handleMessageRead(
                fixture.connection.proxy, new UserInfo("description", 2, 3, true).toByteArray());
        fixture.handler.handleMessageRead(fixture.connection.proxy, new TransferResponse(TOKEN, 123L).toByteArray());
        fixture.handler.handleMessageRead(
                fixture.connection.proxy, new PlaceInQueueResponse(FILENAME, 7).toByteArray());

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

        fixture.handler.handleMessageRead(fixture.connection.proxy, response.toByteArray());

        WaitKey key = new WaitKey(MessageCode.Peer.BROWSE_RESPONSE, USERNAME);
        assertInstanceOf(BrowseResponse.class, fixture.waiter.completed.get(key));

        fixture.handler.handleMessageRead(
                fixture.connection.proxy,
                new MessageBuilder()
                        .writeCode(MessageCode.Peer.BROWSE_RESPONSE)
                        .writeByte(1)
                        .build());
        assertInstanceOf(MessageReadException.class, fixture.waiter.failures.get(key));
        assertTrue(fixture.diagnostic.containsWarning("Error handling peer message"));
    }

    @Test
    void activeSearchReceivesResponseAndInactiveSearchIsIgnored() {
        Fixture fixture = new Fixture(new SoulseekClientOptions());
        SearchInternal search = new SearchInternal(SearchQuery.fromText("query"), SearchScope.getNetwork(), TOKEN);
        search.setState(SearchState.IN_PROGRESS);
        fixture.client.searches.put(TOKEN, search);
        SearchResponse response =
                new SearchResponse(USERNAME, TOKEN, true, 100, 0, List.of(new File(1, FILENAME, 123L, "mp3")));

        fixture.handler.handleMessageRead(fixture.connection.proxy, response.toByteArray());
        fixture.client.searches.clear();
        fixture.handler.handleMessageRead(fixture.connection.proxy, response.toByteArray());

        assertEquals(1, search.getResponseCount());
        assertEquals(1, search.getFileCount());
        search.close();
    }

    /**
     * The profile is a value this account set, not a question asked on every
     * request. There is no failure path left to test because there is no longer
     * anything to fail: a peer asking about us is answered from a field.
     */
    @Test
    void infoRequestWritesTheProfileThisAccountSet() {
        Fixture fixture = new Fixture(new UserProfile("resolved", Optional.of(new byte[] {7}), 2, 3, true));

        fixture.handler.handleMessageRead(
                fixture.connection.proxy, new dev.slsk.internal.messaging.messages.UserInfoRequest().toByteArray());

        assertArrayEquals(
                new UserInfo("resolved", 2, 3, true, new byte[] {7}).toByteArray(),
                fixture.connection.bytes.getFirst());
        assertTrue(fixture.diagnostic.contains("User info sent to"));
    }

    @Test
    void infoRequestAnswersEvenWhenNoProfileWasSet() {
        Fixture fixture = new Fixture(new SoulseekClientOptions());

        fixture.handler.handleMessageRead(
                fixture.connection.proxy, new dev.slsk.internal.messaging.messages.UserInfoRequest().toByteArray());

        // Silence reads as a broken client, and clients that look broken do not
        // get served.
        assertArrayEquals(new UserInfo("", 0, 0, false).toByteArray(), fixture.connection.bytes.getFirst());
    }

    /**
     * The concurrency option used to be passed here by mistake, so every
     * direct peer search was answered with at most two files no matter how
     * many matched. The limit is the match cap both answer paths share.
     */
    @Test
    void searchRequestAsksTheCatalogForTheSharedMatchCap() {
        java.util.concurrent.atomic.AtomicInteger askedLimit = new java.util.concurrent.atomic.AtomicInteger(-1);
        Fixture fixture = new Fixture(new ShareCatalog() {
            @Override
            public dev.slsk.share.BrowseResponse browse(Username requester) {
                return dev.slsk.share.BrowseResponse.empty();
            }

            @Override
            public List<dev.slsk.share.Directory> directory(Username requester, String path) {
                return List.of();
            }

            @Override
            public List<SearchFile> search(Username requester, String terms, int limit) {
                askedLimit.set(limit);
                return List.of();
            }

            @Override
            public Optional<ResolvedFile> resolve(Username requester, String path) {
                return Optional.empty();
            }

            @Override
            public ShareIndex index() {
                return ShareIndex.empty();
            }
        });

        fixture.handler.handleMessageRead(fixture.connection.proxy, peerSearchRequest(TOKEN, "query"));

        assertEquals(Catalogs.MAXIMUM_SEARCH_MATCHES, askedLimit.get());
    }

    @Test
    void searchRequestWritesNonemptyResponseAndSuppressesEmptyOrNull() {
        List<SearchFile> matches = List.of(new SearchFile(FILENAME, 123L, FileAttributes.none()));
        Fixture fixture = new Fixture(catalog(null, null, (requester, terms) -> matches));
        fixture.client.advertisedUploadSpeed = 52_000;
        fixture.handler.handleMessageRead(fixture.connection.proxy, peerSearchRequest(TOKEN, "query"));
        assertArrayEquals(
                Catalogs.searchResponse("me", TOKEN, matches, true, 52_000, 0).toByteArray(),
                fixture.connection.bytes.getFirst(),
                "the response advertises the upload speed the transfer domain reports");

        fixture = new Fixture(catalog(null, null, (requester, terms) -> List.of()));
        fixture.handler.handleMessageRead(fixture.connection.proxy, peerSearchRequest(TOKEN, "empty"));
        assertTrue(fixture.connection.bytes.isEmpty());

        fixture = new Fixture(catalog(null, null, (requester, terms) -> List.of()));
        fixture.handler.handleMessageRead(fixture.connection.proxy, peerSearchRequest(TOKEN, "null"));
        assertTrue(fixture.connection.bytes.isEmpty(), "a search that matches nothing is not answered");
    }

    /**
     * The raw pre-encoded response is no longer something a catalog can return;
     * it survives on the response-cache path, which is where a large share's
     * encoded bytes are actually worth keeping. What a catalog returns is
     * matches, and the handler encodes them.
     */
    @Test
    void searchAnswersTheCatalogsMatchesAndACatalogFailureOnlyWarns() {
        List<SearchFile> matches = List.of(new SearchFile("shared\\hit.mp3", 42, FileAttributes.none()));
        Fixture matched = new Fixture(catalog(null, null, (requester, terms) -> matches));

        matched.handler.handleMessageRead(matched.connection.proxy, peerSearchRequest(TOKEN, "hit"));

        assertArrayEquals(
                Catalogs.searchResponse("me", TOKEN, matches, true, 0, 0).toByteArray(),
                matched.connection.bytes.getFirst(),
                "the peer is answered with our username, its token, and what the catalog matched");

        Fixture failed = new Fixture(catalog(null, null, (requester, terms) -> {
            throw new IllegalStateException("search catalog");
        }));
        failed.handler.handleMessageRead(failed.connection.proxy, peerSearchRequest(TOKEN, "failed"));
        assertTrue(failed.diagnostic.containsWarning("Error resolving search response"));
    }

    @Test
    void browseWritesTheCatalogsShareAndAnswersEmptyWhenItFails() {
        dev.slsk.share.Directory shared = new dev.slsk.share.Directory(
                "shared", List.of(new SearchFile("shared\\song.mp3", 7, FileAttributes.none())));
        Fixture resolved =
                new Fixture(catalog(requester -> dev.slsk.share.BrowseResponse.of(List.of(shared)), null, null));

        resolved.handler.handleMessageRead(resolved.connection.proxy, new BrowseRequest().toByteArray());

        assertArrayEquals(
                Catalogs.browse(dev.slsk.share.BrowseResponse.of(List.of(shared)))
                        .toByteArray(),
                resolved.connection.bytes.getFirst());

        // A catalog that throws is a bug in the application. Leaving the peer
        // on a read that never completes would make it our bug too.
        Fixture failed = new Fixture(catalog(
                requester -> {
                    throw new IllegalStateException("browse catalog");
                },
                null,
                null));
        failed.handler.handleMessageRead(failed.connection.proxy, new BrowseRequest().toByteArray());
        assertEquals(1, failed.connection.bytes.size());
        assertArrayEquals(new BrowseResponse().toByteArray(), failed.connection.bytes.getFirst());
        assertTrue(failed.diagnostic.containsWarning("The share catalog failed to answer a browse"));
    }

    @Test
    void folderRequestWritesTheCatalogsContentsAndAFailureOnlyWarns() {
        dev.slsk.share.Directory shared =
                new dev.slsk.share.Directory("shared", List.of(new SearchFile(FILENAME, 123L, FileAttributes.none())));
        Fixture resolved = new Fixture(catalog(null, (requester, path) -> List.of(shared), null));

        resolved.handler.handleMessageRead(
                resolved.connection.proxy, new FolderContentsRequest(TOKEN, "shared").toByteArray());

        FolderContentsResponse expected =
                new FolderContentsResponse(TOKEN, "shared", List.of(Catalogs.directory(shared)));
        assertArrayEquals(
                expected.toByteArray(), resolved.connection.outgoing.getFirst().toByteArray());

        Fixture failed = new Fixture(catalog(
                null,
                (requester, path) -> {
                    throw new IllegalStateException("directory catalog");
                },
                null));
        failed.handler.handleMessageRead(
                failed.connection.proxy, new FolderContentsRequest(TOKEN, "shared").toByteArray());
        assertTrue(failed.connection.outgoing.isEmpty());
        assertTrue(failed.diagnostic.containsWarning("The share catalog failed to answer a folder request"));
    }

    /**
     * Four callbacks answered parts of this and could not see each other. One
     * policy answers it now, so what is asserted is the decision reaching the
     * wire rather than four functions agreeing by luck.
     */
    @Test
    void queueDownloadSendsThePolicysAnswer() {
        // The policy decides *whether* to queue; the scheduler decides where.
        // The peer is told the place its ordering implies — here 1, the only
        // request in the queue — rather than the policy's unrelated number.
        Fixture queued = new Fixture(policy((request, context) -> new UploadPolicy.Decision.Queue(4)));
        queued.handler.handleMessageRead(queued.connection.proxy, new QueueDownloadRequest(FILENAME).toByteArray());
        assertArrayEquals(
                new PlaceInQueueResponse(FILENAME, 1).toByteArray(),
                queued.connection.outgoing.getFirst().toByteArray());

        Fixture denied = new Fixture(
                policy((request, context) -> new UploadPolicy.Decision.Deny(RejectionReason.QUEUE_FULL, "No slot")));
        denied.handler.handleMessageRead(denied.connection.proxy, new QueueDownloadRequest(FILENAME).toByteArray());
        assertArrayEquals(
                new UploadDenied(FILENAME, "No slot").toByteArray(),
                denied.connection.outgoing.getFirst().toByteArray());
    }

    @Test
    @DisplayName("a policy that throws refuses the request rather than dropping it")
    void aFailingPolicyStillAnswersThePeer() {
        Fixture failed = new Fixture(policy((request, context) -> {
            throw new IllegalStateException("policy is broken");
        }));
        failed.handler.handleMessageRead(failed.connection.proxy, new QueueDownloadRequest(FILENAME).toByteArray());

        // Silence would leave the peer waiting on a read that never completes.
        assertArrayEquals(
                new UploadDenied(FILENAME, "Upload policy failed.").toByteArray(),
                failed.connection.outgoing.getFirst().toByteArray());
    }

    /**
     * The admission guards a throwing policy itself; this is the upload
     * failing to start after the policy said yes. The C# source answers any
     * enqueue failure with a generic denial — generic because the real message
     * can carry filesystem details a stranger should not see — where silence
     * would leave the peer hanging until its own timeout.
     */
    @Test
    @DisplayName("an upload that fails to start after an Allow still answers the peer")
    void aServeThatThrowsAfterAllowStillDeniesThePeer() {
        Fixture fixture = new Fixture(policy((request, context) -> new UploadPolicy.Decision.Allow()));
        fixture.client.serveFailure = new IllegalStateException("no such file on disk: /home/me/secret");

        fixture.handler.handleMessageRead(fixture.connection.proxy, new QueueDownloadRequest(FILENAME).toByteArray());

        assertArrayEquals(
                new UploadDenied(FILENAME, "Enqueue failed due to internal error").toByteArray(),
                fixture.connection.outgoing.getFirst().toByteArray());
    }

    @Test
    void downloadTransferRequestSendsQueuedOrTwoRejectionMessages() {
        Fixture queued = new Fixture(policy((request, context) -> new UploadPolicy.Decision.Allow()));
        queued.handler.handleMessageRead(
                queued.connection.proxy,
                new TransferRequest(TransferDirection.DOWNLOAD, TOKEN, FILENAME).toByteArray());
        assertEquals(1, queued.connection.outgoing.size());
        assertArrayEquals(
                new TransferResponse(TOKEN, "Queued").toByteArray(),
                queued.connection.outgoing.getFirst().toByteArray());

        Fixture rejected = new Fixture(policy(
                (request, context) -> new UploadPolicy.Decision.Deny(RejectionReason.FILE_NOT_SHARED, "Rejected")));
        rejected.handler.handleMessageRead(
                rejected.connection.proxy,
                new TransferRequest(TransferDirection.DOWNLOAD, TOKEN, FILENAME).toByteArray());
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
        tracked.handler.handleMessageRead(
                tracked.connection.proxy, new TransferRequest(TransferDirection.UPLOAD, TOKEN, FILENAME).toByteArray());
        assertInstanceOf(
                TransferRequest.class,
                tracked.waiter.completed.get(new WaitKey(MessageCode.Peer.TRANSFER_REQUEST, USERNAME, FILENAME)));
        assertTrue(tracked.connection.outgoing.isEmpty());

        Fixture unknown = new Fixture(new SoulseekClientOptions());
        unknown.handler.handleMessageRead(
                unknown.connection.proxy, new TransferRequest(TransferDirection.UPLOAD, TOKEN, FILENAME).toByteArray());
        assertArrayEquals(
                new TransferResponse(TOKEN, "Cancelled").toByteArray(),
                unknown.connection.outgoing.getFirst().toByteArray());
        // Even to refuse it, the queue has to be asked: "not a live transfer"
        // is not the same question as "not wanted".
        assertEquals(List.of(USERNAME + " " + FILENAME), unknown.client.offered);
    }

    /**
     * A peer offering a file reaches our name in its queue after a wait that can
     * run to hours, and by then the download is usually still in ours. Refusing
     * it spends that wait for nothing and puts us at the back, so the queue is
     * consulted before anything is refused.
     */
    @Test
    @DisplayName("an offer for a queued download is taken up, silently")
    void offeredDownloadsAreTakenUpRatherThanCancelled() {
        Fixture taken = new Fixture(new SoulseekClientOptions());
        taken.client.disposition = PeerServices.OfferDisposition.TAKEN;
        taken.handler.handleMessageRead(
                taken.connection.proxy, new TransferRequest(TransferDirection.UPLOAD, TOKEN, FILENAME).toByteArray());

        // No reply from here. The download writes the acceptance once it has a
        // peer connection, which is the same message the tracked path sends —
        // sending one here too would accept the transfer twice.
        assertTrue(taken.connection.outgoing.isEmpty());
        assertEquals(List.of(USERNAME + " " + FILENAME), taken.client.offered);
    }

    /**
     * The peer uses the difference to decide whether to keep holding the file
     * for us; "Cancelled" invites it to try again, "Complete" does not.
     */
    @Test
    @DisplayName("an offer for something already downloaded is refused as Complete")
    void offersForFinishedDownloadsAreRefusedAsComplete() {
        Fixture done = new Fixture(new SoulseekClientOptions());
        done.client.disposition = PeerServices.OfferDisposition.COMPLETE;
        done.handler.handleMessageRead(
                done.connection.proxy, new TransferRequest(TransferDirection.UPLOAD, TOKEN, FILENAME).toByteArray());
        assertArrayEquals(
                new TransferResponse(TOKEN, "Complete").toByteArray(),
                done.connection.outgoing.getFirst().toByteArray());
    }

    /**
     * The queue a place refers to is the one the policy put them in, so there is
     * nothing to resolve. A peer asking about a file we are not holding for them
     * gets no answer, which is what that means.
     */
    @Test
    void placeRequestAnswersOnlyForSomeoneActuallyQueued() {
        Fixture placed = new Fixture(policy((request, context) -> new UploadPolicy.Decision.Queue(9)));
        placed.handler.handleMessageRead(placed.connection.proxy, new QueueDownloadRequest(FILENAME).toByteArray());
        placed.connection.outgoing.clear();
        placed.handler.handleMessageRead(placed.connection.proxy, new PlaceInQueueRequest(FILENAME).toByteArray());
        // One queued request, so the honest answer is 1 whatever the policy said.
        assertArrayEquals(
                new PlaceInQueueResponse(FILENAME, 1).toByteArray(),
                placed.connection.outgoing.getFirst().toByteArray());

        Fixture absent = new Fixture(policy((request, context) -> new UploadPolicy.Decision.Allow()));
        absent.handler.handleMessageRead(absent.connection.proxy, new PlaceInQueueRequest(FILENAME).toByteArray());
        assertTrue(absent.connection.outgoing.isEmpty());
    }

    @Test
    void deniedAndFailedMessagesFailWaitsAndRaiseEvents() {
        Fixture fixture = new Fixture(new SoulseekClientOptions());
        List<DownloadDeniedEvent> deniedEvents = new ArrayList<>();
        List<DownloadFailedEvent> failedEvents = new ArrayList<>();
        fixture.handler.addDownloadDeniedListener(eventData -> deniedEvents.add(eventData));
        fixture.handler.addDownloadFailedListener(eventData -> failedEvents.add(eventData));

        fixture.handler.handleMessageRead(
                fixture.connection.proxy, new UploadDenied(FILENAME, "No slot").toByteArray());
        fixture.handler.handleMessageRead(fixture.connection.proxy, new UploadFailed(FILENAME).toByteArray());

        WaitKey key = new WaitKey(MessageCode.Peer.TRANSFER_REQUEST, USERNAME, FILENAME);
        assertInstanceOf(TransferReportedFailedException.class, fixture.waiter.failures.get(key));
        assertEquals("No slot", deniedEvents.getFirst().message());
        assertEquals(FILENAME, deniedEvents.getFirst().filename());
        assertEquals(USERNAME, deniedEvents.getFirst().username());
        assertEquals(FILENAME, failedEvents.getFirst().filename());

        Fixture deniedOnly = new Fixture(new SoulseekClientOptions());
        deniedOnly.handler.handleMessageRead(
                deniedOnly.connection.proxy, new UploadDenied(FILENAME, "No slot").toByteArray());
        assertInstanceOf(TransferRejectedException.class, deniedOnly.waiter.failures.get(key));
    }

    @Test
    void receiptAndWrittenCallbacksCorrelateBrowseAndLogCodes() {
        Fixture fixture = new Fixture(new SoulseekClientOptions());
        byte[] response = new BrowseResponse(List.of()).toByteArray();
        MessageReceivedEvent received =
                new MessageReceivedEvent(fixture.connection.proxy, response.length, Arrays.copyOfRange(response, 4, 8));

        fixture.handler.handleMessageReceived(received);
        Object result =
                fixture.waiter.completed.get(new WaitKey(Constants.WaitKey.BROWSE_RESPONSE_CONNECTION, USERNAME));
        BrowseResponseConnection browse = assertInstanceOf(BrowseResponseConnection.class, result);
        assertSame(received, browse.eventData());
        assertSame(fixture.connection.proxy, browse.connection());

        fixture.handler.handleMessageReceived(new MessageReceivedEvent(
                fixture.connection.proxy, 8, Arrays.copyOfRange(new BrowseRequest().toByteArray(), 4, 8)));
        fixture.handler.handleMessageWritten(
                new MessageEvent(fixture.connection.proxy, new BrowseRequest().toByteArray()));
        assertTrue(fixture.diagnostic.contains("Peer message sent: BROWSE_REQUEST"));
    }

    /**
     * A catalog answering only what a test needs, and empty for the rest. The
     * pieces used to be four separately-configured resolvers; the point of the
     * SPI is that they are one object now, so the fixture builds one.
     */
    /** A fixture whose policy is the one supplied. */
    private static UploadPolicy policy(UploadPolicy value) {
        return value;
    }

    private static ShareCatalog catalog(
            Function<Username, dev.slsk.share.BrowseResponse> browse,
            BiFunction<Username, String, List<dev.slsk.share.Directory>> directory,
            BiFunction<Username, String, List<SearchFile>> search) {
        return new ShareCatalog() {
            @Override
            public dev.slsk.share.BrowseResponse browse(Username requester) {
                return browse == null ? dev.slsk.share.BrowseResponse.empty() : browse.apply(requester);
            }

            @Override
            public List<dev.slsk.share.Directory> directory(Username requester, String path) {
                return directory == null ? List.of() : directory.apply(requester, path);
            }

            @Override
            public List<SearchFile> search(Username requester, String terms, int limit) {
                return search == null ? List.of() : search.apply(requester, terms);
            }

            @Override
            public Optional<ResolvedFile> resolve(Username requester, String path) {
                return Optional.empty();
            }

            @Override
            public ShareIndex index() {
                return ShareIndex.empty();
            }
        };
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
            this(options, ShareCatalog.empty());
        }

        private Fixture(ShareCatalog catalog) {
            this(new SoulseekClientOptions(), catalog, UserProfile.empty());
        }

        private Fixture(UploadPolicy policy) {
            this(new SoulseekClientOptions(), ShareCatalog.empty(), UserProfile.empty(), policy);
        }

        private Fixture(UserProfile profile) {
            this(new SoulseekClientOptions(), ShareCatalog.empty(), profile);
        }

        private Fixture(SoulseekClientOptions options, ShareCatalog catalog) {
            this(options, catalog, UserProfile.empty());
        }

        private Fixture(SoulseekClientOptions options, ShareCatalog catalog, UserProfile profile) {
            this(options, catalog, profile, UploadPolicy.standard(2, 1));
        }

        private Fixture(SoulseekClientOptions options, ShareCatalog catalog, UserProfile profile, UploadPolicy policy) {
            client = new FakeClient(options, waiter, catalog, profile, policy);
            // Answers run on the calling thread here. In production they run
            // off the read loop, which is the point of the seam; a test that
            // asserts on an answer should not have to wait for one.
            handler = new DefaultPeerMessageHandler(
                    () -> client.options,
                    client.waiter,
                    () -> client.searches,
                    () -> client.downloads,
                    () -> "me",
                    client,
                    diagnostic,
                    Runnable::run);
        }
    }

    private static final class FakeClient implements PeerServices {
        private final SoulseekClientOptions options;
        private final Waiter waiter;
        private final ShareCatalog catalog;
        private final UserProfile profile;
        private final UploadPolicy policy;

        /**
         * A real admission over this fake client. It is the thing under test as
         * much as the handler is: bans, the queue a place refers to, and the
         * guard around a policy that throws all live there.
         */
        /** Where a misbehaving policy is reported; asserted on by the tests. */
        private final RecordingDiagnostic admissionDiagnostic = new RecordingDiagnostic();

        private final java.util.concurrent.atomic.AtomicInteger nextToken =
                new java.util.concurrent.atomic.AtomicInteger(900);

        private final dev.slsk.internal.UploadAdmission admission = new dev.slsk.internal.UploadAdmission(
                this::uploadPolicy, Map::of, username -> false, nextToken::getAndIncrement, admissionDiagnostic);
        private final Map<Integer, SearchInternal> searches = new HashMap<>();
        private final Map<Integer, TransferInternal> downloads = new HashMap<>();

        private FakeClient(
                SoulseekClientOptions options,
                Waiter waiter,
                ShareCatalog catalog,
                UserProfile profile,
                UploadPolicy policy) {
            this.options = options;
            this.waiter = waiter;
            this.catalog = catalog;
            this.profile = profile;
            this.policy = policy;
        }

        /** What the handler asked us to serve; asserted on by the tests. */
        private final List<String> served = new java.util.ArrayList<>();

        /** A failure to inject into {@link #serve}. */
        private RuntimeException serveFailure;

        @Override
        public void serve(Username user, String path) {
            if (serveFailure != null) {
                throw serveFailure;
            }
            served.add(user.value() + " " + path);
        }

        /** Places in queue the handler passed on, whether asked for or not. */
        private final java.util.List<String> positions = new java.util.ArrayList<>();

        @Override
        public void queuePosition(String username, String filename, int position) {
            positions.add(username + " " + filename + " " + position);
        }

        /** What the download queue would make of an offer. */
        private OfferDisposition disposition = OfferDisposition.UNKNOWN;

        /** Offers the handler passed on rather than answering itself. */
        private final java.util.List<String> offered = new java.util.ArrayList<>();

        @Override
        public OfferDisposition offered(
                String username, String filename, dev.slsk.internal.messaging.messages.TransferRequest offer) {
            offered.add(username + " " + filename);
            return disposition;
        }

        @Override
        public UploadPolicy uploadPolicy() {
            return policy;
        }

        @Override
        public dev.slsk.internal.UploadAdmission admission() {
            return admission;
        }

        /** What the search-response tests assert we advertised. */
        private int advertisedUploadSpeed;

        @Override
        public int advertisedUploadSpeed() {
            return advertisedUploadSpeed;
        }

        @Override
        public UserProfile profile() {
            return profile;
        }

        @Override
        public ShareCatalog catalog() {
            return catalog;
        }
    }

    private static final class RecordingWaiter implements Waiter {
        private final Map<WaitKey, Object> completed = new HashMap<>();
        private final Map<WaitKey, Throwable> failures = new HashMap<>();

        @Override
        public Duration getDefaultTimeout() {
            return Duration.ofSeconds(5);
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
        public <T> Wait<T> register(
                WaitKey key, Class<T> resultType, Duration timeout, CancellationSignal cancellationSignal) {
            // These tests drive completions, never waits.
            return () -> null;
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
                case "write" -> {
                    if (arguments[0] instanceof byte[] value) {
                        bytes.add(Arrays.copyOf(value, value.length));
                    } else if (arguments[0] instanceof OutgoingMessage message) {
                        outgoing.add(message);
                    } else if (arguments[0] instanceof Long length) {
                        rawLength = length;
                        rawStream = (InputStream) arguments[1];
                    }
                    yield null;
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
