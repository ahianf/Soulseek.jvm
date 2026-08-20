// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.exceptions.RoomJoinForbiddenException;
import dev.slsk.internal.ServerLink;
import dev.slsk.internal.ServerLinks;
import dev.slsk.internal.common.Constants;
import dev.slsk.internal.common.Eventually;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.connection.ServerInfo;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.events.PrivateMessageReceivedEvent;
import dev.slsk.internal.events.PrivilegeNotificationReceivedEvent;
import dev.slsk.internal.events.PublicChatMessageReceivedEvent;
import dev.slsk.internal.events.RoomJoinedEvent;
import dev.slsk.internal.events.RoomLeftEvent;
import dev.slsk.internal.events.RoomMessageReceivedEvent;
import dev.slsk.internal.events.RoomTickerAddedEvent;
import dev.slsk.internal.events.RoomTickerListReceivedEvent;
import dev.slsk.internal.events.RoomTickerRemovedEvent;
import dev.slsk.internal.events.UserCannotConnectEvent;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.messages.AcknowledgePrivateMessageCommand;
import dev.slsk.internal.messaging.messages.AcknowledgePrivilegeNotificationCommand;
import dev.slsk.internal.messaging.messages.ConnectToPeerResponse;
import dev.slsk.internal.messaging.messages.LoginResponse;
import dev.slsk.internal.messaging.messages.NewPassword;
import dev.slsk.internal.messaging.messages.OutgoingMessage;
import dev.slsk.internal.messaging.messages.PrivateRoomToggle;
import dev.slsk.internal.messaging.messages.UserAddressResponse;
import dev.slsk.internal.messaging.messages.WatchUserResponse;
import dev.slsk.internal.network.DistributedConnectionManager;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.MessageEvent;
import dev.slsk.internal.network.PeerConnectionManager;
import dev.slsk.internal.network.PeerEndpoint;
import dev.slsk.internal.network.TransferConnectionResult;
import dev.slsk.internal.network.tcp.Connection;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.internal.options.SoulseekClientOptionsPatch;
import dev.slsk.internal.room.RoomData;
import dev.slsk.internal.room.RoomInfo;
import dev.slsk.internal.room.RoomList;
import dev.slsk.internal.search.SearchInternal;
import dev.slsk.internal.search.SearchQuery;
import dev.slsk.internal.search.SearchResponder;
import dev.slsk.internal.search.SearchScope;
import dev.slsk.internal.transfer.TransferDirection;
import dev.slsk.internal.transfer.TransferInternal;
import dev.slsk.internal.user.UserPresence;
import dev.slsk.internal.user.UserStatistics;
import dev.slsk.internal.user.UserStatus;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ServerMessageHandlerTest {
    private static final String USERNAME = "peer";
    private static final String LOCAL_USER = "local";
    private static final String ROOM = "room";
    private static final int TOKEN = 0x10203040;
    private static final InetSocketAddress ENDPOINT = endpoint(45001);

    @Test
    void constructionRequiresClient() {
        Fixture fixture = new Fixture(options(true, true));
        assertThrows(
                NullPointerException.class,
                () -> new DefaultServerMessageHandler(
                        null,
                        fixture.client.server,
                        fixture.waiter,
                        () -> fixture.client.searches,
                        () -> fixture.client.downloads,
                        () -> fixture.client.peer,
                        () -> fixture.client.distributed,
                        fixture.client::distributedMessages,
                        () -> fixture.client.responder));
    }

    @Test
    void diagnosticsCoverReadUnhandledFailureAndWrite() {
        Fixture fixture = new Fixture(options(false, false));
        fixture.handler.handleMessageRead(
                null,
                new MessageBuilder()
                        .writeCode(MessageCode.Server.ASK_PUBLIC_CHAT)
                        .build());
        assertTrue(fixture.diagnostic.contains("Server message received: ASK_PUBLIC_CHAT"));
        assertTrue(fixture.diagnostic.contains("Unhandled server message"));

        fixture.handler.handleMessageRead(
                null,
                new MessageBuilder().writeCode(MessageCode.Server.ROOM_LIST).build());
        assertTrue(fixture.diagnostic.containsWarning("Error handling server message"));

        fixture.handler.handleMessageWritten(new MessageEvent(null, searchRequest(USERNAME, TOKEN, "query")));
        assertTrue(fixture.diagnostic.contains("Server message sent: FILE_SEARCH"));

        AtomicInteger generated = new AtomicInteger();
        DefaultServerMessageHandler defaultDiagnostic = new DefaultServerMessageHandler(
                () -> fixture.client.options,
                fixture.client.server,
                fixture.client.waiter,
                () -> fixture.client.searches,
                () -> fixture.client.downloads,
                () -> fixture.client.peer,
                () -> fixture.client.distributed,
                fixture.client::distributedMessages,
                () -> fixture.client.responder);
        defaultDiagnostic.subscribe(eventData -> generated.incrementAndGet());
        defaultDiagnostic.handleMessageRead(
                null,
                new MessageBuilder()
                        .writeCode(MessageCode.Server.DISTRIBUTED_RESET)
                        .build());
        assertEquals(1, generated.get());
    }

    @Test
    void scalarServerInfoAndBasicWaitsUseSourceKeys() {
        Fixture fixture = new Fixture(options(false, false));
        List<ServerInfo> info = new ArrayList<>();
        fixture.handler.<ServerInfo>subscribe(ServerMessageEvent.SERVER_INFO_RECEIVED, value -> info.add(value));

        fixture.handle(integer(MessageCode.Server.PARENT_MIN_SPEED, 11));
        fixture.handle(integer(MessageCode.Server.PARENT_SPEED_RATIO, 22));
        fixture.handle(integer(MessageCode.Server.WISHLIST_INTERVAL, 33));
        fixture.handle(integer(MessageCode.Server.CHECK_PRIVILEGES, 44));
        fixture.handle(new MessageBuilder().writeCode(MessageCode.Server.PING).build());
        fixture.handle(new NewPassword("secret").toByteArray());
        fixture.handle(new PrivateRoomToggle(true).toByteArray());
        fixture.handle(new MessageBuilder()
                .writeCode(MessageCode.Server.USER_PRIVILEGES)
                .writeString(USERNAME)
                .writeByte(1)
                .build());

        assertEquals(11, info.get(0).parentMinSpeed());
        assertEquals(22, info.get(1).parentSpeedRatio());
        assertEquals(33, info.get(2).wishlistInterval());
        assertEquals(44, fixture.waiter.completed.get(new WaitKey(MessageCode.Server.CHECK_PRIVILEGES)));
        assertTrue(fixture.waiter.completed.containsKey(new WaitKey(MessageCode.Server.PING)));
        assertEquals("secret", fixture.waiter.completed.get(new WaitKey(MessageCode.Server.NEW_PASSWORD)));
        assertEquals(true, fixture.waiter.completed.get(new WaitKey(MessageCode.Server.PRIVATE_ROOM_TOGGLE)));
        assertEquals(true, fixture.waiter.completed.get(new WaitKey(MessageCode.Server.USER_PRIVILEGES, USERNAME)));
    }

    @Test
    void loginAddressWatchStatusAndStatisticsCompleteAndRaise() {
        Fixture fixture = new Fixture(options(false, false));
        AtomicReference<UserStatus> statusEvent = new AtomicReference<>();
        AtomicReference<UserStatistics> statisticsEvent = new AtomicReference<>();
        fixture.handler.<UserStatus>subscribe(ServerMessageEvent.USER_STATUS_CHANGED, value -> statusEvent.set(value));
        fixture.handler.<UserStatistics>subscribe(
                ServerMessageEvent.USER_STATISTICS_CHANGED, value -> statisticsEvent.set(value));

        byte[] ip = new byte[] {1, 0, 0, 127};
        fixture.handle(new MessageBuilder()
                .writeCode(MessageCode.Server.LOGIN)
                .writeByte(1)
                .writeString("ok")
                .writeBytes(ip)
                .writeString("hash")
                .writeByte(1)
                .build());
        fixture.handle(new MessageBuilder()
                .writeCode(MessageCode.Server.GET_PEER_ADDRESS)
                .writeString(USERNAME)
                .writeBytes(ip)
                .writeInteger(2234)
                .build());
        fixture.handle(new MessageBuilder()
                .writeCode(MessageCode.Server.WATCH_USER)
                .writeString(USERNAME)
                .writeByte(1)
                .writeInteger(UserPresence.ONLINE.getValue())
                .writeInteger(100)
                .writeLong(200L)
                .writeInteger(300)
                .writeInteger(400)
                .writeString("CL")
                .build());
        fixture.handle(status(USERNAME, UserPresence.AWAY, true));
        fixture.handle(statistics(USERNAME, 100, 200L, 300, 400));

        assertInstanceOf(LoginResponse.class, fixture.waiter.completed.get(new WaitKey(MessageCode.Server.LOGIN)));
        UserAddressResponse address = assertInstanceOf(
                UserAddressResponse.class,
                fixture.waiter.completed.get(new WaitKey(MessageCode.Server.GET_PEER_ADDRESS, USERNAME)));
        assertEquals(2234, address.getPort());
        assertInstanceOf(
                WatchUserResponse.class,
                fixture.waiter.completed.get(new WaitKey(MessageCode.Server.WATCH_USER, USERNAME)));
        assertSame(
                statusEvent.get(), fixture.waiter.completed.get(new WaitKey(MessageCode.Server.GET_STATUS, USERNAME)));
        assertSame(
                statisticsEvent.get(),
                fixture.waiter.completed.get(new WaitKey(MessageCode.Server.GET_USER_STATS, USERNAME)));
    }

    @Test
    void roomJoinLeaveAndRejectionPreserveCorrelationAndEvents() {
        Fixture fixture = new Fixture(options(false, false));
        AtomicReference<RoomLeftEvent> left = new AtomicReference<>();
        fixture.handler.<RoomLeftEvent>subscribe(ServerMessageEvent.ROOM_LEFT, value -> left.set(value));

        fixture.handle(new MessageBuilder()
                .writeCode(MessageCode.Server.JOIN_ROOM)
                .writeString(ROOM)
                .writeInteger(0)
                .writeInteger(0)
                .writeInteger(0)
                .writeInteger(0)
                .writeInteger(0)
                .build());
        fixture.handle(string(MessageCode.Server.LEAVE_ROOM, ROOM));
        fixture.handle(string(MessageCode.Server.CANNOT_JOIN_ROOM, "forbidden"));

        RoomData joined = assertInstanceOf(
                RoomData.class, fixture.waiter.completed.get(new WaitKey(MessageCode.Server.JOIN_ROOM, ROOM)));
        assertEquals(ROOM, joined.name());
        assertTrue(fixture.waiter.completed.containsKey(new WaitKey(MessageCode.Server.LEAVE_ROOM, ROOM)));
        assertEquals(LOCAL_USER, left.get().username());
        assertEquals(ROOM, left.get().roomName());
        assertInstanceOf(
                RoomJoinForbiddenException.class,
                fixture.waiter.failures.get(new WaitKey(MessageCode.Server.JOIN_ROOM, "forbidden")));
    }

    @Test
    void roomListCompletesWaitAndRaisesSameSnapshot() {
        Fixture fixture = new Fixture(options(false, false));
        AtomicReference<RoomList> event = new AtomicReference<>();
        fixture.handler.<RoomList>subscribe(ServerMessageEvent.ROOM_LIST_RECEIVED, value -> event.set(value));

        fixture.handle(roomList());

        Object waited = fixture.waiter.completed.get(new WaitKey(MessageCode.Server.ROOM_LIST));
        assertSame(event.get(), waited);
        assertEquals("public", event.get().publicRooms().getFirst().name());
        assertEquals("private", event.get().privateRooms().getFirst().name());
        assertEquals("owned", event.get().ownedRooms().getFirst().name());
        assertEquals("moderated", event.get().moderatedRoomNames().getFirst());
    }

    @Test
    void chatRoomAndTickerNotificationsRaiseTypedPayloads() {
        Fixture fixture = new Fixture(options(false, false));
        AtomicReference<RoomMessageReceivedEvent> roomMessage =
                listen(fixture, ServerMessageEvent.ROOM_MESSAGE_RECEIVED);
        AtomicReference<PublicChatMessageReceivedEvent> publicMessage =
                listen(fixture, ServerMessageEvent.PUBLIC_CHAT_MESSAGE_RECEIVED);
        AtomicReference<RoomJoinedEvent> joined = listen(fixture, ServerMessageEvent.ROOM_JOINED);
        AtomicReference<RoomLeftEvent> left = listen(fixture, ServerMessageEvent.ROOM_LEFT);
        AtomicReference<RoomTickerListReceivedEvent> tickers =
                listen(fixture, ServerMessageEvent.ROOM_TICKER_LIST_RECEIVED);
        AtomicReference<RoomTickerAddedEvent> tickerAdded = listen(fixture, ServerMessageEvent.ROOM_TICKER_ADDED);
        AtomicReference<RoomTickerRemovedEvent> tickerRemoved = listen(fixture, ServerMessageEvent.ROOM_TICKER_REMOVED);

        fixture.handle(chat(MessageCode.Server.SAY_IN_CHAT_ROOM, ROOM, USERNAME, "hello"));
        fixture.handle(chat(MessageCode.Server.PUBLIC_CHAT, ROOM, USERNAME, "public"));
        fixture.handle(userJoinedRoom());
        fixture.handle(new MessageBuilder()
                .writeCode(MessageCode.Server.USER_LEFT_ROOM)
                .writeString(ROOM)
                .writeString(USERNAME)
                .build());
        fixture.handle(new MessageBuilder()
                .writeCode(MessageCode.Server.ROOM_TICKERS)
                .writeString(ROOM)
                .writeInteger(1)
                .writeString(USERNAME)
                .writeString("ticker")
                .build());
        fixture.handle(new MessageBuilder()
                .writeCode(MessageCode.Server.ROOM_TICKER_ADD)
                .writeString(ROOM)
                .writeString(USERNAME)
                .writeString("added")
                .build());
        fixture.handle(new MessageBuilder()
                .writeCode(MessageCode.Server.ROOM_TICKER_REMOVE)
                .writeString(ROOM)
                .writeString(USERNAME)
                .build());

        assertEquals("hello", roomMessage.get().message());
        assertEquals("public", publicMessage.get().message());
        assertEquals(USERNAME, joined.get().username());
        assertEquals(USERNAME, left.get().username());
        assertEquals(1, tickers.get().tickerCount());
        assertEquals("added", tickerAdded.get().ticker().message());
        assertEquals(USERNAME, tickerRemoved.get().username());
    }

    @Test
    void privateAndPrivilegeNotificationsHonorAcknowledgementOptions() {
        Fixture fixture = new Fixture(options(true, true));
        AtomicReference<PrivateMessageReceivedEvent> privateMessage =
                listen(fixture, ServerMessageEvent.PRIVATE_MESSAGE_RECEIVED);
        List<PrivilegeNotificationReceivedEvent> privileges = new ArrayList<>();
        fixture.handler.<PrivilegeNotificationReceivedEvent>subscribe(
                ServerMessageEvent.PRIVILEGE_NOTIFICATION_RECEIVED, value -> privileges.add(value));

        fixture.handle(new MessageBuilder()
                .writeCode(MessageCode.Server.PRIVATE_MESSAGE)
                .writeInteger(12)
                .writeInteger(60)
                .writeString(USERNAME)
                .writeString("private")
                .writeByte(1)
                .build());
        fixture.handle(string(MessageCode.Server.ADD_PRIVILEGED_USER, "member"));
        fixture.handle(new MessageBuilder()
                .writeCode(MessageCode.Server.NOTIFY_PRIVILEGES)
                .writeInteger(13)
                .writeString("supporter")
                .build());

        assertEquals(12, privateMessage.get().id());
        // Both acknowledgements write back to the server, so both go to a
        // thread of their own rather than answering on the server's read loop.
        assertTrue(Eventually.holds(() -> fixture.client
                .acknowledged(AcknowledgePrivateMessageCommand.class)
                .equals(List.of(12))));
        assertNull(privileges.get(0).id());
        assertEquals(13, privileges.get(1).id());
        assertTrue(Eventually.holds(() -> fixture.client
                .acknowledged(AcknowledgePrivilegeNotificationCommand.class)
                .equals(List.of(13))));

        Fixture disabled = new Fixture(options(false, false));
        disabled.handle(new MessageBuilder()
                .writeCode(MessageCode.Server.PRIVATE_MESSAGE)
                .writeInteger(14)
                .writeInteger(60)
                .writeString(USERNAME)
                .writeString("private")
                .writeByte(1)
                .build());
        disabled.handle(new MessageBuilder()
                .writeCode(MessageCode.Server.NOTIFY_PRIVILEGES)
                .writeInteger(15)
                .writeString(USERNAME)
                .build());
        assertTrue(disabled.client.writes.isEmpty(), "nothing is acknowledged when both are disabled");
    }

    @Test
    void listGlobalAndKickNotificationsRaiseSourceEvents() {
        Fixture fixture = new Fixture(options(false, false));
        AtomicReference<List<String>> privileged = listen(fixture, ServerMessageEvent.PRIVILEGED_USER_LIST_RECEIVED);
        AtomicReference<List<String>> excluded = listen(fixture, ServerMessageEvent.EXCLUDED_SEARCH_PHRASES_RECEIVED);
        AtomicReference<String> global = listen(fixture, ServerMessageEvent.GLOBAL_MESSAGE_RECEIVED);
        AtomicInteger kicked = new AtomicInteger();
        fixture.handler.<Void>subscribe(ServerMessageEvent.KICKED_FROM_SERVER, value -> kicked.incrementAndGet());

        fixture.handle(stringList(MessageCode.Server.PRIVILEGED_USERS, "one", "two"));
        fixture.handle(stringList(MessageCode.Server.EXCLUDED_SEARCH_PHRASES, "bad", "phrase"));
        fixture.handle(string(MessageCode.Server.GLOBAL_ADMIN_MESSAGE, "global"));
        fixture.handle(new MessageBuilder()
                .writeCode(MessageCode.Server.KICKED_FROM_SERVER)
                .build());

        assertEquals(List.of("one", "two"), privileged.get());
        assertEquals(List.of("bad", "phrase"), excluded.get());
        assertEquals("global", global.get());
        assertEquals(1, kicked.get());
    }

    @Test
    void privateRoomEventsAndOperationWaitsCoverEveryCommand() {
        Fixture fixture = new Fixture(options(false, false));
        AtomicReference<String> membershipAdded = listen(fixture, ServerMessageEvent.PRIVATE_ROOM_MEMBERSHIP_ADDED);
        AtomicReference<String> membershipRemoved = listen(fixture, ServerMessageEvent.PRIVATE_ROOM_MEMBERSHIP_REMOVED);
        AtomicReference<String> moderationAdded = listen(fixture, ServerMessageEvent.PRIVATE_ROOM_MODERATION_ADDED);
        AtomicReference<String> moderationRemoved = listen(fixture, ServerMessageEvent.PRIVATE_ROOM_MODERATION_REMOVED);
        AtomicReference<RoomInfo> users = listen(fixture, ServerMessageEvent.PRIVATE_ROOM_USER_LIST_RECEIVED);
        AtomicReference<RoomInfo> moderated =
                listen(fixture, ServerMessageEvent.PRIVATE_ROOM_MODERATED_USER_LIST_RECEIVED);

        fixture.handle(string(MessageCode.Server.PRIVATE_ROOM_ADDED, ROOM));
        fixture.handle(string(MessageCode.Server.PRIVATE_ROOM_REMOVED, ROOM));
        fixture.handle(string(MessageCode.Server.PRIVATE_ROOM_OPERATOR_ADDED, ROOM));
        fixture.handle(string(MessageCode.Server.PRIVATE_ROOM_OPERATOR_REMOVED, ROOM));
        fixture.handle(roomUsers(MessageCode.Server.PRIVATE_ROOM_USERS));
        fixture.handle(roomUsers(MessageCode.Server.PRIVATE_ROOM_OWNED));
        fixture.handle(roomOperation(MessageCode.Server.PRIVATE_ROOM_ADD_USER));
        fixture.handle(roomOperation(MessageCode.Server.PRIVATE_ROOM_REMOVE_USER));
        fixture.handle(roomOperation(MessageCode.Server.PRIVATE_ROOM_ADD_OPERATOR));
        fixture.handle(roomOperation(MessageCode.Server.PRIVATE_ROOM_REMOVE_OPERATOR));

        assertEquals(ROOM, membershipAdded.get());
        assertEquals(ROOM, membershipRemoved.get());
        assertEquals(ROOM, moderationAdded.get());
        assertEquals(ROOM, moderationRemoved.get());
        assertEquals(List.of(USERNAME), users.get().users());
        assertEquals(List.of(USERNAME), moderated.get().users());
        assertTrue(fixture.waiter.completed.containsKey(new WaitKey(MessageCode.Server.PRIVATE_ROOM_REMOVED, ROOM)));
        assertTrue(fixture.waiter.completed.containsKey(
                new WaitKey(MessageCode.Server.PRIVATE_ROOM_OPERATOR_REMOVED, ROOM)));
        for (MessageCode.Server code : List.of(
                MessageCode.Server.PRIVATE_ROOM_ADD_USER,
                MessageCode.Server.PRIVATE_ROOM_REMOVE_USER,
                MessageCode.Server.PRIVATE_ROOM_ADD_OPERATOR,
                MessageCode.Server.PRIVATE_ROOM_REMOVE_OPERATOR)) {
            assertTrue(fixture.waiter.completed.containsKey(new WaitKey(code, ROOM, USERNAME)));
        }
    }

    @Test
    void distributedResetAndNetInfoDelegateAndContainFailures() {
        Fixture fixture = new Fixture(options(false, false));
        AtomicInteger resets = new AtomicInteger();
        fixture.handler.<Void>subscribe(
                ServerMessageEvent.DISTRIBUTED_NETWORK_RESET, value -> resets.incrementAndGet());
        fixture.handle(netInfo());
        assertTrue(Eventually.holds(() -> !fixture.distributed.parents.isEmpty()));
        fixture.handle(new MessageBuilder()
                .writeCode(MessageCode.Server.DISTRIBUTED_RESET)
                .build());

        assertEquals(new PeerEndpoint(USERNAME, ENDPOINT), fixture.distributed.parents.getFirst());
        assertEquals(1, fixture.distributed.removed);
        assertEquals(1, fixture.distributed.resets);
        assertEquals(1, resets.get());

        fixture.distributed.addParent = new RuntimeException("parent failure");
        fixture.handle(netInfo());
        assertTrue(
                Eventually.holds(() -> fixture.diagnostic.contains("Error handling NetInfo message: parent failure")));
    }

    @Test
    void cannotConnectDiscardsAndRaisesOnlyWhenUsernameExists() {
        Fixture fixture = new Fixture(options(false, false));
        List<UserCannotConnectEvent> events = new ArrayList<>();
        fixture.handler.<UserCannotConnectEvent>subscribe(
                ServerMessageEvent.USER_CANNOT_CONNECT, value -> events.add(value));

        fixture.handle(new MessageBuilder()
                .writeCode(MessageCode.Server.CANNOT_CONNECT)
                .writeInteger(TOKEN)
                .writeString(USERNAME)
                .build());
        fixture.handle(new MessageBuilder()
                .writeCode(MessageCode.Server.CANNOT_CONNECT)
                .writeInteger(TOKEN + 1)
                .build());

        assertEquals(List.of(TOKEN, TOKEN + 1), fixture.responder.discards);
        assertEquals(1, events.size());
        assertEquals(USERNAME, events.getFirst().username());
    }

    @Test
    void connectToPeerDelegatesPeerAndDistributedTypes() {
        Fixture fixture = new Fixture(options(false, false));
        fixture.handle(connectToPeer(USERNAME, Constants.ConnectionType.PEER, TOKEN));
        fixture.handle(connectToPeer(USERNAME, Constants.ConnectionType.DISTRIBUTED, TOKEN + 1));

        assertTrue(Eventually.holds(() -> fixture.peer.messageRequests.size() == 1));
        assertTrue(Eventually.holds(() -> fixture.distributed.childRequests.size() == 1));
        assertEquals(USERNAME, fixture.peer.messageRequests.getFirst().getUsername());
        assertEquals(1, fixture.distributed.childRequests.size());
        assertEquals(TOKEN + 1, fixture.distributed.childRequests.getFirst().getToken());
    }

    @Test
    void expectedTransferCorrelatesAndMismatchedTransferDisconnects() {
        Fixture fixture = new Fixture(options(false, false));
        TransferInternal transfer = new TransferInternal(TransferDirection.DOWNLOAD, USERNAME, "file", TOKEN);
        transfer.setRemoteToken(91);
        fixture.client.downloads.put(TOKEN, transfer);
        ConnectionProbe connection = new ConnectionProbe();
        fixture.peer.transferResult = new TransferConnectionResult(connection.proxy, 91);

        fixture.handle(connectToPeer(USERNAME, Constants.ConnectionType.TRANSFER, TOKEN));

        WaitKey correlated = new WaitKey(Constants.WaitKey.INDIRECT_TRANSFER, USERNAME, "file", 91);
        assertTrue(Eventually.holds(() -> fixture.waiter.completed.containsKey(correlated)));
        assertSame(connection.proxy, fixture.waiter.completed.get(correlated));

        fixture.peer.transferResult = new TransferConnectionResult(connection.proxy, 92);
        fixture.handle(connectToPeer(USERNAME, Constants.ConnectionType.TRANSFER, TOKEN + 1));
        assertTrue(Eventually.holds(() -> "Unknown transfer".equals(connection.disconnectMessage)));

        Fixture unexpected = new Fixture(options(false, false));
        unexpected.handle(connectToPeer(USERNAME, Constants.ConnectionType.TRANSFER, TOKEN));
        unexpected.handle(connectToPeer(USERNAME, "X", TOKEN));
        assertTrue(unexpected.diagnostic.contains("Unexpected transfer request"));
        assertTrue(unexpected.diagnostic.contains("Unknown Connect To Peer connection type"));
    }

    @Test
    void fileSearchRespondsToRemoteAndOnlyDeliberateSelfSearch() {
        Fixture fixture = new Fixture(options(false, false));
        fixture.handle(searchRequest(USERNAME, TOKEN, "remote"));
        // Answering asks the share catalog and connects to the searcher, so it
        // goes to a thread of its own rather than the server's read loop.
        assertTrue(Eventually.holds(() -> fixture.responder.requests.size() == 1));

        fixture.handle(searchRequest(LOCAL_USER, TOKEN, "self"));
        assertEquals(1, fixture.responder.requests.size());

        SearchInternal network = new SearchInternal(SearchQuery.fromText("self"), SearchScope.getNetwork(), TOKEN);
        fixture.client.searches.put(TOKEN, network);
        fixture.handle(searchRequest(LOCAL_USER, TOKEN, "self"));
        assertEquals(1, fixture.responder.requests.size());
        network.close();

        SearchInternal otherUser = new SearchInternal(SearchQuery.fromText("self"), SearchScope.user("other"), TOKEN);
        fixture.client.searches.put(TOKEN, otherUser);
        fixture.handle(searchRequest(LOCAL_USER, TOKEN, "self"));
        assertEquals(1, fixture.responder.requests.size());
        otherUser.close();

        SearchInternal deliberate = new SearchInternal(SearchQuery.fromText("self"), SearchScope.user("LOCAL"), TOKEN);
        fixture.client.searches.put(TOKEN, deliberate);
        fixture.handle(searchRequest(LOCAL_USER, TOKEN, "self"));
        assertTrue(Eventually.holds(() -> fixture.responder.requests.size() == 2));
        deliberate.close();
    }

    @Test
    void embeddedMessageIsForwardedWithoutReceivedDiagnostic() {
        Fixture fixture = new Fixture(options(false, false));
        byte[] message = new MessageBuilder()
                .writeCode(MessageCode.Server.EMBEDDED_MESSAGE)
                .writeBytes(new byte[] {1, 2, 3})
                .build();
        fixture.handle(message);

        assertSame(message, fixture.client.embedded);
        assertFalse(fixture.diagnostic.contains("Server message received: EMBEDDED_MESSAGE"));
    }

    private static byte[] integer(MessageCode.Server code, int value) {
        return new MessageBuilder().writeCode(code).writeInteger(value).build();
    }

    private static byte[] string(MessageCode.Server code, String value) {
        return new MessageBuilder().writeCode(code).writeString(value).build();
    }

    private static byte[] stringList(MessageCode.Server code, String... values) {
        MessageBuilder builder = new MessageBuilder().writeCode(code).writeInteger(values.length);
        for (String value : values) {
            builder.writeString(value);
        }
        return builder.build();
    }

    private static byte[] status(String username, UserPresence presence, boolean privileged) {
        return new MessageBuilder()
                .writeCode(MessageCode.Server.GET_STATUS)
                .writeString(username)
                .writeInteger(presence.getValue())
                .writeByte(privileged ? 1 : 0)
                .build();
    }

    private static byte[] statistics(String username, int speed, long uploads, int files, int directories) {
        return new MessageBuilder()
                .writeCode(MessageCode.Server.GET_USER_STATS)
                .writeString(username)
                .writeInteger(speed)
                .writeLong(uploads)
                .writeInteger(files)
                .writeInteger(directories)
                .build();
    }

    private static byte[] roomList() {
        return new MessageBuilder()
                .writeCode(MessageCode.Server.ROOM_LIST)
                .writeInteger(1)
                .writeString("public")
                .writeInteger(1)
                .writeInteger(10)
                .writeInteger(1)
                .writeString("owned")
                .writeInteger(1)
                .writeInteger(20)
                .writeInteger(1)
                .writeString("private")
                .writeInteger(1)
                .writeInteger(30)
                .writeInteger(1)
                .writeString("moderated")
                .build();
    }

    private static byte[] chat(MessageCode.Server code, String room, String username, String message) {
        return new MessageBuilder()
                .writeCode(code)
                .writeString(room)
                .writeString(username)
                .writeString(message)
                .build();
    }

    private static byte[] userJoinedRoom() {
        return new MessageBuilder()
                .writeCode(MessageCode.Server.USER_JOINED_ROOM)
                .writeString(ROOM)
                .writeString(USERNAME)
                .writeInteger(UserPresence.ONLINE.getValue())
                .writeInteger(100)
                .writeLong(200L)
                .writeInteger(300)
                .writeInteger(400)
                .writeInteger(1)
                .writeString("CL")
                .build();
    }

    private static byte[] roomUsers(MessageCode.Server code) {
        return new MessageBuilder()
                .writeCode(code)
                .writeString(ROOM)
                .writeInteger(1)
                .writeString(USERNAME)
                .build();
    }

    private static byte[] roomOperation(MessageCode.Server code) {
        return new MessageBuilder()
                .writeCode(code)
                .writeString(ROOM)
                .writeString(USERNAME)
                .build();
    }

    private static byte[] netInfo() {
        byte[] reversed = ENDPOINT.getAddress().getAddress().clone();
        reverse(reversed);
        return new MessageBuilder()
                .writeCode(MessageCode.Server.NET_INFO)
                .writeInteger(1)
                .writeString(USERNAME)
                .writeBytes(reversed)
                .writeInteger(ENDPOINT.getPort())
                .build();
    }

    private static byte[] connectToPeer(String username, String type, int token) {
        byte[] reversed = ENDPOINT.getAddress().getAddress().clone();
        reverse(reversed);
        return new MessageBuilder()
                .writeCode(MessageCode.Server.CONNECT_TO_PEER)
                .writeString(username)
                .writeString(type)
                .writeBytes(reversed)
                .writeInteger(ENDPOINT.getPort())
                .writeInteger(token)
                .writeByte(0)
                .build();
    }

    private static byte[] searchRequest(String username, int token, String query) {
        return new MessageBuilder()
                .writeCode(MessageCode.Server.FILE_SEARCH)
                .writeString(username)
                .writeInteger(token)
                .writeString(query)
                .build();
    }

    private static void reverse(byte[] bytes) {
        for (int left = 0, right = bytes.length - 1; left < right; left++, right--) {
            byte value = bytes[left];
            bytes[left] = bytes[right];
            bytes[right] = value;
        }
    }

    private static SoulseekClientOptions options(boolean autoPrivate, boolean autoPrivilege) {
        SoulseekClientOptionsPatch patch = SoulseekClientOptionsPatch.builder()
                .autoAcknowledgePrivateMessages(autoPrivate)
                .autoAcknowledgePrivilegeNotifications(autoPrivilege)
                .build();
        return new SoulseekClientOptions().with(patch);
    }

    private static InetSocketAddress endpoint(int port) {
        try {
            return new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port);
        } catch (Exception exception) {
            throw new AssertionError(exception);
        }
    }

    private static <T> AtomicReference<T> listen(Fixture fixture, ServerMessageEvent event) {
        AtomicReference<T> result = new AtomicReference<>();
        fixture.handler.<T>subscribe(event, value -> result.set(value));
        return result;
    }

    /**
     * A frame whose code is outside the table is a protocol addition or a
     * newer client, and the C# source ignores it in the switch default. It
     * used to throw out of the prologue, before the try — and for the server
     * connection, killing the read loop kills the whole client.
     */
    @Test
    void anUnknownMessageCodeIsIgnoredNotFatal() {
        Fixture fixture = new Fixture(options(false, false));

        fixture.handle(java.nio.ByteBuffer.allocate(8)
                .order(java.nio.ByteOrder.LITTLE_ENDIAN)
                .putInt(4)
                .putInt(0x7FFF_FFF0)
                .array());

        // The connection is still being served: a known message right behind
        // the unknown one is handled normally.
        fixture.handle(new MessageBuilder().writeCode(MessageCode.Server.PING).build());
        assertTrue(fixture.waiter.completed.containsKey(new WaitKey(MessageCode.Server.PING)));
    }

    private static final class Fixture {
        private final RecordingDiagnostic diagnostic = new RecordingDiagnostic();
        private final RecordingWaiter waiter = new RecordingWaiter();
        private final PeerManagerProbe peer = new PeerManagerProbe();
        private final DistributedManagerProbe distributed = new DistributedManagerProbe();
        private final SearchResponderProbe responder = new SearchResponderProbe();
        private final FakeClient client;
        private final DefaultServerMessageHandler handler;

        private Fixture(SoulseekClientOptions options) {
            client = new FakeClient(options, waiter, peer.proxy, distributed.proxy, responder.proxy, diagnostic);
            handler = new DefaultServerMessageHandler(
                    () -> client.options,
                    client.server,
                    client.waiter,
                    () -> client.searches,
                    () -> client.downloads,
                    () -> client.peer,
                    () -> client.distributed,
                    client::distributedMessages,
                    () -> client.responder,
                    diagnostic);
        }

        private void handle(byte[] message) {
            handler.handleMessageRead(null, message);
        }
    }

    /**
     * The ports the handler is built from, and the server it acknowledges over.
     *
     * <p>The two acknowledgements used to be callbacks this recorded. They go
     * out over {@code ServerLink} now, so what the tests assert is the command
     * on the wire rather than a count of calls — which is what the client is
     * actually obliged to do.
     */
    private static final class FakeClient {
        private final SoulseekClientOptions options;
        private final Waiter waiter;
        private final PeerConnectionManager peer;
        private final DistributedConnectionManager distributed;
        private final SearchResponder responder;
        private final Map<Integer, SearchInternal> searches = new HashMap<>();
        private final Map<Integer, TransferInternal> downloads = new HashMap<>();
        private final List<OutgoingMessage> writes = new CopyOnWriteArrayList<>();
        private final ServerLink server;
        private byte[] embedded;

        private FakeClient(
                SoulseekClientOptions options,
                Waiter waiter,
                PeerConnectionManager peer,
                DistributedConnectionManager distributed,
                SearchResponder responder,
                DiagnosticSink diagnostic) {
            this.options = options;
            this.waiter = waiter;
            this.peer = peer;
            this.distributed = distributed;
            this.responder = responder;
            MessageConnection connection = (MessageConnection) Proxy.newProxyInstance(
                    MessageConnection.class.getClassLoader(),
                    new Class<?>[] {MessageConnection.class},
                    (proxy, method, arguments) -> {
                        if (method.getName().equals("write")
                                && arguments != null
                                && arguments[0] instanceof OutgoingMessage outgoing) {
                            writes.add(outgoing);
                        }
                        return defaultValue(method.getReturnType());
                    });
            server = ServerLinks.loggedIn(waiter, diagnostic, connection, LOCAL_USER);
        }

        /** The ids acknowledged, in order, of the given command type. */
        private List<Integer> acknowledged(Class<?> command) {
            List<Integer> ids = new ArrayList<>();
            for (OutgoingMessage message : writes) {
                if (command.isInstance(message)) {
                    ids.add(idOf(message));
                }
            }
            return ids;
        }

        private static int idOf(OutgoingMessage message) {
            byte[] bytes = message.toByteArray();
            return java.nio.ByteBuffer.wrap(bytes, 8, 4)
                    .order(java.nio.ByteOrder.LITTLE_ENDIAN)
                    .getInt();
        }

        private DistributedMessageHandler distributedMessages() {
            return (DistributedMessageHandler) Proxy.newProxyInstance(
                    DistributedMessageHandler.class.getClassLoader(),
                    new Class<?>[] {DistributedMessageHandler.class},
                    (proxy, method, arguments) -> {
                        if (method.getName().equals("handleEmbeddedMessage")) {
                            embedded = (byte[]) arguments[0];
                        }
                        return defaultValue(method.getReturnType());
                    });
        }
    }

    private static final class RecordingWaiter implements Waiter {
        // Synchronized rather than concurrent: a wait completed with no value
        // is recorded as a null, which a ConcurrentHashMap will not hold, and
        // the handler's dispatched threads write here while this one reads.
        private final Map<WaitKey, Object> completed = Collections.synchronizedMap(new HashMap<>());
        private final Map<WaitKey, Throwable> failures = Collections.synchronizedMap(new HashMap<>());

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

    private static final class PeerManagerProbe {
        private final List<ConnectToPeerResponse> messageRequests = new CopyOnWriteArrayList<>();
        private TransferConnectionResult transferResult;
        private final PeerConnectionManager proxy = (PeerConnectionManager) Proxy.newProxyInstance(
                PeerConnectionManager.class.getClassLoader(),
                new Class<?>[] {PeerConnectionManager.class},
                this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            return switch (method.getName()) {
                case "getOrAddMessageConnection" -> {
                    if (arguments.length == 1 && arguments[0] instanceof ConnectToPeerResponse response) {
                        messageRequests.add(response);
                    }
                    yield null;
                }
                case "getTransferConnection" -> transferResult;
                case "toString" -> "PeerManagerProbe";
                default -> defaultValue(method.getReturnType());
            };
        }
    }

    private static final class DistributedManagerProbe {
        private final List<ConnectToPeerResponse> childRequests = new CopyOnWriteArrayList<>();
        private final List<PeerEndpoint> parents = new CopyOnWriteArrayList<>();
        private RuntimeException addParent;
        private int removed;
        private int resets;
        private final DistributedConnectionManager proxy = (DistributedConnectionManager) Proxy.newProxyInstance(
                DistributedConnectionManager.class.getClassLoader(),
                new Class<?>[] {DistributedConnectionManager.class},
                this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            return switch (method.getName()) {
                case "getOrAddChildConnection" -> {
                    childRequests.add((ConnectToPeerResponse) arguments[0]);
                    yield null;
                }
                case "addParentConnection" -> {
                    parents.clear();
                    for (PeerEndpoint parent : (Iterable<PeerEndpoint>) arguments[0]) {
                        parents.add(parent);
                    }
                    if (addParent != null) {
                        throw addParent;
                    }
                    yield null;
                }
                case "removeAndDisposeAll" -> {
                    removed++;
                    yield null;
                }
                case "resetStatus" -> {
                    resets++;
                    yield null;
                }
                case "toString" -> "DistributedManagerProbe";
                default -> defaultValue(method.getReturnType());
            };
        }
    }

    private record SearchCall(String username, int token, String query) {}

    private static final class SearchResponderProbe {
        private final List<Integer> discards = new ArrayList<>();
        private final List<SearchCall> requests = new CopyOnWriteArrayList<>();
        private final SearchResponder proxy = (SearchResponder) Proxy.newProxyInstance(
                SearchResponder.class.getClassLoader(), new Class<?>[] {SearchResponder.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            return switch (method.getName()) {
                case "tryDiscard" -> {
                    discards.add((Integer) arguments[0]);
                    yield true;
                }
                case "tryRespond" -> {
                    if (arguments.length == 3) {
                        requests.add(
                                new SearchCall((String) arguments[0], (Integer) arguments[1], (String) arguments[2]));
                    }
                    yield CompletableFuture.completedFuture(true);
                }
                case "toString" -> "SearchResponderProbe";
                default -> defaultValue(method.getReturnType());
            };
        }
    }

    private static final class ConnectionProbe {
        private String disconnectMessage;
        private final Connection proxy = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            return switch (method.getName()) {
                case "getIpEndpoint" -> ENDPOINT;
                case "getId" -> UUID.fromString("00000000-0000-0000-0000-000000000001");
                case "disconnect" -> {
                    disconnectMessage = (String) arguments[0];
                    yield null;
                }
                case "toString" -> "ConnectionProbe";
                default -> defaultValue(method.getReturnType());
            };
        }
    }

    private static final class RecordingDiagnostic implements DiagnosticSink {
        // Copy-on-write: a handler's dispatched work reports its own failures
        // now, from a thread of its own, while the test thread is reading.
        private final List<String> messages = new CopyOnWriteArrayList<>();
        private final List<String> warnings = new CopyOnWriteArrayList<>();

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
