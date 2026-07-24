// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.CancellationToken;
import dev.slsk.RoomData;
import dev.slsk.RoomInfo;
import dev.slsk.RoomList;
import dev.slsk.SearchQuery;
import dev.slsk.SearchScope;
import dev.slsk.ServerInfo;
import dev.slsk.TransferDirection;
import dev.slsk.UserPresence;
import dev.slsk.UserStatistics;
import dev.slsk.UserStatus;
import dev.slsk.common.Constants;
import dev.slsk.common.WaitKey;
import dev.slsk.common.Waiter;
import dev.slsk.diagnostics.DiagnosticSink;
import dev.slsk.events.PrivateMessageReceivedEvent;
import dev.slsk.events.PrivilegeNotificationReceivedEvent;
import dev.slsk.events.PublicChatMessageReceivedEvent;
import dev.slsk.events.RoomJoinedEvent;
import dev.slsk.events.RoomLeftEvent;
import dev.slsk.events.RoomMessageReceivedEvent;
import dev.slsk.events.RoomTickerAddedEvent;
import dev.slsk.events.RoomTickerListReceivedEvent;
import dev.slsk.events.RoomTickerRemovedEvent;
import dev.slsk.events.UserCannotConnectEvent;
import dev.slsk.exceptions.RoomJoinForbiddenException;
import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.messages.ConnectToPeerResponse;
import dev.slsk.messaging.messages.LoginResponse;
import dev.slsk.messaging.messages.NewPassword;
import dev.slsk.messaging.messages.PrivateRoomToggle;
import dev.slsk.messaging.messages.UserAddressResponse;
import dev.slsk.messaging.messages.WatchUserResponse;
import dev.slsk.network.DistributedConnectionManager;
import dev.slsk.network.MessageConnection;
import dev.slsk.network.MessageEvent;
import dev.slsk.network.PeerConnectionManager;
import dev.slsk.network.PeerEndpoint;
import dev.slsk.network.TransferConnectionResult;
import dev.slsk.network.tcp.Connection;
import dev.slsk.options.SoulseekClientOptions;
import dev.slsk.options.SoulseekClientOptionsPatch;
import dev.slsk.search.SearchInternal;
import dev.slsk.search.SearchResponder;
import dev.slsk.transfer.TransferInternal;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
        assertThrows(NullPointerException.class, () -> new DefaultServerMessageHandler(null));
    }

    @Test
    void diagnosticsCoverReadUnhandledFailureAndWrite() {
        Fixture fixture = new Fixture(options(false, false));
        fixture.handler
                .handleMessageReadAsync(
                        null,
                        new MessageBuilder()
                                .writeCode(MessageCode.Server.ASK_PUBLIC_CHAT)
                                .build())
                .join();
        assertTrue(fixture.diagnostic.contains("Server message received: ASK_PUBLIC_CHAT"));
        assertTrue(fixture.diagnostic.contains("Unhandled server message"));

        fixture.handler
                .handleMessageReadAsync(
                        null,
                        new MessageBuilder()
                                .writeCode(MessageCode.Server.ROOM_LIST)
                                .build())
                .join();
        assertTrue(fixture.diagnostic.containsWarning("Error handling server message"));

        fixture.handler.handleMessageWritten(null, new MessageEvent(searchRequest(USERNAME, TOKEN, "query")));
        assertTrue(fixture.diagnostic.contains("Server message sent: FILE_SEARCH"));

        AtomicInteger generated = new AtomicInteger();
        DefaultServerMessageHandler defaultDiagnostic = new DefaultServerMessageHandler(fixture.client);
        defaultDiagnostic.addDiagnosticGeneratedListener((sender, eventData) -> generated.incrementAndGet());
        defaultDiagnostic
                .handleMessageReadAsync(
                        null,
                        new MessageBuilder()
                                .writeCode(MessageCode.Server.DISTRIBUTED_RESET)
                                .build())
                .join();
        assertEquals(1, generated.get());
    }

    @Test
    void scalarServerInfoAndBasicWaitsUseSourceKeys() {
        Fixture fixture = new Fixture(options(false, false));
        List<ServerInfo> info = new ArrayList<>();
        fixture.handler.<ServerInfo>addListener(
                ServerMessageEvent.SERVER_INFO_RECEIVED, (sender, value) -> info.add(value));

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

        assertEquals(11, info.get(0).getParentMinSpeed());
        assertEquals(22, info.get(1).getParentSpeedRatio());
        assertEquals(33, info.get(2).getWishlistInterval());
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
        fixture.handler.<UserStatus>addListener(
                ServerMessageEvent.USER_STATUS_CHANGED, (sender, value) -> statusEvent.set(value));
        fixture.handler.<UserStatistics>addListener(
                ServerMessageEvent.USER_STATISTICS_CHANGED, (sender, value) -> statisticsEvent.set(value));

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
        fixture.handler.<RoomLeftEvent>addListener(ServerMessageEvent.ROOM_LEFT, (sender, value) -> left.set(value));

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
        assertEquals(ROOM, joined.getName());
        assertTrue(fixture.waiter.completed.containsKey(new WaitKey(MessageCode.Server.LEAVE_ROOM, ROOM)));
        assertEquals(LOCAL_USER, left.get().getUsername());
        assertEquals(ROOM, left.get().getRoomName());
        assertInstanceOf(
                RoomJoinForbiddenException.class,
                fixture.waiter.failures.get(new WaitKey(MessageCode.Server.JOIN_ROOM, "forbidden")));
    }

    @Test
    void roomListCompletesWaitAndRaisesSameSnapshot() {
        Fixture fixture = new Fixture(options(false, false));
        AtomicReference<RoomList> event = new AtomicReference<>();
        fixture.handler.<RoomList>addListener(
                ServerMessageEvent.ROOM_LIST_RECEIVED, (sender, value) -> event.set(value));

        fixture.handle(roomList());

        Object waited = fixture.waiter.completed.get(new WaitKey(MessageCode.Server.ROOM_LIST));
        assertSame(event.get(), waited);
        assertEquals("public", event.get().getPublic().getFirst().getName());
        assertEquals("private", event.get().getPrivate().getFirst().getName());
        assertEquals("owned", event.get().getOwned().getFirst().getName());
        assertEquals("moderated", event.get().getModeratedRoomNames().getFirst());
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

        assertEquals("hello", roomMessage.get().getMessage());
        assertEquals("public", publicMessage.get().getMessage());
        assertEquals(USERNAME, joined.get().getUsername());
        assertEquals(USERNAME, left.get().getUsername());
        assertEquals(1, tickers.get().getTickerCount());
        assertEquals("added", tickerAdded.get().getTicker().getMessage());
        assertEquals(USERNAME, tickerRemoved.get().getUsername());
    }

    @Test
    void privateAndPrivilegeNotificationsHonorAcknowledgementOptions() {
        Fixture fixture = new Fixture(options(true, true));
        AtomicReference<PrivateMessageReceivedEvent> privateMessage =
                listen(fixture, ServerMessageEvent.PRIVATE_MESSAGE_RECEIVED);
        List<PrivilegeNotificationReceivedEvent> privileges = new ArrayList<>();
        fixture.handler.<PrivilegeNotificationReceivedEvent>addListener(
                ServerMessageEvent.PRIVILEGE_NOTIFICATION_RECEIVED, (sender, value) -> privileges.add(value));

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

        assertEquals(12, privateMessage.get().getId());
        assertEquals(1, fixture.client.privateAcknowledgements.size());
        assertEquals(12, fixture.client.privateAcknowledgements.getFirst());
        assertNull(privileges.get(0).getId());
        assertEquals(13, privileges.get(1).getId());
        assertEquals(13, fixture.client.privilegeAcknowledgements.getFirst());

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
        assertTrue(disabled.client.privateAcknowledgements.isEmpty());
        assertTrue(disabled.client.privilegeAcknowledgements.isEmpty());
    }

    @Test
    void listGlobalAndKickNotificationsRaiseSourceEvents() {
        Fixture fixture = new Fixture(options(false, false));
        AtomicReference<List<String>> privileged = listen(fixture, ServerMessageEvent.PRIVILEGED_USER_LIST_RECEIVED);
        AtomicReference<List<String>> excluded = listen(fixture, ServerMessageEvent.EXCLUDED_SEARCH_PHRASES_RECEIVED);
        AtomicReference<String> global = listen(fixture, ServerMessageEvent.GLOBAL_MESSAGE_RECEIVED);
        AtomicInteger kicked = new AtomicInteger();
        fixture.handler.<Void>addListener(
                ServerMessageEvent.KICKED_FROM_SERVER, (sender, value) -> kicked.incrementAndGet());

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
        assertEquals(List.of(USERNAME), users.get().getUsers());
        assertEquals(List.of(USERNAME), moderated.get().getUsers());
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
        fixture.handler.<Void>addListener(
                ServerMessageEvent.DISTRIBUTED_NETWORK_RESET, (sender, value) -> resets.incrementAndGet());
        fixture.handle(netInfo());
        fixture.handle(new MessageBuilder()
                .writeCode(MessageCode.Server.DISTRIBUTED_RESET)
                .build());

        assertEquals(new PeerEndpoint(USERNAME, ENDPOINT), fixture.distributed.parents.getFirst());
        assertEquals(1, fixture.distributed.removed);
        assertEquals(1, fixture.distributed.resets);
        assertEquals(1, resets.get());

        fixture.distributed.addParent = CompletableFuture.failedFuture(new RuntimeException("parent failure"));
        fixture.handle(netInfo());
        assertTrue(fixture.diagnostic.contains("Error handling NetInfo message: parent failure"));
    }

    @Test
    void cannotConnectDiscardsAndRaisesOnlyWhenUsernameExists() {
        Fixture fixture = new Fixture(options(false, false));
        List<UserCannotConnectEvent> events = new ArrayList<>();
        fixture.handler.<UserCannotConnectEvent>addListener(
                ServerMessageEvent.USER_CANNOT_CONNECT, (sender, value) -> events.add(value));

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
        assertEquals(USERNAME, events.getFirst().getUsername());
    }

    @Test
    void connectToPeerDelegatesPeerAndDistributedTypes() {
        Fixture fixture = new Fixture(options(false, false));
        fixture.handle(connectToPeer(USERNAME, Constants.ConnectionType.PEER, TOKEN));
        fixture.handle(connectToPeer(USERNAME, Constants.ConnectionType.DISTRIBUTED, TOKEN + 1));

        assertEquals(1, fixture.peer.messageRequests.size());
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

        assertSame(
                connection.proxy,
                fixture.waiter.completed.get(new WaitKey(Constants.WaitKey.INDIRECT_TRANSFER, USERNAME, "file", 91)));

        fixture.peer.transferResult = new TransferConnectionResult(connection.proxy, 92);
        fixture.handle(connectToPeer(USERNAME, Constants.ConnectionType.TRANSFER, TOKEN + 1));
        assertEquals("Unknown transfer", connection.disconnectMessage);

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
        assertEquals(1, fixture.responder.requests.size());

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
        assertEquals(2, fixture.responder.requests.size());
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
                autoPrivate,
                autoPrivilege,
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
                null);
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
        fixture.handler.<T>addListener(event, (sender, value) -> result.set(value));
        return result;
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
            client = new FakeClient(options, waiter, peer.proxy, distributed.proxy, responder.proxy);
            handler = new DefaultServerMessageHandler(client, diagnostic);
        }

        private void handle(byte[] message) {
            handler.handleMessageReadAsync(null, message).join();
        }
    }

    private static final class FakeClient implements ServerMessageHandlerClient {
        private final SoulseekClientOptions options;
        private final Waiter waiter;
        private final PeerConnectionManager peer;
        private final DistributedConnectionManager distributed;
        private final SearchResponder responder;
        private final Map<Integer, SearchInternal> searches = new HashMap<>();
        private final Map<Integer, TransferInternal> downloads = new HashMap<>();
        private final List<Integer> privateAcknowledgements = new ArrayList<>();
        private final List<Integer> privilegeAcknowledgements = new ArrayList<>();
        private byte[] embedded;

        private FakeClient(
                SoulseekClientOptions options,
                Waiter waiter,
                PeerConnectionManager peer,
                DistributedConnectionManager distributed,
                SearchResponder responder) {
            this.options = options;
            this.waiter = waiter;
            this.peer = peer;
            this.distributed = distributed;
            this.responder = responder;
        }

        @Override
        public SoulseekClientOptions getOptions() {
            return options;
        }

        @Override
        public String getUsername() {
            return LOCAL_USER;
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

        @Override
        public PeerConnectionManager getPeerConnectionManager() {
            return peer;
        }

        @Override
        public DistributedConnectionManager getDistributedConnectionManager() {
            return distributed;
        }

        @Override
        public DistributedMessageHandler getDistributedMessageHandler() {
            return new DistributedMessageHandler() {
                @Override
                public void handleEmbeddedMessage(byte[] message) {
                    embedded = message;
                }

                @Override
                public void addDiagnosticGeneratedListener(dev.slsk.diagnostics.DiagnosticEventListener listener) {}

                @Override
                public void removeDiagnosticGeneratedListener(dev.slsk.diagnostics.DiagnosticEventListener listener) {}

                @Override
                public void handleMessageRead(MessageConnection sender, MessageEvent eventData) {}

                @Override
                public void handleMessageRead(MessageConnection sender, byte[] message) {}

                @Override
                public void handleMessageWritten(MessageConnection sender, MessageEvent eventData) {}

                @Override
                public void handleChildMessageRead(MessageConnection sender, MessageEvent eventData) {}

                @Override
                public void handleChildMessageRead(MessageConnection sender, byte[] message) {}

                @Override
                public void handleChildMessageWritten(MessageConnection sender, MessageEvent eventData) {}
            };
        }

        @Override
        public SearchResponder getSearchResponder() {
            return responder;
        }

        @Override
        public CompletableFuture<Void> acknowledgePrivateMessageAsync(int id, CancellationToken cancellationToken) {
            privateAcknowledgements.add(id);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> acknowledgePrivilegeNotificationAsync(
                int id, CancellationToken cancellationToken) {
            privilegeAcknowledgements.add(id);
            return CompletableFuture.completedFuture(null);
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
        public CompletableFuture<Void> waitAsync(WaitKey key, Integer timeout, CancellationToken cancellationToken) {
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
                WaitKey key, Class<T> resultType, Integer timeout, CancellationToken cancellationToken) {
            return waitAsync(key, resultType);
        }

        @Override
        public CompletableFuture<Void> waitIndefinitelyAsync(WaitKey key) {
            return waitAsync(key);
        }

        @Override
        public CompletableFuture<Void> waitIndefinitelyAsync(WaitKey key, CancellationToken cancellationToken) {
            return waitAsync(key);
        }

        @Override
        public <T> CompletableFuture<T> waitIndefinitelyAsync(WaitKey key, Class<T> resultType) {
            return waitAsync(key, resultType);
        }

        @Override
        public <T> CompletableFuture<T> waitIndefinitelyAsync(
                WaitKey key, Class<T> resultType, CancellationToken cancellationToken) {
            return waitAsync(key, resultType);
        }

        @Override
        public void close() {}
    }

    private static final class PeerManagerProbe {
        private final List<ConnectToPeerResponse> messageRequests = new ArrayList<>();
        private TransferConnectionResult transferResult;
        private final PeerConnectionManager proxy = (PeerConnectionManager) Proxy.newProxyInstance(
                PeerConnectionManager.class.getClassLoader(),
                new Class<?>[] {PeerConnectionManager.class},
                this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            return switch (method.getName()) {
                case "getOrAddMessageConnectionAsync" -> {
                    if (arguments.length == 1 && arguments[0] instanceof ConnectToPeerResponse response) {
                        messageRequests.add(response);
                    }
                    yield CompletableFuture.completedFuture(null);
                }
                case "getTransferConnectionAsync" -> CompletableFuture.completedFuture(transferResult);
                case "toString" -> "PeerManagerProbe";
                default -> defaultValue(method.getReturnType());
            };
        }
    }

    private static final class DistributedManagerProbe {
        private final List<ConnectToPeerResponse> childRequests = new ArrayList<>();
        private final List<PeerEndpoint> parents = new ArrayList<>();
        private CompletableFuture<Void> addParent = CompletableFuture.completedFuture(null);
        private int removed;
        private int resets;
        private final DistributedConnectionManager proxy = (DistributedConnectionManager) Proxy.newProxyInstance(
                DistributedConnectionManager.class.getClassLoader(),
                new Class<?>[] {DistributedConnectionManager.class},
                this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            return switch (method.getName()) {
                case "getOrAddChildConnectionAsync" -> {
                    childRequests.add((ConnectToPeerResponse) arguments[0]);
                    yield CompletableFuture.completedFuture(null);
                }
                case "addParentConnectionAsync" -> {
                    parents.clear();
                    for (PeerEndpoint parent : (Iterable<PeerEndpoint>) arguments[0]) {
                        parents.add(parent);
                    }
                    yield addParent;
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
        private final List<SearchCall> requests = new ArrayList<>();
        private final SearchResponder proxy = (SearchResponder) Proxy.newProxyInstance(
                SearchResponder.class.getClassLoader(), new Class<?>[] {SearchResponder.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            return switch (method.getName()) {
                case "tryDiscard" -> {
                    discards.add((Integer) arguments[0]);
                    yield true;
                }
                case "tryRespondAsync" -> {
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
