// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MultiFieldServerCommandTest {
    @Test
    @DisplayName("GivePrivileges preserves properties and wire order")
    void givePrivilegesPreservesData() {
        GivePrivilegesCommand command = new GivePrivilegesCommand("alice", -17);
        MessageReader<MessageCode.Server> reader = reader(command);

        assertEquals("alice", command.getUsername());
        assertEquals(-17, command.getDays());
        assertEquals(MessageCode.Server.GIVE_PRIVILEGES, reader.readCode());
        assertEquals("alice", reader.readString());
        assertEquals(-17, reader.readInteger());
        assertEquals(0, reader.getRemaining());
    }

    @Test
    @DisplayName("ConnectToPeer preserves properties and wire order")
    void connectToPeerPreservesData() {
        ConnectToPeerRequest request = new ConnectToPeerRequest(0x12345678, "alice", "P");
        MessageReader<MessageCode.Server> reader = reader(request);

        assertEquals(0x12345678, request.getToken());
        assertEquals("alice", request.getUsername());
        assertEquals("P", request.getType());
        assertEquals(MessageCode.Server.CONNECT_TO_PEER, reader.readCode());
        assertEquals(0x12345678, reader.readInteger());
        assertEquals("alice", reader.readString());
        assertEquals("P", reader.readString());
        assertEquals(0, reader.getRemaining());
    }

    @Test
    @DisplayName("JoinRoom preserves optional privacy and integer flag")
    void joinRoomPreservesData() {
        JoinRoomRequest publicRoom = new JoinRoomRequest("public");
        JoinRoomRequest privateRoom = new JoinRoomRequest("private", true);

        assertEquals("public", publicRoom.getRoomName());
        assertEquals(false, publicRoom.isPrivate());
        assertEquals("private", privateRoom.getRoomName());
        assertEquals(true, privateRoom.isPrivate());
        assertRoomFlag(publicRoom, "public", 0);
        assertRoomFlag(privateRoom, "private", 1);
    }

    @Test
    @DisplayName("Message and ticker commands preserve both strings")
    void messageCommandsPreserveData() {
        RoomMessageCommand room = new RoomMessageCommand("r", "room");
        PrivateMessageCommand direct = new PrivateMessageCommand("u", "private");
        SetRoomTickerCommand ticker = new SetRoomTickerCommand("r", "ticker");

        assertEquals("r", room.getRoomName());
        assertEquals("room", room.getMessage());
        assertEquals("u", direct.getUsername());
        assertEquals("private", direct.getMessage());
        assertEquals("r", ticker.getRoomName());
        assertEquals("ticker", ticker.getMessage());
        assertTwoStrings(room, MessageCode.Server.SAY_IN_CHAT_ROOM, "r", "room");
        assertTwoStrings(direct, MessageCode.Server.PRIVATE_MESSAGE, "u", "private");
        assertTwoStrings(ticker, MessageCode.Server.SET_ROOM_TICKER, "r", "ticker");
    }

    @Test
    @DisplayName("Search commands preserve scope fields and wire order")
    void searchCommandsPreserveData() {
        SearchRequestMessage network = new SearchRequestMessage("n", 1);
        WishlistSearchRequest wishlist = new WishlistSearchRequest("w", 2);
        RoomSearchRequest room = new RoomSearchRequest("r", "q", 3);
        UserSearchRequest user = new UserSearchRequest("u", "q", 4);

        assertTokenQuery(network, MessageCode.Server.FILE_SEARCH, 1, "n");
        assertTokenQuery(wishlist, MessageCode.Server.WISHLIST_SEARCH, 2, "w");
        assertScopedSearch(room, MessageCode.Server.ROOM_SEARCH, "r", 3, "q");
        assertScopedSearch(user, MessageCode.Server.USER_SEARCH, "u", 4, "q");
        assertEquals("n", network.getSearchText());
        assertEquals(1, network.getToken());
        assertEquals("w", wishlist.getSearchText());
        assertEquals(2, wishlist.getToken());
        assertEquals("r", room.getRoomName());
        assertEquals("q", room.getSearchText());
        assertEquals(3, room.getToken());
        assertEquals("u", user.getUsername());
        assertEquals("q", user.getSearchText());
        assertEquals(4, user.getToken());
    }

    @Test
    @DisplayName("Shared counts preserve signed values and exact fields")
    void sharedCountsPreserveData() {
        SetSharedCountsCommand command = new SetSharedCountsCommand(-1, Integer.MIN_VALUE);
        MessageReader<MessageCode.Server> reader = reader(command);

        assertEquals(-1, command.getDirectoryCount());
        assertEquals(Integer.MIN_VALUE, command.getFileCount());
        assertEquals(MessageCode.Server.SHARED_FOLDERS_AND_FILES, reader.readCode());
        assertEquals(-1, reader.readInteger());
        assertEquals(Integer.MIN_VALUE, reader.readInteger());
        assertEquals(0, reader.getRemaining());
    }

    private static void assertRoomFlag(JoinRoomRequest request, String roomName, int flag) {
        MessageReader<MessageCode.Server> reader = reader(request);
        assertEquals(MessageCode.Server.JOIN_ROOM, reader.readCode());
        assertEquals(roomName, reader.readString());
        assertEquals(flag, reader.readInteger());
        assertEquals(0, reader.getRemaining());
    }

    private static void assertTwoStrings(
            OutgoingMessage message, MessageCode.Server code, String first, String second) {
        MessageReader<MessageCode.Server> reader = reader(message);
        assertEquals(code, reader.readCode());
        assertEquals(first, reader.readString());
        assertEquals(second, reader.readString());
        assertEquals(0, reader.getRemaining());
    }

    private static void assertTokenQuery(OutgoingMessage message, MessageCode.Server code, int token, String query) {
        MessageReader<MessageCode.Server> reader = reader(message);
        assertEquals(code, reader.readCode());
        assertEquals(token, reader.readInteger());
        assertEquals(query, reader.readString());
        assertEquals(0, reader.getRemaining());
    }

    private static void assertScopedSearch(
            OutgoingMessage message, MessageCode.Server code, String scope, int token, String query) {
        MessageReader<MessageCode.Server> reader = reader(message);
        assertEquals(code, reader.readCode());
        assertEquals(scope, reader.readString());
        assertEquals(token, reader.readInteger());
        assertEquals(query, reader.readString());
        assertEquals(0, reader.getRemaining());
    }

    private static MessageReader<MessageCode.Server> reader(OutgoingMessage message) {
        return new MessageReader<>(message.toByteArray(), MessageCode.Server.class);
    }
}
