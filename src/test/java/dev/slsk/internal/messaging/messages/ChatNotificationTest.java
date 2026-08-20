// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.exceptions.MessageException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.internal.events.PrivateMessageReceivedEvent;
import dev.slsk.internal.events.PublicChatMessageReceivedEvent;
import dev.slsk.internal.events.RoomMessageReceivedEvent;
import dev.slsk.internal.events.RoomTickerListReceivedEvent;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.room.RoomTickerMessage;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ChatNotificationTest {
    @Test
    @DisplayName("Private message retains fields and parses timestamp/replay flag")
    void privateMessageParses() {
        Instant timestamp = Instant.ofEpochSecond(-1234);
        PrivateMessageNotification direct = new PrivateMessageNotification(42, timestamp, "alice", "hello", false);
        assertEquals(42, direct.getId());
        assertSame(timestamp, direct.getTimestamp());
        assertEquals("alice", direct.getUsername());
        assertEquals("hello", direct.getMessage());
        assertEquals(false, direct.isReplayed());

        byte[] frame = new MessageBuilder()
                .writeCode(MessageCode.Server.PRIVATE_MESSAGE)
                .writeInteger(42)
                .writeInteger(-1234)
                .writeString("alice")
                .writeString("hello")
                .writeByte(0)
                .build();
        PrivateMessageNotification parsed = PrivateMessageNotification.fromByteArray(frame);
        assertEquals(timestamp, parsed.getTimestamp());
        assertEquals(true, parsed.isReplayed());
    }

    @Test
    @DisplayName("Private message only treats replay byte one as not replayed")
    void privateMessageReplayFlagMatchesSource() {
        for (int value : new int[] {1, 2, 255}) {
            PrivateMessageNotification parsed = PrivateMessageNotification.fromByteArray(privateMessage(value));
            assertEquals(value != 1, parsed.isReplayed());
        }
    }

    @Test
    @DisplayName("Room and public chat messages retain and parse fields")
    void roomAndPublicMessagesParse() {
        RoomMessageNotification room =
                RoomMessageNotification.fromByteArray(threeStrings(MessageCode.Server.SAY_IN_CHAT_ROOM));
        PublicChatMessageNotification publicChat =
                PublicChatMessageNotification.fromByteArray(threeStrings(MessageCode.Server.PUBLIC_CHAT));

        assertChat(room.getRoomName(), room.getUsername(), room.getMessage());
        assertChat(publicChat.getRoomName(), publicChat.getUsername(), publicChat.getMessage());
        RoomMessageNotification directRoom = new RoomMessageNotification("room", "üser", "message");
        assertChat(directRoom.getRoomName(), directRoom.getUsername(), directRoom.getMessage());
        PublicChatMessageNotification directPublic = new PublicChatMessageNotification("room", "üser", "message");
        assertChat(directPublic.getRoomName(), directPublic.getUsername(), directPublic.getMessage());
    }

    @Test
    @DisplayName("Chat notifications reject code mismatches and missing data")
    void chatMessagesRejectInvalidFrames() {
        byte[] mismatch =
                new MessageBuilder().writeCode(MessageCode.Peer.BROWSE_REQUEST).build();

        assertThrows(MessageException.class, () -> PrivateMessageNotification.fromByteArray(mismatch));
        assertThrows(MessageException.class, () -> RoomMessageNotification.fromByteArray(mismatch));
        assertThrows(MessageException.class, () -> PublicChatMessageNotification.fromByteArray(mismatch));
        assertThrows(
                MessageReadException.class,
                () -> PrivateMessageNotification.fromByteArray(new MessageBuilder()
                        .writeCode(MessageCode.Server.PRIVATE_MESSAGE)
                        .build()));
    }

    @Test
    @DisplayName("Ticker added and removed notifications parse fields")
    void tickerChangesParse() {
        RoomTickerAddedNotification added =
                RoomTickerAddedNotification.fromByteArray(threeStrings(MessageCode.Server.ROOM_TICKER_ADD));
        RoomTickerRemovedNotification removed = RoomTickerRemovedNotification.fromByteArray(new MessageBuilder()
                .writeCode(MessageCode.Server.ROOM_TICKER_REMOVE)
                .writeString("room")
                .writeString("üser")
                .build());

        assertEquals("room", added.getRoomName());
        assertEquals("üser", added.getTicker().username());
        assertEquals("message", added.getTicker().message());
        assertEquals("room", removed.getRoomName());
        assertEquals("üser", removed.getUsername());
    }

    @Test
    @DisplayName("Ticker list preserves explicit count and snapshots values")
    void tickerListRetainsAndParses() {
        List<RoomTickerMessage> source = new ArrayList<>();
        RoomTickerMessage ticker = new RoomTickerMessage("alice", "hello");
        source.add(ticker);
        RoomTickerListNotification direct = new RoomTickerListNotification("room", 7, source);
        source.clear();

        assertEquals("room", direct.roomName());
        assertEquals(7, direct.tickerCount());
        assertEquals(List.of(ticker), direct.tickers());
        assertThrows(UnsupportedOperationException.class, () -> direct.tickers().clear());

        RoomTickerListNotification parsed = RoomTickerListNotification.fromByteArray(new MessageBuilder()
                .writeCode(MessageCode.Server.ROOM_TICKERS)
                .writeString("room")
                .writeInteger(2)
                .writeString("alice")
                .writeString("hello")
                .writeString("bob")
                .writeString("bye")
                .build());
        assertEquals(2, parsed.tickerCount());
        assertEquals("bob", parsed.tickers().get(1).username());
    }

    @Test
    @DisplayName("Ticker notifications reject mismatches and missing data")
    void tickerMessagesRejectInvalidFrames() {
        byte[] mismatch =
                new MessageBuilder().writeCode(MessageCode.Peer.BROWSE_REQUEST).build();

        assertThrows(MessageException.class, () -> RoomTickerAddedNotification.fromByteArray(mismatch));
        assertThrows(MessageException.class, () -> RoomTickerListNotification.fromByteArray(mismatch));
        assertThrows(MessageException.class, () -> RoomTickerRemovedNotification.fromByteArray(mismatch));
        assertThrows(
                MessageReadException.class,
                () -> RoomTickerListNotification.fromByteArray(new MessageBuilder()
                        .writeCode(MessageCode.Server.ROOM_TICKERS)
                        .build()));
    }

    @Test
    @DisplayName("Protocol notifications map to public event arguments")
    void notificationsMapToEventArguments() {
        PrivateMessageReceivedEvent privateArgs =
                new PrivateMessageReceivedEvent(PrivateMessageNotification.fromByteArray(privateMessage(0)));
        assertEquals(42, privateArgs.id());
        assertEquals("alice", privateArgs.username());
        assertEquals(true, privateArgs.replayed());

        RoomMessageReceivedEvent roomArgs = new RoomMessageReceivedEvent(
                RoomMessageNotification.fromByteArray(threeStrings(MessageCode.Server.SAY_IN_CHAT_ROOM)));
        assertChat(roomArgs.roomName(), roomArgs.username(), roomArgs.message());

        PublicChatMessageReceivedEvent publicArgs = new PublicChatMessageReceivedEvent(
                PublicChatMessageNotification.fromByteArray(threeStrings(MessageCode.Server.PUBLIC_CHAT)));
        assertChat(publicArgs.roomName(), publicArgs.username(), publicArgs.message());

        RoomTickerListNotification list =
                new RoomTickerListNotification("room", 99, List.of(new RoomTickerMessage("alice", "hello")));
        RoomTickerListReceivedEvent tickerArgs = new RoomTickerListReceivedEvent(list);
        assertEquals("room", tickerArgs.roomName());
        assertEquals(1, tickerArgs.tickerCount());
    }

    private static byte[] privateMessage(int replayByte) {
        return new MessageBuilder()
                .writeCode(MessageCode.Server.PRIVATE_MESSAGE)
                .writeInteger(42)
                .writeInteger(-1234)
                .writeString("alice")
                .writeString("hello")
                .writeByte(replayByte)
                .build();
    }

    private static byte[] threeStrings(MessageCode.Server code) {
        return new MessageBuilder()
                .writeCode(code)
                .writeString("room")
                .writeString("üser")
                .writeString("message")
                .build();
    }

    private static void assertChat(String roomName, String username, String message) {
        assertEquals("room", roomName);
        assertEquals("üser", username);
        assertEquals("message", message);
    }
}
