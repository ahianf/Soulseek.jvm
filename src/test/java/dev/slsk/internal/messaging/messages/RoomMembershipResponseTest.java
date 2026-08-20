// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.exceptions.MessageException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.internal.events.RoomJoinedEvent;
import dev.slsk.internal.events.RoomLeftEvent;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.room.RoomData;
import dev.slsk.internal.user.UserData;
import dev.slsk.internal.user.WireUserPresence;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RoomMembershipResponseTest {
    @Test
    @DisplayName("Cannot-join and leave responses retain room names")
    void simpleRoomResponsesParse() {
        CannotJoinRoomNotification cannot =
                CannotJoinRoomNotification.fromByteArray(oneString(MessageCode.Server.CANNOT_JOIN_ROOM, "secret"));
        LeaveRoomResponse left = LeaveRoomResponse.fromByteArray(oneString(MessageCode.Server.LEAVE_ROOM, "secret"));

        assertEquals("secret", cannot.getRoomName());
        assertEquals("secret", left.getRoomName());
        assertEquals("secret", new CannotJoinRoomNotification("secret").getRoomName());
        assertEquals("secret", new LeaveRoomResponse("secret").getRoomName());
    }

    @Test
    @DisplayName("Join-room parses an empty public room")
    void emptyPublicRoomParses() {
        RoomData room = JoinRoomResponse.fromByteArray(new MessageBuilder()
                .writeCode(MessageCode.Server.JOIN_ROOM)
                .writeString("room")
                .writeInteger(0)
                .writeInteger(0)
                .writeInteger(0)
                .writeInteger(0)
                .writeInteger(0)
                .build());

        assertEquals("room", room.name());
        assertEquals(0, room.userCount());
        assertEquals(false, room.privateRoom());
        assertNull(room.owner());
        assertNull(room.operators());
    }

    @Test
    @DisplayName("Join-room combines columnar data for multiple users")
    void publicRoomUsersParse() {
        RoomData room = JoinRoomResponse.fromByteArray(joinedRoomFrame(false));

        assertEquals("room", room.name());
        assertEquals(2, room.userCount());
        UserData alice = room.users().get(0);
        assertEquals("alice", alice.username());
        assertEquals(WireUserPresence.ONLINE, alice.status());
        assertEquals(10, alice.averageSpeed());
        assertEquals(11L, alice.uploadCount());
        assertEquals(12, alice.fileCount());
        assertEquals(13, alice.directoryCount());
        assertEquals(14, alice.slotsFree());
        assertEquals("CL", alice.countryCode());
        assertEquals(WireUserPresence.AWAY, room.users().get(1).status());
    }

    @Test
    @DisplayName("Join-room parses private owner and operators")
    void privateRoomParses() {
        RoomData room = JoinRoomResponse.fromByteArray(joinedRoomFrame(true));

        assertTrue(room.privateRoom());
        assertEquals("owner", room.owner());
        assertEquals(2, room.operatorCount());
        assertEquals(List.of("op1", "op2"), room.operators());
    }

    @Test
    @DisplayName("User-joined parses complete user data and maps event args")
    void userJoinedParses() {
        UserJoinedRoomNotification notification = UserJoinedRoomNotification.fromByteArray(new MessageBuilder()
                .writeCode(MessageCode.Server.USER_JOINED_ROOM)
                .writeString("room")
                .writeString("alice")
                .writeInteger(WireUserPresence.ONLINE.getValue())
                .writeInteger(10)
                .writeLong(11)
                .writeInteger(12)
                .writeInteger(13)
                .writeInteger(14)
                .writeString("")
                .build());
        UserData data = notification.getUserData();
        assertEquals("room", notification.getRoomName());
        assertEquals("alice", notification.getUsername());
        assertEquals("alice", data.username());
        assertEquals(WireUserPresence.ONLINE, data.status());
        assertEquals(10, data.averageSpeed());
        assertEquals(11L, data.uploadCount());
        assertEquals(12, data.fileCount());
        assertEquals(13, data.directoryCount());
        assertEquals(14, data.slotsFree());
        assertEquals("", data.countryCode());

        RoomJoinedEvent eventData = new RoomJoinedEvent(notification);
        assertEquals("room", eventData.roomName());
        assertEquals("alice", eventData.username());
        assertSame(data, eventData.userData());
    }

    @Test
    @DisplayName("User-left retains fields and maps event args")
    void userLeftParses() {
        UserLeftRoomNotification direct = new UserLeftRoomNotification("room", "alice");
        UserLeftRoomNotification parsed = UserLeftRoomNotification.fromByteArray(new MessageBuilder()
                .writeCode(MessageCode.Server.USER_LEFT_ROOM)
                .writeString("room")
                .writeString("alice")
                .build());
        assertEquals("room", direct.getRoomName());
        assertEquals("alice", direct.getUsername());
        assertEquals("room", parsed.getRoomName());
        assertEquals("alice", parsed.getUsername());

        RoomLeftEvent eventData = new RoomLeftEvent(parsed);
        assertEquals("room", eventData.roomName());
        assertEquals("alice", eventData.username());
    }

    @Test
    @DisplayName("Room responses reject mismatches and missing data")
    void roomResponsesRejectInvalidFrames() {
        byte[] mismatch =
                new MessageBuilder().writeCode(MessageCode.Peer.BROWSE_REQUEST).build();

        assertThrows(MessageException.class, () -> CannotJoinRoomNotification.fromByteArray(mismatch));
        assertThrows(MessageException.class, () -> JoinRoomResponse.fromByteArray(mismatch));
        assertThrows(MessageException.class, () -> LeaveRoomResponse.fromByteArray(mismatch));
        assertThrows(MessageException.class, () -> UserJoinedRoomNotification.fromByteArray(mismatch));
        assertThrows(MessageException.class, () -> UserLeftRoomNotification.fromByteArray(mismatch));

        assertThrows(
                MessageReadException.class,
                () -> JoinRoomResponse.fromByteArray(new MessageBuilder()
                        .writeCode(MessageCode.Server.JOIN_ROOM)
                        .build()));
        assertThrows(
                MessageReadException.class,
                () -> UserJoinedRoomNotification.fromByteArray(new MessageBuilder()
                        .writeCode(MessageCode.Server.USER_JOINED_ROOM)
                        .build()));
        assertThrows(
                MessageReadException.class,
                () -> UserLeftRoomNotification.fromByteArray(new MessageBuilder()
                        .writeCode(MessageCode.Server.USER_LEFT_ROOM)
                        .build()));
    }

    private static byte[] oneString(MessageCode.Server code, String value) {
        return new MessageBuilder().writeCode(code).writeString(value).build();
    }

    private static byte[] joinedRoomFrame(boolean privateRoom) {
        MessageBuilder builder = new MessageBuilder()
                .writeCode(MessageCode.Server.JOIN_ROOM)
                .writeString("room")
                .writeInteger(2)
                .writeString("alice")
                .writeString("bob")
                .writeInteger(2)
                .writeInteger(WireUserPresence.ONLINE.getValue())
                .writeInteger(WireUserPresence.AWAY.getValue())
                .writeInteger(2)
                .writeInteger(10)
                .writeLong(11)
                .writeInteger(12)
                .writeInteger(13)
                .writeInteger(20)
                .writeLong(21)
                .writeInteger(22)
                .writeInteger(23)
                .writeInteger(2)
                .writeInteger(14)
                .writeInteger(24)
                .writeInteger(2)
                .writeString("CL")
                .writeString("US");
        if (privateRoom) {
            builder.writeString("owner").writeInteger(2).writeString("op1").writeString("op2");
        }
        return builder.build();
    }
}
