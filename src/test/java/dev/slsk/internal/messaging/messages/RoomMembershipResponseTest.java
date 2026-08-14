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
import dev.slsk.internal.RoomData;
import dev.slsk.internal.events.RoomJoinedEvent;
import dev.slsk.internal.events.RoomLeftEvent;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.user.UserData;
import dev.slsk.internal.user.UserPresence;
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

        assertEquals("room", room.getName());
        assertEquals(0, room.getUserCount());
        assertEquals(false, room.isPrivate());
        assertNull(room.getOwner());
        assertNull(room.getOperators());
    }

    @Test
    @DisplayName("Join-room combines columnar data for multiple users")
    void publicRoomUsersParse() {
        RoomData room = JoinRoomResponse.fromByteArray(joinedRoomFrame(false));

        assertEquals("room", room.getName());
        assertEquals(2, room.getUserCount());
        UserData alice = room.getUsers().get(0);
        assertEquals("alice", alice.getUsername());
        assertEquals(UserPresence.ONLINE, alice.getStatus());
        assertEquals(10, alice.getAverageSpeed());
        assertEquals(11L, alice.getUploadCount());
        assertEquals(12, alice.getFileCount());
        assertEquals(13, alice.getDirectoryCount());
        assertEquals(14, alice.getSlotsFree());
        assertEquals("CL", alice.getCountryCode());
        assertEquals(UserPresence.AWAY, room.getUsers().get(1).getStatus());
    }

    @Test
    @DisplayName("Join-room parses private owner and operators")
    void privateRoomParses() {
        RoomData room = JoinRoomResponse.fromByteArray(joinedRoomFrame(true));

        assertTrue(room.isPrivate());
        assertEquals("owner", room.getOwner());
        assertEquals(2, room.getOperatorCount());
        assertEquals(List.of("op1", "op2"), room.getOperators());
    }

    @Test
    @DisplayName("User-joined parses complete user data and maps event args")
    void userJoinedParses() {
        UserJoinedRoomNotification notification = UserJoinedRoomNotification.fromByteArray(new MessageBuilder()
                .writeCode(MessageCode.Server.USER_JOINED_ROOM)
                .writeString("room")
                .writeString("alice")
                .writeInteger(UserPresence.ONLINE.getValue())
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
        assertEquals("alice", data.getUsername());
        assertEquals(UserPresence.ONLINE, data.getStatus());
        assertEquals(10, data.getAverageSpeed());
        assertEquals(11L, data.getUploadCount());
        assertEquals(12, data.getFileCount());
        assertEquals(13, data.getDirectoryCount());
        assertEquals(14, data.getSlotsFree());
        assertEquals("", data.getCountryCode());

        RoomJoinedEvent eventData = new RoomJoinedEvent(notification);
        assertEquals("room", eventData.getRoomName());
        assertEquals("alice", eventData.getUsername());
        assertSame(data, eventData.getUserData());
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
        assertEquals("room", eventData.getRoomName());
        assertEquals("alice", eventData.getUsername());
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
                .writeInteger(UserPresence.ONLINE.getValue())
                .writeInteger(UserPresence.AWAY.getValue())
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
