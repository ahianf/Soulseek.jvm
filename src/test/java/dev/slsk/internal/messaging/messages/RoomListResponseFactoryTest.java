// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.exceptions.MessageException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.internal.RoomInfo;
import dev.slsk.internal.RoomList;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RoomListResponseFactoryTest {
    @Test
    @DisplayName("Room list parses every category in protocol order")
    void parsesRoomCategories() {
        MessageBuilder builder = new MessageBuilder().writeCode(MessageCode.Server.ROOM_LIST);
        writeRooms(builder, List.of(new RoomInfo("public-a", 12), new RoomInfo("public-b", 34)));
        writeRooms(builder, List.of(new RoomInfo("owned", 56)));
        writeRooms(builder, List.of(new RoomInfo("private", 78)));
        builder.writeInteger(2).writeString("moderated-a").writeString("moderated-b");

        RoomList result = RoomListResponseFactory.fromByteArray(builder.build());
        assertRooms(List.of(new ExpectedRoom("public-a", 12), new ExpectedRoom("public-b", 34)), result.getPublic());
        assertRooms(List.of(new ExpectedRoom("private", 78)), result.getPrivate());
        assertRooms(List.of(new ExpectedRoom("owned", 56)), result.getOwned());
        assertEquals(List.of("moderated-a", "moderated-b"), result.getModeratedRoomNames());
        assertEquals(2, result.getPublicCount());
        assertEquals(1, result.getPrivateCount());
        assertEquals(1, result.getOwnedCount());
        assertEquals(2, result.getModeratedRoomNameCount());
    }

    @Test
    @DisplayName("Room list parses four empty categories")
    void parsesEmptyRoomList() {
        RoomList result = RoomListResponseFactory.fromByteArray(new MessageBuilder()
                .writeCode(MessageCode.Server.ROOM_LIST)
                .writeInteger(0)
                .writeInteger(0)
                .writeInteger(0)
                .writeInteger(0)
                .writeInteger(0)
                .writeInteger(0)
                .writeInteger(0)
                .build());

        assertEquals(List.of(), result.getPublic());
        assertEquals(List.of(), result.getOwned());
        assertEquals(List.of(), result.getPrivate());
        assertEquals(List.of(), result.getModeratedRoomNames());
    }

    @Test
    @DisplayName("Room list rejects mismatches and missing data")
    void rejectsInvalidFrames() {
        assertThrows(
                MessageException.class,
                () -> RoomListResponseFactory.fromByteArray(new MessageBuilder()
                        .writeCode(MessageCode.Peer.BROWSE_REQUEST)
                        .build()));
        assertThrows(
                MessageReadException.class,
                () -> RoomListResponseFactory.fromByteArray(new MessageBuilder()
                        .writeCode(MessageCode.Server.ROOM_LIST)
                        .writeInteger(1)
                        .build()));
    }

    @Test
    @DisplayName("Room list preserves source count-alignment failure")
    void rejectsUnalignedNameAndCountArrays() {
        byte[] message = new MessageBuilder()
                .writeCode(MessageCode.Server.ROOM_LIST)
                .writeInteger(0)
                .writeInteger(1)
                .writeInteger(12)
                .build();

        assertThrows(IndexOutOfBoundsException.class, () -> RoomListResponseFactory.fromByteArray(message));
    }

    private static void writeRooms(MessageBuilder builder, List<RoomInfo> rooms) {
        builder.writeInteger(rooms.size());
        for (RoomInfo room : rooms) {
            builder.writeString(room.getName());
        }
        builder.writeInteger(rooms.size());
        for (RoomInfo room : rooms) {
            builder.writeInteger(room.getUserCount());
        }
    }

    private static void assertRooms(List<ExpectedRoom> expected, List<RoomInfo> actual) {
        assertEquals(expected.size(), actual.size());
        for (int index = 0; index < expected.size(); index++) {
            assertEquals(expected.get(index).name(), actual.get(index).getName());
            assertEquals(expected.get(index).userCount(), actual.get(index).getUserCount());
        }
    }

    private record ExpectedRoom(String name, int userCount) {}
}
