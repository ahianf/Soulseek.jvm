// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.exceptions.MessageException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.room.RoomInfoMessage;
import dev.slsk.internal.room.RoomListMessage;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RoomListResponseCodecTest {
    @Test
    @DisplayName("Room list parses every category in protocol order")
    void parsesRoomCategories() {
        MessageBuilder builder = new MessageBuilder().writeCode(MessageCode.Server.ROOM_LIST);
        writeRooms(builder, List.of(new RoomInfoMessage("public-a", 12), new RoomInfoMessage("public-b", 34)));
        writeRooms(builder, List.of(new RoomInfoMessage("owned", 56)));
        writeRooms(builder, List.of(new RoomInfoMessage("private", 78)));
        builder.writeInteger(2).writeString("moderated-a").writeString("moderated-b");

        RoomListMessage result = RoomListResponseCodec.fromByteArray(builder.build());
        assertRooms(List.of(new ExpectedRoom("public-a", 12), new ExpectedRoom("public-b", 34)), result.publicRooms());
        assertRooms(List.of(new ExpectedRoom("private", 78)), result.privateRooms());
        assertRooms(List.of(new ExpectedRoom("owned", 56)), result.ownedRooms());
        assertEquals(List.of("moderated-a", "moderated-b"), result.moderatedRoomNames());
        assertEquals(2, result.publicCount());
        assertEquals(1, result.privateCount());
        assertEquals(1, result.ownedCount());
        assertEquals(2, result.moderatedRoomNameCount());
    }

    @Test
    @DisplayName("Room list parses four empty categories")
    void parsesEmptyRoomList() {
        RoomListMessage result = RoomListResponseCodec.fromByteArray(new MessageBuilder()
                .writeCode(MessageCode.Server.ROOM_LIST)
                .writeInteger(0)
                .writeInteger(0)
                .writeInteger(0)
                .writeInteger(0)
                .writeInteger(0)
                .writeInteger(0)
                .writeInteger(0)
                .build());

        assertEquals(List.of(), result.publicRooms());
        assertEquals(List.of(), result.ownedRooms());
        assertEquals(List.of(), result.privateRooms());
        assertEquals(List.of(), result.moderatedRoomNames());
    }

    @Test
    @DisplayName("Room list rejects mismatches and missing data")
    void rejectsInvalidFrames() {
        assertThrows(
                MessageException.class,
                () -> RoomListResponseCodec.fromByteArray(new MessageBuilder()
                        .writeCode(MessageCode.Peer.BROWSE_REQUEST)
                        .build()));
        assertThrows(
                MessageReadException.class,
                () -> RoomListResponseCodec.fromByteArray(new MessageBuilder()
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

        assertThrows(IndexOutOfBoundsException.class, () -> RoomListResponseCodec.fromByteArray(message));
    }

    private static void writeRooms(MessageBuilder builder, List<RoomInfoMessage> rooms) {
        builder.writeInteger(rooms.size());
        for (RoomInfoMessage room : rooms) {
            builder.writeString(room.name());
        }
        builder.writeInteger(rooms.size());
        for (RoomInfoMessage room : rooms) {
            builder.writeInteger(room.userCount());
        }
    }

    private static void assertRooms(List<ExpectedRoom> expected, List<RoomInfoMessage> actual) {
        assertEquals(expected.size(), actual.size());
        for (int index = 0; index < expected.size(); index++) {
            assertEquals(expected.get(index).name(), actual.get(index).name());
            assertEquals(expected.get(index).userCount(), actual.get(index).userCount());
        }
    }

    private record ExpectedRoom(String name, int userCount) {}
}
