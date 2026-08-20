// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.room;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RoomListMessageTest {
    @Test
    @DisplayName("RoomListMessage instantiates properly")
    void instantiatesProperly() {
        List<RoomInfoMessage> publicRooms = List.of(new RoomInfoMessage("public", 1));
        List<RoomInfoMessage> privateRooms = List.of(new RoomInfoMessage("private", 2));
        List<RoomInfoMessage> ownedRooms = List.of(new RoomInfoMessage("owned", 3));
        List<String> moderated = List.of("moderated");

        RoomListMessage list = new RoomListMessage(publicRooms, privateRooms, ownedRooms, moderated);

        assertEquals(publicRooms, list.publicRooms());
        assertEquals(1, list.publicCount());
        assertEquals(privateRooms, list.privateRooms());
        assertEquals(1, list.privateCount());
        assertEquals(ownedRooms, list.ownedRooms());
        assertEquals(1, list.ownedCount());
        assertEquals(moderated, list.moderatedRoomNames());
        assertEquals(1, list.moderatedRoomNameCount());
    }

    @Test
    @DisplayName("RoomListMessage instantiates with empty lists if not given")
    void instantiatesWithEmptyListsIfNotGiven() {
        RoomListMessage list = new RoomListMessage(null, null, null, null);

        assertTrue(list.publicRooms().isEmpty());
        assertEquals(0, list.publicCount());
        assertTrue(list.privateRooms().isEmpty());
        assertEquals(0, list.privateCount());
        assertTrue(list.ownedRooms().isEmpty());
        assertEquals(0, list.ownedCount());
        assertTrue(list.moderatedRoomNames().isEmpty());
        assertEquals(0, list.moderatedRoomNameCount());
    }

    @Test
    @DisplayName("RoomListMessage copies and protects every list")
    void copiesAndProtectsEveryList() {
        RoomInfoMessage room = new RoomInfoMessage("room", 1);
        List<RoomInfoMessage> rooms = new ArrayList<>(List.of(room));
        List<String> names = new ArrayList<>(List.of("room"));
        RoomListMessage list = new RoomListMessage(rooms, rooms, rooms, names);

        rooms.clear();
        names.clear();

        assertEquals(1, list.publicCount());
        assertEquals(1, list.privateCount());
        assertEquals(1, list.ownedCount());
        assertEquals(1, list.moderatedRoomNameCount());
        assertThrows(
                UnsupportedOperationException.class, () -> list.publicRooms().add(room));
        assertThrows(
                UnsupportedOperationException.class,
                () -> list.moderatedRoomNames().add("other"));
    }
}
