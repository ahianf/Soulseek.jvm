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

class RoomListTest {
    @Test
    @DisplayName("RoomList instantiates properly")
    void instantiatesProperly() {
        List<RoomInfo> publicRooms = List.of(new RoomInfo("public", 1));
        List<RoomInfo> privateRooms = List.of(new RoomInfo("private", 2));
        List<RoomInfo> ownedRooms = List.of(new RoomInfo("owned", 3));
        List<String> moderated = List.of("moderated");

        RoomList list = new RoomList(publicRooms, privateRooms, ownedRooms, moderated);

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
    @DisplayName("RoomList instantiates with empty lists if not given")
    void instantiatesWithEmptyListsIfNotGiven() {
        RoomList list = new RoomList(null, null, null, null);

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
    @DisplayName("RoomList copies and protects every list")
    void copiesAndProtectsEveryList() {
        RoomInfo room = new RoomInfo("room", 1);
        List<RoomInfo> rooms = new ArrayList<>(List.of(room));
        List<String> names = new ArrayList<>(List.of("room"));
        RoomList list = new RoomList(rooms, rooms, rooms, names);

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
