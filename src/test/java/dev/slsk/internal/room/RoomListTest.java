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

        assertEquals(publicRooms, list.getPublic());
        assertEquals(1, list.getPublicCount());
        assertEquals(privateRooms, list.getPrivate());
        assertEquals(1, list.getPrivateCount());
        assertEquals(ownedRooms, list.getOwned());
        assertEquals(1, list.getOwnedCount());
        assertEquals(moderated, list.getModeratedRoomNames());
        assertEquals(1, list.getModeratedRoomNameCount());
    }

    @Test
    @DisplayName("RoomList instantiates with empty lists if not given")
    void instantiatesWithEmptyListsIfNotGiven() {
        RoomList list = new RoomList(null, null, null, null);

        assertTrue(list.getPublic().isEmpty());
        assertEquals(0, list.getPublicCount());
        assertTrue(list.getPrivate().isEmpty());
        assertEquals(0, list.getPrivateCount());
        assertTrue(list.getOwned().isEmpty());
        assertEquals(0, list.getOwnedCount());
        assertTrue(list.getModeratedRoomNames().isEmpty());
        assertEquals(0, list.getModeratedRoomNameCount());
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

        assertEquals(1, list.getPublicCount());
        assertEquals(1, list.getPrivateCount());
        assertEquals(1, list.getOwnedCount());
        assertEquals(1, list.getModeratedRoomNameCount());
        assertThrows(UnsupportedOperationException.class, () -> list.getPublic().add(room));
        assertThrows(
                UnsupportedOperationException.class,
                () -> list.getModeratedRoomNames().add("other"));
    }
}
