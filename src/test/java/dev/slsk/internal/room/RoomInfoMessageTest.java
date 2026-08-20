// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.room;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RoomInfoMessageTest {
    @Test
    @DisplayName("RoomInfoMessage instantiates properly")
    void instantiatesProperly() {
        List<String> users = List.of("alice", "bob");
        RoomInfoMessage info = new RoomInfoMessage("room", users);

        assertEquals("room", info.name());
        assertEquals(users.size(), info.userCount());
        assertEquals(users, info.users());
    }

    @Test
    @DisplayName("RoomInfoMessage instantiates properly with count only")
    void instantiatesProperlyWithCountOnly() {
        RoomInfoMessage info = new RoomInfoMessage("room", -3);

        assertEquals("room", info.name());
        assertEquals(-3, info.userCount());
        assertTrue(info.users().isEmpty());
    }

    @Test
    @DisplayName("RoomInfoMessage instantiates with empty user list if none is given")
    void instantiatesWithEmptyUserListIfNotGiven() {
        RoomInfoMessage info = new RoomInfoMessage("room", (List<String>) null);

        assertEquals(0, info.userCount());
        assertTrue(info.users().isEmpty());
    }

    @Test
    @DisplayName("RoomInfoMessage copies and protects the user list")
    void copiesAndProtectsUserList() {
        List<String> source = new ArrayList<>(List.of("alice"));
        RoomInfoMessage info = new RoomInfoMessage(null, source);

        source.clear();

        assertNull(info.name());
        assertEquals(List.of("alice"), info.users());
        assertThrows(UnsupportedOperationException.class, () -> info.users().add("bob"));
    }
}
