// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.room;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.user.UserData;
import dev.slsk.internal.user.UserPresence;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RoomDataTest {
    @Test
    @DisplayName("RoomData users uses empty list if one is omitted")
    void usersUsesEmptyListIfOneIsOmitted() {
        RoomData data = new RoomData("room", null);

        assertTrue(data.getUsers().isEmpty());
        assertEquals(0, data.getUserCount());
        assertFalse(data.isPrivate());
        assertNull(data.getOwner());
    }

    @Test
    @DisplayName("RoomData operators uses null list if one is omitted")
    void operatorsUsesNullListIfOneIsOmitted() {
        RoomData data = new RoomData("room", null, false, null, null);

        assertNull(data.getOperators());
        assertNull(data.getOperatorCount());
    }

    @Test
    @DisplayName("RoomData instantiates private-room data")
    void instantiatesPrivateRoomData() {
        UserData user = new UserData("alice", UserPresence.ONLINE, 1, 2, 3, 4, "CL");
        RoomData data = new RoomData("room", List.of(user), true, "owner", List.of("operator"));

        assertEquals("room", data.getName());
        assertTrue(data.isPrivate());
        assertEquals("owner", data.getOwner());
        assertEquals(1, data.getUserCount());
        assertSame(user, data.getUsers().getFirst());
        assertEquals(1, data.getOperatorCount());
        assertEquals(List.of("operator"), data.getOperators());
    }

    @Test
    @DisplayName("RoomData copies and protects supplied lists")
    void copiesAndProtectsSuppliedLists() {
        UserData user = new UserData("alice", UserPresence.ONLINE, 1, 2, 3, 4, "CL");
        List<UserData> users = new ArrayList<>(List.of(user));
        List<String> operators = new ArrayList<>(List.of("operator"));
        RoomData data = new RoomData(null, users, true, null, operators);

        users.clear();
        operators.clear();

        assertNull(data.getName());
        assertEquals(1, data.getUserCount());
        assertEquals(1, data.getOperatorCount());
        assertThrows(UnsupportedOperationException.class, () -> data.getUsers().add(user));
        assertThrows(
                UnsupportedOperationException.class, () -> data.getOperators().add("other"));
    }
}
