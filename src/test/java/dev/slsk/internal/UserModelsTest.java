// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class UserModelsTest {
    @Test
    @DisplayName("UserData instantiates with the given data")
    void userDataInstantiatesWithTheGivenData() {
        UserData data = new UserData("alice", UserPresence.AWAY, 123, 456L, 7, 8, "CL", 2);

        assertEquals("alice", data.getUsername());
        assertEquals(UserPresence.AWAY, data.getStatus());
        assertEquals(123, data.getAverageSpeed());
        assertEquals(456L, data.getUploadCount());
        assertEquals(7, data.getFileCount());
        assertEquals(8, data.getDirectoryCount());
        assertEquals("CL", data.getCountryCode());
        assertEquals(2, data.getSlotsFree());
    }

    @Test
    @DisplayName("UserData defaults SlotsFree to null")
    void userDataDefaultsSlotsFreeToNull() {
        UserData data = new UserData("alice", UserPresence.ONLINE, 1, 2, 3, 4, null);

        assertNull(data.getSlotsFree());
        assertNull(data.getCountryCode());
    }

    @Test
    @DisplayName("UserData rejects null status because the C# enum is non-nullable")
    void userDataRejectsNullStatus() {
        assertThrows(NullPointerException.class, () -> new UserData("alice", null, 1, 2, 3, 4, "CL"));
    }

    @Test
    @DisplayName("UserInfo instantiates with the given data")
    void userInfoInstantiatesWithTheGivenData() {
        byte[] picture = {1, 2, 3};
        UserInfo info = new UserInfo("description", 4, 5, true, picture);

        assertEquals("description", info.getDescription());
        assertEquals(4, info.getUploadSlots());
        assertEquals(5, info.getQueueLength());
        assertTrue(info.hasFreeUploadSlot());
        assertTrue(info.hasPicture());
        assertSame(picture, info.getPicture());
    }

    @Test
    @DisplayName("UserInfo preserves the source array alias")
    void userInfoPreservesSourceArrayAlias() {
        byte[] picture = {1};
        UserInfo info = new UserInfo(null, 0, 0, false, picture);

        picture[0] = 9;

        assertEquals(9, info.getPicture()[0]);
        assertFalse(info.hasFreeUploadSlot());
    }

    @Test
    @DisplayName("UserInfo defaults Picture to null")
    void userInfoDefaultsPictureToNull() {
        UserInfo info = new UserInfo("description", 4, 5, false);

        assertFalse(info.hasPicture());
        assertNull(info.getPicture());
    }

    @Test
    @DisplayName("UserInfo treats an empty picture as configured")
    void userInfoTreatsEmptyPictureAsConfigured() {
        UserInfo info = new UserInfo("description", 4, 5, false, new byte[0]);

        assertTrue(info.hasPicture());
    }

    @Test
    @DisplayName("UserStatistics instantiates with the given data")
    void userStatisticsInstantiatesWithTheGivenData() {
        UserStatistics statistics = new UserStatistics(null, -1, Long.MAX_VALUE, -2, -3);

        assertNull(statistics.getUsername());
        assertEquals(-1, statistics.getAverageSpeed());
        assertEquals(Long.MAX_VALUE, statistics.getUploadCount());
        assertEquals(-2, statistics.getFileCount());
        assertEquals(-3, statistics.getDirectoryCount());
    }

    @Test
    @DisplayName("UserStatus instantiates with the given data")
    void userStatusInstantiatesWithTheGivenData() {
        UserStatus status = new UserStatus(null, UserPresence.OFFLINE, true);

        assertNull(status.getUsername());
        assertEquals(UserPresence.OFFLINE, status.getPresence());
        assertTrue(status.isPrivileged());
    }

    @Test
    @DisplayName("UserStatus rejects null presence because the C# enum is non-nullable")
    void userStatusRejectsNullPresence() {
        assertThrows(NullPointerException.class, () -> new UserStatus("alice", null, false));
    }
}
