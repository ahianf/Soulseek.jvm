// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.user;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class UserModelsTest {
    @Test
    @DisplayName("UserData instantiates with the given data")
    void userDataInstantiatesWithTheGivenData() {
        UserData data = new UserData("alice", WireUserPresence.AWAY, 123, 456L, 7, 8, "CL", 2);

        assertEquals("alice", data.username());
        assertEquals(WireUserPresence.AWAY, data.status());
        assertEquals(123, data.averageSpeed());
        assertEquals(456L, data.uploadCount());
        assertEquals(7, data.fileCount());
        assertEquals(8, data.directoryCount());
        assertEquals("CL", data.countryCode());
        assertEquals(2, data.slotsFree());
    }

    @Test
    @DisplayName("UserData defaults SlotsFree to null")
    void userDataDefaultsSlotsFreeToNull() {
        UserData data = new UserData("alice", WireUserPresence.ONLINE, 1, 2, 3, 4, null);

        assertNull(data.slotsFree());
        assertNull(data.countryCode());
    }

    @Test
    @DisplayName("UserData rejects null status because the C# enum is non-nullable")
    void userDataRejectsNullStatus() {
        assertThrows(NullPointerException.class, () -> new UserData("alice", null, 1, 2, 3, 4, "CL"));
    }

    @Test
    @DisplayName("UserInfoMessage instantiates with the given data")
    void userInfoInstantiatesWithTheGivenData() {
        byte[] picture = {1, 2, 3};
        UserInfoMessage info = new UserInfoMessage("description", 4, 5, true, picture);

        assertEquals("description", info.description());
        assertEquals(4, info.uploadSlots());
        assertEquals(5, info.queueLength());
        assertTrue(info.freeUploadSlot());
        assertTrue(info.hasPicture());
        assertArrayEquals(picture, info.picture());
        assertNotSame(picture, info.picture());
    }

    @Test
    @DisplayName("UserInfoMessage snapshots and protects picture data")
    void userInfoSnapshotsAndProtectsPictureData() {
        byte[] picture = {1};
        UserInfoMessage info = new UserInfoMessage(null, 0, 0, false, picture);

        picture[0] = 9;
        byte[] returned = info.picture();
        returned[0] = 8;

        assertEquals(1, info.picture()[0]);
        assertFalse(info.freeUploadSlot());
    }

    @Test
    @DisplayName("UserInfoMessage defaults Picture to null")
    void userInfoDefaultsPictureToNull() {
        UserInfoMessage info = new UserInfoMessage("description", 4, 5, false);

        assertFalse(info.hasPicture());
        assertNull(info.picture());
    }

    @Test
    @DisplayName("UserInfoMessage treats an empty picture as configured")
    void userInfoTreatsEmptyPictureAsConfigured() {
        UserInfoMessage info = new UserInfoMessage("description", 4, 5, false, new byte[0]);

        assertTrue(info.hasPicture());
    }

    @Test
    @DisplayName("UserStatisticsSnapshot instantiates with the given data")
    void userStatisticsInstantiatesWithTheGivenData() {
        UserStatisticsSnapshot statistics = new UserStatisticsSnapshot(null, -1, Long.MAX_VALUE, -2, -3);

        assertNull(statistics.username());
        assertEquals(-1, statistics.averageSpeed());
        assertEquals(Long.MAX_VALUE, statistics.uploadCount());
        assertEquals(-2, statistics.fileCount());
        assertEquals(-3, statistics.directoryCount());
    }

    @Test
    @DisplayName("UserStatusSnapshot instantiates with the given data")
    void userStatusInstantiatesWithTheGivenData() {
        UserStatusSnapshot status = new UserStatusSnapshot(null, WireUserPresence.OFFLINE, true);

        assertNull(status.username());
        assertEquals(WireUserPresence.OFFLINE, status.presence());
        assertTrue(status.privileged());
    }

    @Test
    @DisplayName("UserStatusSnapshot rejects null presence because the C# enum is non-nullable")
    void userStatusRejectsNullPresence() {
        assertThrows(NullPointerException.class, () -> new UserStatusSnapshot("alice", null, false));
    }
}
