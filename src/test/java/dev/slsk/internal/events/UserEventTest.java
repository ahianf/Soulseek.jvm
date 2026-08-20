// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class UserEventTest {
    @Test
    @DisplayName("UserCannotConnectEvent instantiates with the given data")
    void userCannotConnectInstantiatesWithTheGivenData() {
        UserCannotConnectEvent args = new UserCannotConnectEvent(42, "alice");

        assertEquals("alice", args.username());
        assertEquals(42, args.token());
        assertTrue(args instanceof SoulseekClientEvent);
    }

    @Test
    @DisplayName("DownloadDeniedEvent instantiates with the given data")
    void downloadDeniedInstantiatesWithTheGivenData() {
        DownloadDeniedEvent args = new DownloadDeniedEvent("alice", "file.mp3", "denied");

        assertEquals("alice", args.username());
        assertEquals("file.mp3", args.filename());
        assertEquals("denied", args.message());
    }

    @Test
    @DisplayName("DownloadFailedEvent instantiates with the given data")
    void downloadFailedInstantiatesWithTheGivenData() {
        DownloadFailedEvent args = new DownloadFailedEvent("alice", "file.mp3");

        assertEquals("alice", args.username());
        assertEquals("file.mp3", args.filename());
    }

    @Test
    @DisplayName("Preserves nullable reference data")
    void preservesNullableReferenceData() {
        DownloadDeniedEvent denied = new DownloadDeniedEvent(null, null, null);
        DownloadFailedEvent failed = new DownloadFailedEvent(null, null);

        assertNull(denied.username());
        assertNull(denied.filename());
        assertNull(denied.message());
        assertNull(failed.username());
        assertNull(failed.filename());
    }
}
