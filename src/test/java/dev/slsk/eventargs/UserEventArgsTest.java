// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class UserEventArgsTest {
    @Test
    @DisplayName("UserCannotConnectEventArgs instantiates with the given data")
    void userCannotConnectInstantiatesWithTheGivenData() {
        UserCannotConnectEventArgs args = new UserCannotConnectEventArgs(42, "alice");

        assertEquals("alice", args.getUsername());
        assertEquals(42, args.getToken());
        assertTrue(args instanceof SoulseekClientEventArgs);
    }

    @Test
    @DisplayName("DownloadDeniedEventArgs instantiates with the given data")
    void downloadDeniedInstantiatesWithTheGivenData() {
        DownloadDeniedEventArgs args = new DownloadDeniedEventArgs("alice", "file.mp3", "denied");

        assertEquals("alice", args.getUsername());
        assertEquals("file.mp3", args.getFilename());
        assertEquals("denied", args.getMessage());
    }

    @Test
    @DisplayName("DownloadFailedEventArgs instantiates with the given data")
    void downloadFailedInstantiatesWithTheGivenData() {
        DownloadFailedEventArgs args = new DownloadFailedEventArgs("alice", "file.mp3");

        assertEquals("alice", args.getUsername());
        assertEquals("file.mp3", args.getFilename());
    }

    @Test
    @DisplayName("Preserves nullable reference data")
    void preservesNullableReferenceData() {
        DownloadDeniedEventArgs denied = new DownloadDeniedEventArgs(null, null, null);
        DownloadFailedEventArgs failed = new DownloadFailedEventArgs(null, null);

        assertNull(denied.getUsername());
        assertNull(denied.getFilename());
        assertNull(denied.getMessage());
        assertNull(failed.getUsername());
        assertNull(failed.getFilename());
    }
}
