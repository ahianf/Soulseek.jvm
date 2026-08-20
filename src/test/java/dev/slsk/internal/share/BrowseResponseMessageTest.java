// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.share;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BrowseResponseMessageTest {
    @Test
    @DisplayName("BrowseResponseMessage defaults both directory lists to empty")
    void defaultsDirectoryListsToEmpty() {
        BrowseResponseMessage response = new BrowseResponseMessage();

        assertEquals(0, response.directoryCount());
        assertEquals(List.of(), response.directories());
        assertEquals(0, response.lockedDirectoryCount());
        assertEquals(List.of(), response.lockedDirectories());
    }

    @Test
    @DisplayName("BrowseResponseMessage snapshots both directory lists")
    void snapshotsDirectoryLists() {
        SharedDirectory unlocked = new SharedDirectory("open");
        SharedDirectory locked = new SharedDirectory("locked");
        List<SharedDirectory> unlockedSource = new ArrayList<>(List.of(unlocked));
        List<SharedDirectory> lockedSource = new ArrayList<>(List.of(locked));

        BrowseResponseMessage response = new BrowseResponseMessage(unlockedSource, lockedSource);
        unlockedSource.clear();
        lockedSource.clear();

        assertEquals(1, response.directoryCount());
        assertSame(unlocked, response.directories().getFirst());
        assertEquals(1, response.lockedDirectoryCount());
        assertSame(locked, response.lockedDirectories().getFirst());
        assertThrows(
                UnsupportedOperationException.class,
                () -> response.directories().clear());
        assertThrows(
                UnsupportedOperationException.class,
                () -> response.lockedDirectories().clear());
    }

    @Test
    @DisplayName("BrowseResponseMessage defaults null lists and rejects null elements")
    void supportsSourceNullSemantics() {
        BrowseResponseMessage response = new BrowseResponseMessage(null, null);

        assertEquals(0, response.directoryCount());
        assertEquals(0, response.lockedDirectoryCount());
        assertThrows(
                NullPointerException.class,
                () -> new BrowseResponseMessage(List.of(), java.util.Arrays.asList((SharedDirectory) null)));
    }

    @Test
    @DisplayName("RawBrowseResponse preserves its stream and length")
    void rawResponsePreservesData() {
        ByteArrayInputStream stream = new ByteArrayInputStream(new byte[] {1, 2});
        RawBrowseResponse response = new RawBrowseResponse(2, stream);

        assertEquals(2, response.length());
        assertSame(stream, response.stream());
    }

    @Test
    @DisplayName("RawBrowseResponse validates length before stream")
    void rawResponsePreservesValidation() {
        assertThrows(IllegalArgumentException.class, () -> new RawBrowseResponse(0, null));
        assertThrows(
                IllegalArgumentException.class, () -> new RawBrowseResponse(-1, new ByteArrayInputStream(new byte[0])));
        assertThrows(NullPointerException.class, () -> new RawBrowseResponse(1, null));
    }
}
