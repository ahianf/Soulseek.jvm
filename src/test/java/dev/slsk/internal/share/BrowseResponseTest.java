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

class BrowseResponseTest {
    @Test
    @DisplayName("BrowseResponse defaults both directory lists to empty")
    void defaultsDirectoryListsToEmpty() {
        BrowseResponse response = new BrowseResponse();

        assertEquals(0, response.getDirectoryCount());
        assertEquals(List.of(), response.getDirectories());
        assertEquals(0, response.getLockedDirectoryCount());
        assertEquals(List.of(), response.getLockedDirectories());
    }

    @Test
    @DisplayName("BrowseResponse snapshots both directory lists")
    void snapshotsDirectoryLists() {
        Directory unlocked = new Directory("open");
        Directory locked = new Directory("locked");
        List<Directory> unlockedSource = new ArrayList<>(List.of(unlocked));
        List<Directory> lockedSource = new ArrayList<>(List.of(locked));

        BrowseResponse response = new BrowseResponse(unlockedSource, lockedSource);
        unlockedSource.clear();
        lockedSource.clear();

        assertEquals(1, response.getDirectoryCount());
        assertSame(unlocked, response.getDirectories().getFirst());
        assertEquals(1, response.getLockedDirectoryCount());
        assertSame(locked, response.getLockedDirectories().getFirst());
        assertThrows(
                UnsupportedOperationException.class,
                () -> response.getDirectories().clear());
        assertThrows(
                UnsupportedOperationException.class,
                () -> response.getLockedDirectories().clear());
    }

    @Test
    @DisplayName("BrowseResponse supports null lists and null elements")
    void supportsSourceNullSemantics() {
        BrowseResponse response = new BrowseResponse(null, java.util.Arrays.asList((Directory) null));

        assertEquals(0, response.getDirectoryCount());
        assertEquals(1, response.getLockedDirectoryCount());
        assertSame(null, response.getLockedDirectories().getFirst());
    }

    @Test
    @DisplayName("RawBrowseResponse preserves its stream and length")
    void rawResponsePreservesData() {
        ByteArrayInputStream stream = new ByteArrayInputStream(new byte[] {1, 2});
        RawBrowseResponse response = new RawBrowseResponse(2, stream);

        assertEquals(2, response.getLength());
        assertSame(stream, response.getStream());
        assertEquals(0, response.getDirectoryCount());
        assertEquals(0, response.getLockedDirectoryCount());
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
