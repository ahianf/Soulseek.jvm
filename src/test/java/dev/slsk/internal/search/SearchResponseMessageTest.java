// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.internal.share.File;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SearchResponseMessageTest {
    @Test
    @DisplayName("SearchResponseMessage preserves scalar and file data")
    void preservesConstructorData() {
        File file = new File(1, "f", 2, "e");
        List<File> files = new ArrayList<>(List.of(file));
        List<File> locked = new ArrayList<>(List.of(file, file));

        SearchResponseMessage response = new SearchResponseMessage("alice", 17, true, 99, 3, files, locked);
        files.clear();
        locked.clear();

        assertEquals("alice", response.username());
        assertEquals(17, response.token());
        assertEquals(true, response.hasFreeUploadSlot());
        assertEquals(99, response.uploadSpeed());
        assertEquals(3, response.queueLength());
        assertEquals(1, response.fileCount());
        assertSame(file, response.files().getFirst());
        assertEquals(2, response.lockedFileCount());
        assertSame(file, response.lockedFiles().getFirst());
        assertThrows(UnsupportedOperationException.class, () -> response.files().clear());
        assertThrows(
                UnsupportedOperationException.class,
                () -> response.lockedFiles().clear());
    }

    @Test
    @DisplayName("SearchResponseMessage defaults null lists to empty")
    void defaultsNullListsToEmpty() {
        SearchResponseMessage response = new SearchResponseMessage(null, 0, false, 0, 0, null, null);

        assertSame(null, response.username());
        assertEquals(0, response.fileCount());
        assertEquals(List.of(), response.files());
        assertEquals(0, response.lockedFileCount());
        assertEquals(List.of(), response.lockedFiles());
    }

    @Test
    @DisplayName("SearchResponseMessage rejects null list elements")
    void rejectsNullListElements() {
        assertThrows(
                NullPointerException.class,
                () -> new SearchResponseMessage("u", 1, false, 2, 3, java.util.Arrays.asList((File) null), List.of()));
        assertThrows(
                NullPointerException.class,
                () -> new SearchResponseMessage("u", 1, false, 2, 3, List.of(), java.util.Arrays.asList((File) null)));
    }

    @Test
    @DisplayName("Internal copy replaces file lists and preserves metadata")
    void copyReplacesFilesAndPreservesMetadata() {
        SearchResponseMessage original = new SearchResponseMessage("alice", 17, true, 99, 3, null);
        File replacement = new File(1, "f", 2, "e");
        SearchResponseMessage copy = new SearchResponseMessage(original, List.of(replacement), null);

        assertEquals("alice", copy.username());
        assertEquals(17, copy.token());
        assertEquals(true, copy.hasFreeUploadSlot());
        assertEquals(99, copy.uploadSpeed());
        assertEquals(3, copy.queueLength());
        assertEquals(1, copy.fileCount());
        assertSame(replacement, copy.files().getFirst());
        assertEquals(0, copy.lockedFileCount());
    }

    @Test
    @DisplayName("RawSearchResponse preserves stream and length")
    void rawResponsePreservesData() {
        ByteArrayInputStream stream = new ByteArrayInputStream(new byte[] {1, 2});
        RawSearchResponse response = new RawSearchResponse(2, stream);

        assertEquals(2, response.length());
        assertSame(stream, response.stream());
    }

    @Test
    @DisplayName("RawSearchResponse validates length before stream")
    void rawResponsePreservesValidation() {
        assertThrows(IllegalArgumentException.class, () -> new RawSearchResponse(0, null));
        assertThrows(
                IllegalArgumentException.class, () -> new RawSearchResponse(-1, new ByteArrayInputStream(new byte[0])));
        assertThrows(NullPointerException.class, () -> new RawSearchResponse(1, null));
    }
}
