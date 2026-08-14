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

class SearchResponseTest {
    @Test
    @DisplayName("SearchResponse preserves scalar and file data")
    void preservesConstructorData() {
        File file = new File(1, "f", 2, "e");
        List<File> files = new ArrayList<>(List.of(file));
        List<File> locked = new ArrayList<>(List.of(file, file));

        SearchResponse response = new SearchResponse("alice", 17, true, 99, 3, files, locked);
        files.clear();
        locked.clear();

        assertEquals("alice", response.getUsername());
        assertEquals(17, response.getToken());
        assertEquals(true, response.hasFreeUploadSlot());
        assertEquals(99, response.getUploadSpeed());
        assertEquals(3, response.getQueueLength());
        assertEquals(1, response.getFileCount());
        assertSame(file, response.getFiles().getFirst());
        assertEquals(2, response.getLockedFileCount());
        assertSame(file, response.getLockedFiles().getFirst());
        assertThrows(
                UnsupportedOperationException.class, () -> response.getFiles().clear());
        assertThrows(
                UnsupportedOperationException.class,
                () -> response.getLockedFiles().clear());
    }

    @Test
    @DisplayName("SearchResponse defaults null lists to empty")
    void defaultsNullListsToEmpty() {
        SearchResponse response = new SearchResponse(null, 0, false, 0, 0, null, null);

        assertSame(null, response.getUsername());
        assertEquals(0, response.getFileCount());
        assertEquals(List.of(), response.getFiles());
        assertEquals(0, response.getLockedFileCount());
        assertEquals(List.of(), response.getLockedFiles());
    }

    @Test
    @DisplayName("SearchResponse retains null list elements")
    void retainsNullListElements() {
        SearchResponse response = new SearchResponse(
                "u", 1, false, 2, 3, java.util.Arrays.asList((File) null), java.util.Arrays.asList((File) null));

        assertEquals(1, response.getFileCount());
        assertSame(null, response.getFiles().getFirst());
        assertEquals(1, response.getLockedFileCount());
        assertSame(null, response.getLockedFiles().getFirst());
    }

    @Test
    @DisplayName("Internal copy replaces file lists and preserves metadata")
    void copyReplacesFilesAndPreservesMetadata() {
        SearchResponse original = new SearchResponse("alice", 17, true, 99, 3, null);
        File replacement = new File(1, "f", 2, "e");
        SearchResponse copy = new SearchResponse(original, List.of(replacement), null);

        assertEquals("alice", copy.getUsername());
        assertEquals(17, copy.getToken());
        assertEquals(true, copy.hasFreeUploadSlot());
        assertEquals(99, copy.getUploadSpeed());
        assertEquals(3, copy.getQueueLength());
        assertEquals(1, copy.getFileCount());
        assertSame(replacement, copy.getFiles().getFirst());
        assertEquals(0, copy.getLockedFileCount());
    }

    @Test
    @DisplayName("RawSearchResponse preserves stream and base defaults")
    void rawResponsePreservesData() {
        ByteArrayInputStream stream = new ByteArrayInputStream(new byte[] {1, 2});
        RawSearchResponse response = new RawSearchResponse(2, stream);

        assertEquals(2, response.getLength());
        assertSame(stream, response.getStream());
        assertEquals("", response.getUsername());
        assertEquals(0, response.getToken());
        assertEquals(0, response.getFileCount());
        assertEquals(0, response.getLockedFileCount());
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
