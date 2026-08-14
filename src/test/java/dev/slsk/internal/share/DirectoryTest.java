// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.share;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DirectoryTest {
    @Test
    @DisplayName("Instantiates with empty File list given no list")
    void instantiatesWithEmptyFileListGivenNoList() {
        Directory directory = new Directory("music");

        assertEquals("music", directory.getName());
        assertTrue(directory.getFiles().isEmpty());
        assertEquals(0, directory.getFileCount());
    }

    @Test
    @DisplayName("Instantiates with given File list given list")
    void instantiatesWithGivenFileListGivenList() {
        File file = new File(1, "a", 2, "b");

        Directory directory = new Directory("music", List.of(file));

        assertEquals(1, directory.getFileCount());
        assertSame(file, directory.getFiles().getFirst());
    }

    @Test
    @DisplayName("Treats an explicit null fileList like an omitted fileList")
    void treatsNullFileListAsEmpty() {
        Directory directory = new Directory("music", null);

        assertTrue(directory.getFiles().isEmpty());
    }

    @Test
    @DisplayName("Copies and protects the file list")
    void copiesAndProtectsFileList() {
        File file = new File(1, "a", 2, "b");
        List<File> source = new ArrayList<>(List.of(file));
        Directory directory = new Directory("music", source);

        source.clear();

        assertEquals(1, directory.getFileCount());
        assertSame(file, directory.getFiles().getFirst());
        assertThrows(
                UnsupportedOperationException.class, () -> directory.getFiles().add(file));
    }

    @Test
    @DisplayName("Preserves null name and null file elements")
    void preservesNullNameAndNullFileElements() {
        Directory directory = new Directory(null, java.util.Arrays.asList((File) null));

        assertNull(directory.getName());
        assertEquals(1, directory.getFileCount());
        assertNull(directory.getFiles().getFirst());
    }
}
