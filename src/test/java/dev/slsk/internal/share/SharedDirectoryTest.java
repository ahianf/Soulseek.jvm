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

class SharedDirectoryTest {
    @Test
    @DisplayName("Instantiates with empty File list given no list")
    void instantiatesWithEmptyFileListGivenNoList() {
        SharedDirectory directory = new SharedDirectory("music");

        assertEquals("music", directory.name());
        assertTrue(directory.files().isEmpty());
        assertEquals(0, directory.fileCount());
    }

    @Test
    @DisplayName("Instantiates with given File list given list")
    void instantiatesWithGivenFileListGivenList() {
        File file = new File(1, "a", 2, "b");

        SharedDirectory directory = new SharedDirectory("music", List.of(file));

        assertEquals(1, directory.fileCount());
        assertSame(file, directory.files().getFirst());
    }

    @Test
    @DisplayName("Treats an explicit null fileList like an omitted fileList")
    void treatsNullFileListAsEmpty() {
        SharedDirectory directory = new SharedDirectory("music", null);

        assertTrue(directory.files().isEmpty());
    }

    @Test
    @DisplayName("Copies and protects the file list")
    void copiesAndProtectsFileList() {
        File file = new File(1, "a", 2, "b");
        List<File> source = new ArrayList<>(List.of(file));
        SharedDirectory directory = new SharedDirectory("music", source);

        source.clear();

        assertEquals(1, directory.fileCount());
        assertSame(file, directory.files().getFirst());
        assertThrows(
                UnsupportedOperationException.class, () -> directory.files().add(file));
    }

    @Test
    @DisplayName("Preserves a null name and rejects null file elements")
    void preservesNullNameAndRejectsNullFileElements() {
        SharedDirectory directory = new SharedDirectory(null);

        assertNull(directory.name());
        assertThrows(
                NullPointerException.class, () -> new SharedDirectory("music", java.util.Arrays.asList((File) null)));
    }
}
