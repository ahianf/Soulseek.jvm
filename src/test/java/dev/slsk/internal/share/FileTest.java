// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.share;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FileTest {
    @Test
    @DisplayName("Instantiates with the given data")
    void instantiatesWithTheGivenData() {
        List<FileAttribute> attributes = List.of(
                new FileAttribute(FileAttributeType.BIT_RATE, 320), new FileAttribute(FileAttributeType.LENGTH, 123));

        File file = new File(7, "music/file.mp3", 45_678L, "mp3", attributes);

        assertEquals(7, file.code());
        assertEquals("music/file.mp3", file.filename());
        assertEquals(45_678L, file.size());
        assertEquals("mp3", file.extension());
        assertEquals(2, file.attributeCount());
        assertEquals(attributes, file.attributes());
    }

    @Test
    @DisplayName("Instantiates with empty Attributes given no attributeList")
    void instantiatesWithEmptyAttributesGivenNoAttributeList() {
        File file = new File(1, "a", 2, "b");

        assertTrue(file.attributes().isEmpty());
        assertEquals(0, file.attributeCount());
    }

    @Test
    @DisplayName("Treats an explicit null attributeList like an omitted attributeList")
    void treatsNullAttributeListAsEmpty() {
        File file = new File(1, "a", 2, "b", null);

        assertTrue(file.attributes().isEmpty());
    }

    @Test
    @DisplayName("Copies and protects the attribute list")
    void copiesAndProtectsAttributeList() {
        FileAttribute attribute = new FileAttribute(FileAttributeType.BIT_RATE, 128);
        List<FileAttribute> source = new ArrayList<>(List.of(attribute));
        File file = new File(1, "a", 2, "b", source);

        source.clear();

        assertEquals(1, file.attributeCount());
        assertSame(attribute, file.attributes().getFirst());
        assertThrows(
                UnsupportedOperationException.class, () -> file.attributes().add(attribute));
    }

    @Test
    @DisplayName("Derived attributes return matching values")
    void derivedAttributesReturnMatchingValues() {
        File file = new File(
                1,
                "a",
                2,
                "b",
                List.of(
                        new FileAttribute(FileAttributeType.BIT_DEPTH, 24),
                        new FileAttribute(FileAttributeType.BIT_RATE, 320),
                        new FileAttribute(FileAttributeType.SAMPLE_RATE, 96),
                        new FileAttribute(FileAttributeType.LENGTH, 180)));

        assertEquals(24, file.bitDepth());
        assertEquals(320, file.bitRate());
        assertEquals(96, file.sampleRate());
        assertEquals(180, file.length());
    }

    @Test
    @DisplayName("Derived attributes return null when no value")
    void derivedAttributesReturnNullWhenNoValue() {
        File file = new File(1, "a", 2, "b");

        assertNull(file.bitDepth());
        assertNull(file.bitRate());
        assertNull(file.sampleRate());
        assertNull(file.length());
        assertNull(file.variableBitRate());
    }

    @Test
    @DisplayName("IsVariableBitRate returns true when attribute is nonzero")
    void variableBitRateReturnsTrueWhenAttributeIsNonzero() {
        File file = new File(1, "a", 2, "b", List.of(new FileAttribute(FileAttributeType.VARIABLE_BIT_RATE, -1)));

        assertTrue(file.variableBitRate());
    }

    @Test
    @DisplayName("IsVariableBitRate returns false when attribute is zero")
    void variableBitRateReturnsFalseWhenAttributeIsZero() {
        File file = new File(1, "a", 2, "b", List.of(new FileAttribute(FileAttributeType.VARIABLE_BIT_RATE, 0)));

        assertFalse(file.variableBitRate());
    }

    @Test
    @DisplayName("The last duplicate attribute determines the computed value")
    void lastDuplicateAttributeDeterminesComputedValue() {
        File file = new File(
                1,
                "a",
                2,
                "b",
                List.of(
                        new FileAttribute(FileAttributeType.BIT_RATE, 128),
                        new FileAttribute(FileAttributeType.BIT_RATE, 256)));

        assertEquals(256, file.bitRate());
        assertEquals(2, file.attributeCount());
    }

    @Test
    @DisplayName("Preserves null filename and extension")
    void preservesNullFilenameAndExtension() {
        File file = new File(1, null, 2, null);

        assertNull(file.filename());
        assertNull(file.extension());
    }

    @Test
    @DisplayName("Rejects a null attribute element while computing properties")
    void rejectsNullAttributeElement() {
        assertThrows(
                NullPointerException.class,
                () -> new File(1, "a", 2, "b", java.util.Arrays.asList((FileAttribute) null)));
    }
}
