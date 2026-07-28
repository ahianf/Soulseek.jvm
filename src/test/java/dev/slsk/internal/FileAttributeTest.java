// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FileAttributeTest {
    @Test
    @DisplayName("Instantiates with the given data")
    void instantiatesWithTheGivenData() {
        FileAttribute attribute = new FileAttribute(FileAttributeType.SAMPLE_RATE, 44100);

        assertEquals(FileAttributeType.SAMPLE_RATE, attribute.getType());
        assertEquals(44100, attribute.getValue());
    }

    @Test
    @DisplayName("Rejects null because the C# enum value is non-nullable")
    void rejectsNullType() {
        assertThrows(NullPointerException.class, () -> new FileAttribute(null, 1));
    }
}
