// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class HashesTest {
    @Test
    @DisplayName("MD5 encoding matches Soulseek login vectors")
    void md5HexMatchesVectors() {
        assertEquals("d41d8cd98f00b204e9800998ecf8427e", Hashes.md5Hex(""));
        assertEquals("5d41402abc4b2a76b9719d911017c592", Hashes.md5Hex("hello"));
        assertEquals("45bfb2ac344e0fee8b89047858fae25a", Hashes.md5Hex("påsswörd"));
        assertThrows(NullPointerException.class, () -> Hashes.md5Hex(null));
    }
}
