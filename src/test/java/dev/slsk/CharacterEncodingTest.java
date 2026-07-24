// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CharacterEncodingTest {
    @Test
    @DisplayName("Returns UTF-8 from UTF-8 getter")
    void returnsUtf8FromUtf8Getter() {
        CharacterEncoding encoding = CharacterEncoding.getUtf8();

        assertEquals("UTF-8", encoding.toString());
        assertSame(encoding, CharacterEncoding.getUtf8());
    }

    @Test
    @DisplayName("Returns ISO-8859-1 from ISO-8859-1 getter")
    void returnsIso88591FromIso88591Getter() {
        CharacterEncoding encoding = CharacterEncoding.getIso88591();

        assertEquals("ISO-8859-1", encoding.toString());
        assertSame(encoding, CharacterEncoding.getIso88591());
    }

    @Test
    @DisplayName("Throws given anything other than UTF-8 or ISO-8859-1")
    void throwsGivenUnsupportedEncoding() {
        assertThrows(IllegalArgumentException.class, () -> new CharacterEncoding("foo"));
    }

    @Test
    @DisplayName("Throws given null")
    void throwsGivenNull() {
        assertThrows(NullPointerException.class, () -> new CharacterEncoding(null));
    }

    @Test
    @DisplayName("Throws given an empty encoding")
    void throwsGivenEmptyEncoding() {
        assertThrows(IllegalArgumentException.class, () -> new CharacterEncoding(""));
    }
}
