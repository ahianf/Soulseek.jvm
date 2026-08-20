// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TextTest {
    @Test
    @DisplayName("Unicode whitespace includes non-breaking separators")
    void isNullOrUnicodeWhitespaceIncludesNonBreakingSeparators() {
        assertTrue(Text.isNullOrUnicodeWhitespace(null));
        assertTrue(Text.isNullOrUnicodeWhitespace(""));
        assertTrue(Text.isNullOrUnicodeWhitespace(" "));
        assertTrue(Text.isNullOrUnicodeWhitespace("\t\r\n"));

        // Separators above U+0020 that String.trim() leaves in place and that the source still rejects.
        assertTrue(Text.isNullOrUnicodeWhitespace("\u00A0"), "no-break space");
        assertTrue(Text.isNullOrUnicodeWhitespace("\u2003"), "em space");
        assertTrue(Text.isNullOrUnicodeWhitespace("\u2007"), "figure space");
        assertTrue(Text.isNullOrUnicodeWhitespace("\u202F"), "narrow no-break space");
        assertTrue(Text.isNullOrUnicodeWhitespace("\u3000"), "ideographic space");
        assertTrue(Text.isNullOrUnicodeWhitespace(" \u2003\t"), "mixed separators");

        assertFalse(Text.isNullOrUnicodeWhitespace("a"));
        assertFalse(Text.isNullOrUnicodeWhitespace(" a "));
        assertFalse(Text.isNullOrUnicodeWhitespace("\u2003a"));
    }

    @Test
    @DisplayName("Text requirements distinguish blank from empty")
    void requirementsDistinguishBlankFromEmpty() {
        Text.requireText("value", "name");
        Text.requireNonEmpty(" ", "name");
        assertThrows(IllegalArgumentException.class, () -> Text.requireText(" ", "name"));
        assertThrows(IllegalArgumentException.class, () -> Text.requireNonEmpty("", "name"));
    }
}
