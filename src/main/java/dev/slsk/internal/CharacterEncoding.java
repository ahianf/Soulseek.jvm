// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

/**
 * A supported character encoding.
 */
public class CharacterEncoding {
    private static final String ISO_8859_1_NAME = "ISO-8859-1";
    private static final String UTF_8_NAME = "UTF-8";
    private static final CharacterEncoding ISO_8859_1 = new CharacterEncoding(ISO_8859_1_NAME);
    private static final CharacterEncoding UTF_8 = new CharacterEncoding(UTF_8_NAME);

    private final String name;

    CharacterEncoding(String name) {
        if (name == null) {
            throw new NullPointerException("name");
        }
        if (name.isEmpty()) {
            throw new IllegalArgumentException("Character encoding must not be empty");
        }
        if (!UTF_8_NAME.equals(name) && !ISO_8859_1_NAME.equals(name)) {
            throw new IllegalArgumentException("Invalid character encoding; must be one of UTF-8, ISO-8859-1");
        }

        this.name = name;
    }

    /**
     * Returns the ISO-8859-1 encoding.
     *
     * @return the ISO-8859-1 encoding
     */
    public static CharacterEncoding getIso88591() {
        return ISO_8859_1;
    }

    /**
     * Returns the UTF-8 encoding.
     *
     * @return the UTF-8 encoding
     */
    public static CharacterEncoding getUtf8() {
        return UTF_8;
    }

    /**
     * Returns the canonical encoding name.
     *
     * @return the canonical encoding name
     */
    @Override
    public String toString() {
        return name;
    }
}
