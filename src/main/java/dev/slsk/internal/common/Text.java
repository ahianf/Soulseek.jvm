// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

/** Internal text classification and validation. */
public final class Text {
    private Text() {}

    /**
     * Reports whether a value is null, empty, or consists only of whitespace.
     *
     * <p>{@code String.isBlank()} classifies fewer code points as whitespace.
     * Testing {@code Character.isWhitespace} together with {@code
     * Character.isSpaceChar} also covers non-breaking separators.
     *
     * @param value the value to test
     * @return {@code true} when the value is null, empty, or entirely whitespace
     */
    public static boolean isNullOrUnicodeWhitespace(String value) {
        return value == null
                || value.isEmpty()
                || value.codePoints()
                        .allMatch(codePoint -> Character.isWhitespace(codePoint) || Character.isSpaceChar(codePoint));
    }

    /**
     * Requires a value that is not null, empty, or entirely whitespace.
     *
     * @param value the value to check
     * @param name the argument name, for the message
     * @throws IllegalArgumentException when the value is blank
     */
    public static void requireText(String value, String name) {
        if (isNullOrUnicodeWhitespace(value)) {
            throw new IllegalArgumentException(name + " must not be null, empty, or whitespace");
        }
    }

    /**
     * Requires a value that is neither null nor empty.
     *
     * <p>Deliberately weaker than {@link #requireText}: a password of spaces is
     * a password, and a message of spaces is a message.
     *
     * @param value the value to check
     * @param name the argument name, for the message
     * @throws IllegalArgumentException when the value is null or empty
     */
    public static void requireNonEmpty(String value, String name) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be null or empty");
        }
    }
}
