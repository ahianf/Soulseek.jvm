// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.search;

import java.util.Objects;

/**
 * Identifies one search for as long as the library remembers it.
 *
 * <p>A search is addressed by id rather than by holding the search object,
 * because the consumer this API is designed for projects everything through HTTP
 * and SSE and can only hold values that survive being turned into JSON and
 * handed back later. An id does; a live object does not.
 *
 * <p>The value is the string form of the protocol token the search was issued
 * with. Keeping the two aligned means a response arriving off the wire maps back
 * to a search without a side table, and a token that shows up in a diagnostic
 * log can be matched against what the consumer is holding. The consumer should
 * still treat it as opaque — that it is a number today is an implementation
 * detail of how searches are correlated.
 *
 * @param value the identifier
 */
public record SearchId(String value) {

    /**
     * Validates and returns the identifier.
     *
     * @throws NullPointerException if {@code value} is {@code null}
     * @throws IllegalArgumentException if {@code value} is blank
     */
    public SearchId {
        Objects.requireNonNull(value, "value");
        if (value.isBlank()) {
            throw new IllegalArgumentException("search id must not be blank");
        }
    }

    /**
     * Returns the identifier for {@code value}.
     *
     * @param value the identifier
     * @return the wrapped identifier
     */
    public static SearchId of(String value) {
        return new SearchId(value);
    }

    /**
     * Returns the identifier for a protocol token.
     *
     * @param token the token the search was issued with
     * @return the wrapped identifier
     */
    public static SearchId ofToken(int token) {
        return new SearchId(Integer.toString(token));
    }

    @Override
    public String toString() {
        return value;
    }
}
