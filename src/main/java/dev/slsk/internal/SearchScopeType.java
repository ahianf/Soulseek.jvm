// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

/**
 * Search scope type.
 */
public enum SearchScopeType {
    /** Search the network. */
    NETWORK(0),

    /** Search a user. */
    USER(1),

    /** Search a room. */
    ROOM(2),

    /** Run a wishlist search. */
    WISHLIST(3);

    private final int value;

    SearchScopeType(int value) {
        this.value = value;
    }

    /**
     * Returns the protocol value.
     *
     * @return the protocol value
     */
    public int getValue() {
        return value;
    }

    /**
     * Returns the scope represented by a protocol value.
     *
     * @param value the protocol value
     * @return the matching scope
     * @throws IllegalArgumentException when the value is unknown
     */
    public static SearchScopeType fromValue(int value) {
        for (SearchScopeType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown search scope type: " + value);
    }
}
