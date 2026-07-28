// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

/**
 * User presence status.
 */
public enum UserPresence {
    /** The user is offline. */
    OFFLINE(0),

    /** The user is away. */
    AWAY(1),

    /** The user is online. */
    ONLINE(2);

    private final int value;

    UserPresence(int value) {
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
     * Returns the presence represented by a protocol value.
     *
     * @param value the protocol value
     * @return the matching presence
     * @throws IllegalArgumentException when the value is unknown
     */
    public static UserPresence fromValue(int value) {
        for (UserPresence presence : values()) {
            if (presence.value == value) {
                return presence;
            }
        }
        throw new IllegalArgumentException("Unknown user presence: " + value);
    }
}
