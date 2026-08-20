// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.user;

/**
 * User presence status.
 */
public enum WireUserPresence {
    /** The user is offline. */
    OFFLINE(0),

    /** The user is away. */
    AWAY(1),

    /** The user is online. */
    ONLINE(2);

    private final int value;

    WireUserPresence(int value) {
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
    public static WireUserPresence fromValue(int value) {
        for (WireUserPresence presence : values()) {
            if (presence.value == value) {
                return presence;
            }
        }
        throw new IllegalArgumentException("Unknown user presence: " + value);
    }
}
