// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

/**
 * Whether a user is around.
 *
 * <p>A user being offline is a value, not a failure: asking for someone's status
 * and getting {@link #OFFLINE} is the normal answer to a normal question, and
 * throwing for it would make every caller wrap a lookup in a try block.
 */
public enum UserPresence {

    /** Not connected. */
    OFFLINE,

    /** Connected, but idle. */
    AWAY,

    /** Connected and active. */
    ONLINE
}
