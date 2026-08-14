// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import dev.slsk.user.Username;

/**
 * Maps wire-supplied names onto {@link Username} without trusting them.
 *
 * <p>{@link Username#of} throws on blank and control-character values, which is
 * right for a caller's own input and wrong for the network's: the live server
 * sends a blank entry in its privileged-user list, and a name a peer sends is
 * whatever the peer felt like sending. A facet listener that maps such a value
 * through {@code Username.of} throws inside event delivery — the event is lost
 * for every consumer, and where the event carries an acknowledgement duty the
 * server redelivers it at every login forever.
 */
public final class Usernames {
    private Usernames() {}

    /**
     * Returns the username, or {@code null} when the wire value cannot be one.
     *
     * <p>Rejects exactly what {@link Username}'s constructor rejects — {@code
     * null}, blank, control characters — but by returning {@code null} so the
     * caller decides whether to skip the entry or drop the event, instead of
     * throwing away everything travelling with it.
     *
     * @param value the name as it came off the wire
     * @return the username, or {@code null} if the value cannot be one
     */
    public static Username fromWire(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        for (int index = 0; index < value.length(); index++) {
            if (Character.isISOControl(value.charAt(index))) {
                return null;
            }
        }
        return new Username(value);
    }
}
