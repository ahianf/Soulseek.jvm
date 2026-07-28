// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.util.Objects;

/**
 * A Soulseek username: the key of nearly everything the network does.
 *
 * <p>This exists because the surface it replaced was full of adjacent {@code
 * String} parameters the compiler could not tell apart — {@code
 * addPrivateRoomMember(String room, String username)}, {@code setRoomTicker(
 * String room, String message)}, {@code sendPrivateMessage(String username,
 * String message)}. Swapping either pair compiles cleanly and fails on the wire.
 * Wrapping one side of each pair is enough to make the mistake impossible.
 *
 * <p><strong>Case is preserved and equality is case-sensitive.</strong> That is
 * not an oversight. Soulseek puts the username on the wire exactly as given, and
 * every correlation map in this library — pending transfers, watched users,
 * connection caches — is keyed on that exact string. Folding case here would
 * silently change which entries those maps find, so a name that differs in case
 * is a different {@code Username}, and any case-insensitive comparison stays an
 * explicit decision at the one call site that wants it.
 *
 * <p>Validation is deliberately thin. The server owns the rules about what
 * names may exist, and a client that invents stricter ones only fails to talk to
 * users the server was happy to create. Rejected here are the values that cannot
 * be a username under any rule: {@code null}, blank, and anything carrying a
 * control character — which would corrupt the message framing rather than
 * produce a failed lookup.
 *
 * @param value the username, exactly as it goes on the wire
 */
public record Username(String value) implements Comparable<Username> {

    /**
     * Validates and returns the username.
     *
     * @throws NullPointerException if {@code value} is {@code null}
     * @throws IllegalArgumentException if {@code value} is blank or contains a
     *     control character
     */
    public Username {
        Objects.requireNonNull(value, "value");
        if (value.isBlank()) {
            throw new IllegalArgumentException("username must not be blank");
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (Character.isISOControl(character)) {
                throw new IllegalArgumentException("username must not contain control characters, found 0x"
                        + Integer.toHexString(character) + " at index " + index);
            }
        }
    }

    /**
     * Returns the username for {@code value}.
     *
     * @param value the username
     * @return the wrapped username
     */
    public static Username of(String value) {
        return new Username(value);
    }

    /** Orders by the underlying string, so a rendered list is stable. */
    @Override
    public int compareTo(Username other) {
        return value.compareTo(other.value);
    }

    /** Returns the username itself, so logging and string concatenation read right. */
    @Override
    public String toString() {
        return value;
    }
}
