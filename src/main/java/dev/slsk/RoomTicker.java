// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.user.Username;
import java.util.Objects;

/**
 * A message a user pinned to a room.
 *
 * <p>Tickers are room state, not chat: the server replaces the whole list, one
 * per user, and a user setting a second ticker replaces their first rather than
 * adding to it.
 *
 * @param user who set it
 * @param message what it says
 */
public record RoomTicker(Username user, String message) {

    /** Validates and returns the ticker. */
    public RoomTicker {
        Objects.requireNonNull(user, "user");
        Objects.requireNonNull(message, "message");
    }
}
