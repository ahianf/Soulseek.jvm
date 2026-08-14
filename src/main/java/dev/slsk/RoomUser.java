// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.user.UserPresence;
import dev.slsk.user.UserStatistics;
import dev.slsk.user.Username;
import java.util.Objects;
import java.util.OptionalInt;

/**
 * Somebody in a room, as the room reports them.
 *
 * <p>The server sends the membership and everyone's figures together when a room
 * is joined, so this carries what is already known rather than making a consumer
 * ask about each user separately.
 *
 * @param user who
 * @param status their presence and privilege
 * @param statistics their sharing figures
 * @param freeUploadSlots slots they have free, if the server said
 * @param countryCode their country, if the server said
 */
public record RoomUser(
        Username user,
        UserPresence status,
        UserStatistics statistics,
        OptionalInt freeUploadSlots,
        java.util.Optional<String> countryCode) {

    /** Validates and returns the room user. */
    public RoomUser {
        Objects.requireNonNull(user, "user");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(statistics, "statistics");
        Objects.requireNonNull(freeUploadSlots, "freeUploadSlots");
        Objects.requireNonNull(countryCode, "countryCode");
    }
}
