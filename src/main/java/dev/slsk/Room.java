// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A room we are in, as it stands now.
 *
 * <p><strong>There is no message list here, and that is deliberate.</strong>
 * Membership, tickers and operators are state: the server asserts them and
 * replaces them wholesale, so a snapshot is always the truth. Messages are
 * history — an append-only record of things that happened — and the library does
 * not accumulate history. Only the application knows how much scrollback to keep
 * and where to put it, and a library that guessed would be wrong for every
 * consumer that wanted a different answer.
 *
 * @param name the room name
 * @param users who is in it
 * @param tickers the pinned messages
 * @param isPrivate whether it is a private room
 * @param owner its owner, for a private room
 * @param operators its moderators, for a private room
 */
public record Room(
        String name,
        List<RoomUser> users,
        List<RoomTicker> tickers,
        boolean isPrivate,
        Optional<Username> owner,
        Set<Username> operators) {

    /** Validates and returns the room. */
    public Room {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(owner, "owner");
        users = List.copyOf(Objects.requireNonNull(users, "users"));
        tickers = List.copyOf(Objects.requireNonNull(tickers, "tickers"));
        operators = Set.copyOf(Objects.requireNonNull(operators, "operators"));
    }

    /**
     * Returns how many users are in the room.
     *
     * @return the member count
     */
    public int userCount() {
        return users.size();
    }
}
