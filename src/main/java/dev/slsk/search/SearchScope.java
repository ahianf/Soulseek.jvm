// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.search;

import dev.slsk.user.Username;
import java.util.List;
import java.util.Objects;

/**
 * Who a search is asked of.
 *
 * @param kind which sort of scope this is
 * @param targets the rooms or users it names; empty for a network search
 */
public record SearchScope(Kind kind, List<String> targets) {

    private static final SearchScope NETWORK = new SearchScope(Kind.NETWORK, List.of());
    private static final SearchScope WISHLIST = new SearchScope(Kind.WISHLIST, List.of());

    /** Which sort of scope a search has. */
    public enum Kind {
        /** Everyone, through the distributed mesh. */
        NETWORK,
        /** The members of named rooms. */
        ROOM,
        /** Named users directly. */
        USER,
        /** A wishlist search, which the server paces. */
        WISHLIST
    }

    /** Validates and returns the scope. */
    public SearchScope {
        Objects.requireNonNull(kind, "kind");
        targets = List.copyOf(Objects.requireNonNull(targets, "targets"));
        if ((kind == Kind.ROOM || kind == Kind.USER) && targets.isEmpty()) {
            throw new IllegalArgumentException(kind + " scope needs at least one target");
        }
        if (kind == Kind.ROOM && targets.size() != 1) {
            throw new IllegalArgumentException("a room-scoped search names exactly one room");
        }
    }

    /** Searches the whole network. */
    public static SearchScope network() {
        return NETWORK;
    }

    /** Searches as a wishlist entry, on the server's own interval. */
    public static SearchScope wishlist() {
        return WISHLIST;
    }

    /**
     * Searches the members of a room.
     *
     * <p>One room, not several: the protocol carries a single room name in a
     * room-scoped search, and offering a list here would promise a fan-out the
     * wire cannot express.
     *
     * @param room the room
     * @return the scope
     */
    public static SearchScope room(String room) {
        return new SearchScope(Kind.ROOM, List.of(room));
    }

    /**
     * Searches named users directly.
     *
     * @param users the users
     * @return the scope
     */
    public static SearchScope users(Username... users) {
        return new SearchScope(
                Kind.USER, List.of(users).stream().map(Username::value).toList());
    }
}
