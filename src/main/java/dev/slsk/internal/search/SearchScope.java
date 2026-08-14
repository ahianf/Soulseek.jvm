// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import java.util.Arrays;
import java.util.Objects;

/**
 * Search scope definition.
 */
public class SearchScope {
    private final String[] subjects;
    private final SearchScopeType type;

    /**
     * Creates a search scope.
     *
     * @param type the scope type
     * @param subjects the scope subjects, if applicable
     * @throws IllegalArgumentException when the subjects are invalid for the
     *     selected scope
     */
    public SearchScope(SearchScopeType type, String... subjects) {
        this.type = Objects.requireNonNull(type, "type");
        this.subjects = subjects == null ? new String[0] : subjects;

        if ((type == SearchScopeType.NETWORK || type == SearchScopeType.WISHLIST) && this.subjects.length > 0) {
            throw new IllegalArgumentException(
                    "The " + displayName(type) + " search scope can not be used with subjects");
        }

        if (type == SearchScopeType.ROOM
                && (this.subjects.length != 1 || this.subjects[0] == null || this.subjects[0].isEmpty())) {
            throw new IllegalArgumentException(
                    "The Room search scope requires a single, non null and non empty subject");
        }

        if (type == SearchScopeType.USER) {
            if (this.subjects.length == 0) {
                throw new IllegalArgumentException("The User search scope requires at least one subject");
            }
            if (Arrays.stream(this.subjects).anyMatch(value -> value == null || value.isEmpty())) {
                throw new IllegalArgumentException("One or more of the supplied User scope subjects is null or empty");
            }
        }
    }

    /**
     * Returns a new network search scope.
     *
     * @return a network search scope
     */
    public static SearchScope getNetwork() {
        return new SearchScope(SearchScopeType.NETWORK);
    }

    /**
     * Returns a new wishlist search scope.
     *
     * @return a wishlist search scope
     */
    public static SearchScope getWishlist() {
        return new SearchScope(SearchScopeType.WISHLIST);
    }

    /**
     * Returns the scope subjects.
     *
     * <p>The C# source retains the supplied parameter array. This iterable is
     * therefore backed by the caller's array rather than being a snapshot.</p>
     *
     * @return the scope subjects
     */
    public final Iterable<String> getSubjects() {
        return Arrays.asList(subjects);
    }

    /**
     * Returns the scope type.
     *
     * @return the scope type
     */
    public final SearchScopeType getType() {
        return type;
    }

    /**
     * Returns a room scope.
     *
     * @param roomName the room to search
     * @return the room scope
     */
    public static SearchScope room(String roomName) {
        return new SearchScope(SearchScopeType.ROOM, roomName);
    }

    /**
     * Returns a user scope.
     *
     * @param usernames the users to search
     * @return the user scope
     */
    public static SearchScope user(String... usernames) {
        return new SearchScope(SearchScopeType.USER, usernames);
    }

    private static String displayName(SearchScopeType type) {
        return switch (type) {
            case NETWORK -> "Network";
            case USER -> "User";
            case ROOM -> "Room";
            case WISHLIST -> "Wishlist";
        };
    }
}
