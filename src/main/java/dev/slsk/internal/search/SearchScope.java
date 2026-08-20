// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Search scope definition. */
public record SearchScope(SearchScopeType type, List<String> subjects) {

    public SearchScope {
        type = Objects.requireNonNull(type, "type");
        List<String> suppliedSubjects = subjects == null ? List.of() : subjects;
        if ((type == SearchScopeType.NETWORK || type == SearchScopeType.WISHLIST) && !suppliedSubjects.isEmpty()) {
            throw new IllegalArgumentException(
                    "The " + displayName(type) + " search scope can not be used with subjects");
        }
        if (type == SearchScopeType.ROOM
                && (suppliedSubjects.size() != 1
                        || suppliedSubjects.getFirst() == null
                        || suppliedSubjects.getFirst().isEmpty())) {
            throw new IllegalArgumentException(
                    "The Room search scope requires a single, non null and non empty subject");
        }
        if (type == SearchScopeType.USER) {
            if (suppliedSubjects.isEmpty()) {
                throw new IllegalArgumentException("The User search scope requires at least one subject");
            }
            if (suppliedSubjects.stream().anyMatch(value -> value == null || value.isEmpty())) {
                throw new IllegalArgumentException("One or more of the supplied User scope subjects is null or empty");
            }
        }
        subjects = List.copyOf(suppliedSubjects);
    }

    public SearchScope(SearchScopeType type, String... subjects) {
        this(type, subjects == null ? List.of() : Arrays.asList(subjects));
    }

    public static SearchScope getNetwork() {
        return new SearchScope(SearchScopeType.NETWORK);
    }

    public static SearchScope getWishlist() {
        return new SearchScope(SearchScopeType.WISHLIST);
    }

    public static SearchScope room(String roomName) {
        return new SearchScope(SearchScopeType.ROOM, roomName);
    }

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
