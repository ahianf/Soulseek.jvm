// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** SearchStateSnapshot scope definition. */
public record SearchTarget(SearchScopeType type, List<String> subjects) {

    public SearchTarget {
        type = Objects.requireNonNull(type, "type");
        List<String> suppliedSubjects = subjects == null ? List.of() : subjects;
        if ((type == SearchScopeType.NETWORK || type == SearchScopeType.WISHLIST) && !suppliedSubjects.isEmpty()) {
            throw new IllegalArgumentException(
                    "subjects must be empty for " + displayName(type) + " scope: " + suppliedSubjects);
        }
        if (type == SearchScopeType.ROOM
                && (suppliedSubjects.size() != 1
                        || suppliedSubjects.getFirst() == null
                        || suppliedSubjects.getFirst().isEmpty())) {
            throw new IllegalArgumentException(
                    "subjects must contain one non-empty room for Room scope: " + suppliedSubjects);
        }
        if (type == SearchScopeType.USER) {
            if (suppliedSubjects.isEmpty()) {
                throw new IllegalArgumentException("subjects must not be empty for User scope");
            }
            if (suppliedSubjects.stream().anyMatch(value -> value == null || value.isEmpty())) {
                throw new IllegalArgumentException(
                        "subjects must contain only non-empty usernames: " + suppliedSubjects);
            }
        }
        subjects = List.copyOf(suppliedSubjects);
    }

    public SearchTarget(SearchScopeType type, String... subjects) {
        this(type, subjects == null ? List.of() : Arrays.asList(subjects));
    }

    public static SearchTarget getNetwork() {
        return new SearchTarget(SearchScopeType.NETWORK);
    }

    public static SearchTarget getWishlist() {
        return new SearchTarget(SearchScopeType.WISHLIST);
    }

    public static SearchTarget room(String roomName) {
        return new SearchTarget(SearchScopeType.ROOM, roomName);
    }

    public static SearchTarget user(String... usernames) {
        return new SearchTarget(SearchScopeType.USER, usernames);
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
