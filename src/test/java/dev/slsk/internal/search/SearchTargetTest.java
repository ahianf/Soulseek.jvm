// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SearchTargetTest {
    @Test
    @DisplayName("Instantiates Network Default")
    void instantiatesNetworkDefault() {
        SearchTarget scope = new SearchTarget(SearchScopeType.NETWORK);

        assertEquals(SearchScopeType.NETWORK, scope.type());
        assertTrue(subjects(scope).isEmpty());
    }

    @Test
    @DisplayName("Throws on Network when subjects is not empty")
    void throwsOnNetworkWhenSubjectsIsNotEmpty() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class, () -> new SearchTarget(SearchScopeType.NETWORK, "subject"));

        assertTrue(exception.getMessage().toLowerCase().contains("subjects"));
    }

    @Test
    @DisplayName("Throws on Wishlist when subjects is not empty")
    void throwsOnWishlistWhenSubjectsIsNotEmpty() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class, () -> new SearchTarget(SearchScopeType.WISHLIST, "subject"));

        assertTrue(exception.getMessage().toLowerCase().contains("subjects"));
    }

    @Test
    @DisplayName("Instantiates Room")
    void instantiatesRoom() {
        SearchTarget scope = new SearchTarget(SearchScopeType.ROOM, "room");

        assertEquals(SearchScopeType.ROOM, scope.type());
        assertEquals(List.of("room"), subjects(scope));
    }

    @Test
    @DisplayName("Throws on Room when subjects is empty")
    void throwsOnRoomWhenSubjectsIsEmpty() {
        assertRoomSubjectError(() -> new SearchTarget(SearchScopeType.ROOM, (String[]) null));
    }

    @Test
    @DisplayName("Throws on Room when subjects is one null string")
    void throwsOnRoomWhenSubjectsIsOneNullString() {
        assertRoomSubjectError(() -> new SearchTarget(SearchScopeType.ROOM, (String) null));
    }

    @Test
    @DisplayName("Throws on Room when subjects is one empty string")
    void throwsOnRoomWhenSubjectsIsOneEmptyString() {
        assertRoomSubjectError(() -> new SearchTarget(SearchScopeType.ROOM, ""));
    }

    @Test
    @DisplayName("Throws on Room when subjects is more than one")
    void throwsOnRoomWhenSubjectsIsMoreThanOne() {
        assertRoomSubjectError(() -> new SearchTarget(SearchScopeType.ROOM, "one", "two"));
    }

    @Test
    @DisplayName("Instantiates User")
    void instantiatesUser() {
        SearchTarget scope = new SearchTarget(SearchScopeType.USER, "alice");

        assertEquals(SearchScopeType.USER, scope.type());
        assertEquals(List.of("alice"), subjects(scope));
    }

    @Test
    @DisplayName("Throws on User when subjects is empty")
    void throwsOnUserWhenSubjectsIsEmpty() {
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> new SearchTarget(SearchScopeType.USER));

        assertTrue(exception.getMessage().contains("subjects must not be empty for User scope"));
    }

    @Test
    @DisplayName("Throws on User when subjects contains a null")
    void throwsOnUserWhenSubjectsContainsNull() {
        assertUserElementError(() -> new SearchTarget(SearchScopeType.USER, "one", null));
    }

    @Test
    @DisplayName("Throws on User when subjects contains an empty string")
    void throwsOnUserWhenSubjectsContainsEmptyString() {
        assertUserElementError(() -> new SearchTarget(SearchScopeType.USER, "one", ""));
    }

    @Test
    @DisplayName("Instantiates User with multiples")
    void instantiatesUserWithMultiples() {
        SearchTarget scope = new SearchTarget(SearchScopeType.USER, "alice", "bob");

        assertEquals(List.of("alice", "bob"), subjects(scope));
    }

    @Test
    @DisplayName("Network returns Network scope")
    void networkReturnsNetworkScope() {
        SearchTarget scope = SearchTarget.getNetwork();

        assertEquals(SearchScopeType.NETWORK, scope.type());
        assertTrue(subjects(scope).isEmpty());
    }

    @Test
    @DisplayName("Wishlist returns Wishlist scope")
    void wishlistReturnsWishlistScope() {
        SearchTarget scope = SearchTarget.getWishlist();

        assertEquals(SearchScopeType.WISHLIST, scope.type());
        assertTrue(subjects(scope).isEmpty());
    }

    @Test
    @DisplayName("Room() returns Room scope")
    void roomReturnsRoomScope() {
        SearchTarget scope = SearchTarget.room("room");

        assertEquals(SearchScopeType.ROOM, scope.type());
        assertEquals(List.of("room"), subjects(scope));
    }

    @Test
    @DisplayName("User() returns User scope")
    void userReturnsUserScope() {
        SearchTarget scope = SearchTarget.user("alice", "bob");

        assertEquals(SearchScopeType.USER, scope.type());
        assertEquals(List.of("alice", "bob"), subjects(scope));
    }

    @Test
    @DisplayName("Subjects snapshots the supplied array")
    void subjectsSnapshotsSuppliedArray() {
        String[] source = {"alice"};
        SearchTarget scope = new SearchTarget(SearchScopeType.USER, source);

        source[0] = "bob";

        assertEquals(List.of("alice"), subjects(scope));
    }

    @Test
    @DisplayName("Rejects a null search target type")
    void rejectsNullType() {
        assertThrows(NullPointerException.class, () -> new SearchTarget(null));
    }

    private static void assertRoomSubjectError(Runnable action) {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, action::run);
        assertTrue(exception.getMessage().contains("subjects must contain one non-empty room"));
    }

    private static void assertUserElementError(Runnable action) {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, action::run);
        assertTrue(exception.getMessage().contains("subjects must contain only non-empty usernames"));
    }

    private static List<String> subjects(SearchTarget scope) {
        return StreamSupport.stream(scope.subjects().spliterator(), false).toList();
    }
}
