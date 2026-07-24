// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SearchScopeTest {
    @Test
    @DisplayName("Instantiates Network Default")
    void instantiatesNetworkDefault() {
        SearchScope scope = new SearchScope(SearchScopeType.NETWORK);

        assertEquals(SearchScopeType.NETWORK, scope.getType());
        assertTrue(subjects(scope).isEmpty());
    }

    @Test
    @DisplayName("Throws on Network when subjects is not empty")
    void throwsOnNetworkWhenSubjectsIsNotEmpty() {
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> new SearchScope(SearchScopeType.NETWORK, "subject"));

        assertTrue(exception.getMessage().toLowerCase().contains("subjects"));
    }

    @Test
    @DisplayName("Throws on Wishlist when subjects is not empty")
    void throwsOnWishlistWhenSubjectsIsNotEmpty() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class, () -> new SearchScope(SearchScopeType.WISHLIST, "subject"));

        assertTrue(exception.getMessage().toLowerCase().contains("subjects"));
    }

    @Test
    @DisplayName("Instantiates Room")
    void instantiatesRoom() {
        SearchScope scope = new SearchScope(SearchScopeType.ROOM, "room");

        assertEquals(SearchScopeType.ROOM, scope.getType());
        assertEquals(List.of("room"), subjects(scope));
    }

    @Test
    @DisplayName("Throws on Room when subjects is empty")
    void throwsOnRoomWhenSubjectsIsEmpty() {
        assertRoomSubjectError(() -> new SearchScope(SearchScopeType.ROOM, (String[]) null));
    }

    @Test
    @DisplayName("Throws on Room when subjects is one null string")
    void throwsOnRoomWhenSubjectsIsOneNullString() {
        assertRoomSubjectError(() -> new SearchScope(SearchScopeType.ROOM, (String) null));
    }

    @Test
    @DisplayName("Throws on Room when subjects is one empty string")
    void throwsOnRoomWhenSubjectsIsOneEmptyString() {
        assertRoomSubjectError(() -> new SearchScope(SearchScopeType.ROOM, ""));
    }

    @Test
    @DisplayName("Throws on Room when subjects is more than one")
    void throwsOnRoomWhenSubjectsIsMoreThanOne() {
        assertRoomSubjectError(() -> new SearchScope(SearchScopeType.ROOM, "one", "two"));
    }

    @Test
    @DisplayName("Instantiates User")
    void instantiatesUser() {
        SearchScope scope = new SearchScope(SearchScopeType.USER, "alice");

        assertEquals(SearchScopeType.USER, scope.getType());
        assertEquals(List.of("alice"), subjects(scope));
    }

    @Test
    @DisplayName("Throws on User when subjects is empty")
    void throwsOnUserWhenSubjectsIsEmpty() {
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> new SearchScope(SearchScopeType.USER));

        assertTrue(exception.getMessage().contains("requires at least one subject"));
    }

    @Test
    @DisplayName("Throws on User when subjects contains a null")
    void throwsOnUserWhenSubjectsContainsNull() {
        assertUserElementError(() -> new SearchScope(SearchScopeType.USER, "one", null));
    }

    @Test
    @DisplayName("Throws on User when subjects contains an empty string")
    void throwsOnUserWhenSubjectsContainsEmptyString() {
        assertUserElementError(() -> new SearchScope(SearchScopeType.USER, "one", ""));
    }

    @Test
    @DisplayName("Instantiates User with multiples")
    void instantiatesUserWithMultiples() {
        SearchScope scope = new SearchScope(SearchScopeType.USER, "alice", "bob");

        assertEquals(List.of("alice", "bob"), subjects(scope));
    }

    @Test
    @DisplayName("Network returns Network scope")
    void networkReturnsNetworkScope() {
        SearchScope scope = SearchScope.getNetwork();

        assertEquals(SearchScopeType.NETWORK, scope.getType());
        assertTrue(subjects(scope).isEmpty());
    }

    @Test
    @DisplayName("Wishlist returns Wishlist scope")
    void wishlistReturnsWishlistScope() {
        SearchScope scope = SearchScope.getWishlist();

        assertEquals(SearchScopeType.WISHLIST, scope.getType());
        assertTrue(subjects(scope).isEmpty());
    }

    @Test
    @DisplayName("Room() returns Room scope")
    void roomReturnsRoomScope() {
        SearchScope scope = SearchScope.room("room");

        assertEquals(SearchScopeType.ROOM, scope.getType());
        assertEquals(List.of("room"), subjects(scope));
    }

    @Test
    @DisplayName("User() returns User scope")
    void userReturnsUserScope() {
        SearchScope scope = SearchScope.user("alice", "bob");

        assertEquals(SearchScopeType.USER, scope.getType());
        assertEquals(List.of("alice", "bob"), subjects(scope));
    }

    @Test
    @DisplayName("Subjects retains the supplied array like the C# params value")
    void subjectsRetainsSuppliedArray() {
        String[] source = {"alice"};
        SearchScope scope = new SearchScope(SearchScopeType.USER, source);

        source[0] = "bob";

        assertEquals(List.of("bob"), subjects(scope));
    }

    @Test
    @DisplayName("Rejects null type because the C# enum is non-nullable")
    void rejectsNullType() {
        assertThrows(NullPointerException.class, () -> new SearchScope(null));
    }

    private static void assertRoomSubjectError(Runnable action) {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, action::run);
        assertTrue(exception.getMessage().contains("requires a single, non null and non empty"));
    }

    private static void assertUserElementError(Runnable action) {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, action::run);
        assertTrue(exception.getMessage().contains("One or more of the supplied User scope subjects is null or empty"));
    }

    private static List<String> subjects(SearchScope scope) {
        return StreamSupport.stream(scope.getSubjects().spliterator(), false).toList();
    }
}
