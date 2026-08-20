// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SearchQueryTest {
    @Test
    @DisplayName("Instantiates with given values")
    void instantiatesWithGivenValues() {
        SearchQuery query = new SearchQuery("foo bar", List.of("baz", "qux"));

        assertEquals("foo bar", query.query());
        assertEquals(List.of("baz", "qux"), query.exclusions());
    }

    @Test
    @DisplayName("Instantiates with null searchText")
    void instantiatesWithNullSearchText() {
        SearchQuery query = new SearchQuery((String) null);

        assertEmptyQuery(query);
    }

    @Test
    @DisplayName("Instantiates with null query and exclusions")
    void instantiatesWithNullQueryAndExclusions() {
        SearchQuery query = new SearchQuery((String) null, null);

        assertEmptyQuery(query);
    }

    @Test
    @DisplayName("Instantiates with null terms and exclusions")
    void instantiatesWithNullTermsAndExclusions() {
        SearchQuery query = new SearchQuery((List<String>) null, null);

        assertEmptyQuery(query);
    }

    @Test
    @DisplayName("Splits terms and exclusions")
    void splitsTermsAndExclusions() {
        SearchQuery query = new SearchQuery("foo bar -baz -qux");

        assertEquals(List.of("foo", "bar"), query.terms());
        assertEquals(List.of("baz", "qux"), query.exclusions());
    }

    @ParameterizedTest
    @MethodSource("expectedSearchTexts")
    @DisplayName("Constructs expected search text")
    void constructsExpectedSearchText(String queryText, List<String> exclusions, String expected) {
        SearchQuery query = new SearchQuery(queryText, exclusions);

        assertEquals(expected, query.searchText());
    }

    @Test
    @DisplayName("Parses query-only search text")
    void parsesQueryOnlySearchText() {
        SearchQuery query = new SearchQuery("foo");

        assertEquals("foo", query.query());
        assertEquals("foo", query.searchText());
        assertTrue(query.exclusions().isEmpty());
    }

    @Test
    @DisplayName("Parses single character tokens and punctuation from search text")
    void parsesSingleCharacterTokensAndPunctuation() {
        String input = "a ! b @ c # d % e ^ f & g * h ( i ) j - k _ l + m = n "
                + "-big_old_exclusion ~ o ` p [ q { r ] s } t | u \\ v ; w : x ' y \" z , "
                + "a < b . c > d / e ?";
        SearchQuery query = new SearchQuery(input);

        assertEquals(
                "a ! b @ c # d % e ^ f & g * h ( i ) j - k _ l + m = n ~ o ` p [ q { "
                        + "r ] s } t | u \\ v ; w : x ' y \" z , a < b . c > d / e ?",
                query.query());
        assertEquals(
                "a ! b @ c # d % e ^ f & g * h ( i ) j - k _ l + m = n ~ o ` p [ q { "
                        + "r ] s } t | u \\ v ; w : x ' y \" z , a < b . c > d / e ? "
                        + "-big_old_exclusion",
                query.searchText());
        assertEquals(List.of("big_old_exclusion"), query.exclusions());
    }

    @Test
    @DisplayName("Parses exclusions")
    void parsesExclusions() {
        SearchQuery query = new SearchQuery("foo -bar -baz");

        assertEquals("foo", query.query());
        assertEquals("foo -bar -baz", query.searchText());
        assertEquals(List.of("bar", "baz"), query.exclusions());
    }

    @Test
    @DisplayName("Parses exclusions out of order")
    void parsesExclusionsOutOfOrder() {
        SearchQuery query = new SearchQuery("-bar foo -baz");

        assertEquals("foo", query.query());
        assertEquals("foo -bar -baz", query.searchText());
        assertEquals(List.of("bar", "baz"), query.exclusions());
    }

    @Test
    @DisplayName("Parses repeated exclusions singly")
    void parsesRepeatedExclusionsSingly() {
        SearchQuery query = new SearchQuery("-bar foo -baz -baz -bar");

        assertEquals("foo", query.query());
        assertEquals("foo -bar -baz", query.searchText());
        assertEquals(List.of("bar", "baz"), query.exclusions());
    }

    @Test
    @DisplayName("Preserves duplicate terms")
    void preservesDuplicateTerms() {
        SearchQuery query = new SearchQuery("foo bar foo foo");

        assertEquals("foo bar foo foo", query.query());
        assertEquals("foo bar foo foo", query.searchText());
        assertTrue(query.exclusions().isEmpty());
    }

    @Test
    @DisplayName("FromText returns new instance from given text")
    void fromTextReturnsNewInstanceFromGivenText() {
        SearchQuery query = SearchQuery.fromText("foo bar");

        assertEquals("foo bar", query.searchText());
    }

    @Test
    @DisplayName("Preserves empty tokens from repeated and trailing spaces")
    void preservesEmptyTokensFromRepeatedAndTrailingSpaces() {
        SearchQuery query = new SearchQuery("foo  bar ");

        assertEquals(List.of("foo", "", "bar", ""), query.terms());
        assertEquals("foo  bar ", query.searchText());
    }

    @Test
    @DisplayName("Trims every leading hyphen from exclusions")
    void trimsEveryLeadingHyphenFromExclusions() {
        SearchQuery query = new SearchQuery("foo ---bar");

        assertEquals(List.of("bar"), query.exclusions());
        assertEquals("foo -bar", query.searchText());
    }

    @Test
    @DisplayName("Exclusion distinctness is case-sensitive")
    void exclusionDistinctnessIsCaseSensitive() {
        SearchQuery query = new SearchQuery("foo -bar -BAR");

        assertEquals(List.of("bar", "BAR"), query.exclusions());
    }

    @Test
    @DisplayName("Copies and protects explicit terms and exclusions")
    void copiesAndProtectsExplicitTermsAndExclusions() {
        List<String> terms = new ArrayList<>(List.of("foo"));
        List<String> exclusions = new ArrayList<>(List.of("bar"));
        SearchQuery query = new SearchQuery(terms, exclusions);

        terms.clear();
        exclusions.clear();

        assertEquals(List.of("foo"), query.terms());
        assertEquals(List.of("bar"), query.exclusions());
        assertThrows(UnsupportedOperationException.class, () -> query.terms().add("baz"));
        assertThrows(
                UnsupportedOperationException.class, () -> query.exclusions().add("qux"));
    }

    @Test
    @DisplayName("Rejects null collection elements")
    void rejectsNullCollectionElements() {
        assertThrows(
                NullPointerException.class,
                () -> new SearchQuery(java.util.Arrays.asList("foo", null, "bar"), List.of()));
        assertThrows(
                NullPointerException.class,
                () -> new SearchQuery(List.of("foo"), java.util.Arrays.asList((String) null)));
    }

    private static Stream<Arguments> expectedSearchTexts() {
        return Stream.of(
                Arguments.of("foo", List.of("bar", "baz"), "foo -bar -baz"),
                Arguments.of("foo", List.of("bar"), "foo -bar"),
                Arguments.of("foo", null, "foo"));
    }

    private static void assertEmptyQuery(SearchQuery query) {
        assertTrue(query.terms().isEmpty());
        assertTrue(query.exclusions().isEmpty());
        assertEquals("", query.query());
        assertEquals("", query.searchText());
    }
}
