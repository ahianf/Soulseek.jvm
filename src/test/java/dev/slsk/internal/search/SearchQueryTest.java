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

        assertEquals("foo bar", query.getQuery());
        assertEquals(List.of("baz", "qux"), query.getExclusions());
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
        SearchQuery query = new SearchQuery((Iterable<String>) null, null);

        assertEmptyQuery(query);
    }

    @Test
    @DisplayName("Splits terms and exclusions")
    void splitsTermsAndExclusions() {
        SearchQuery query = new SearchQuery("foo bar -baz -qux");

        assertEquals(List.of("foo", "bar"), query.getTerms());
        assertEquals(List.of("baz", "qux"), query.getExclusions());
    }

    @ParameterizedTest
    @MethodSource("expectedSearchTexts")
    @DisplayName("Constructs expected search text")
    void constructsExpectedSearchText(String queryText, List<String> exclusions, String expected) {
        SearchQuery query = new SearchQuery(queryText, exclusions);

        assertEquals(expected, query.getSearchText());
    }

    @Test
    @DisplayName("Parses query-only search text")
    void parsesQueryOnlySearchText() {
        SearchQuery query = new SearchQuery("foo");

        assertEquals("foo", query.getQuery());
        assertEquals("foo", query.getSearchText());
        assertTrue(query.getExclusions().isEmpty());
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
                query.getQuery());
        assertEquals(
                "a ! b @ c # d % e ^ f & g * h ( i ) j - k _ l + m = n ~ o ` p [ q { "
                        + "r ] s } t | u \\ v ; w : x ' y \" z , a < b . c > d / e ? "
                        + "-big_old_exclusion",
                query.getSearchText());
        assertEquals(List.of("big_old_exclusion"), query.getExclusions());
    }

    @Test
    @DisplayName("Parses exclusions")
    void parsesExclusions() {
        SearchQuery query = new SearchQuery("foo -bar -baz");

        assertEquals("foo", query.getQuery());
        assertEquals("foo -bar -baz", query.getSearchText());
        assertEquals(List.of("bar", "baz"), query.getExclusions());
    }

    @Test
    @DisplayName("Parses exclusions out of order")
    void parsesExclusionsOutOfOrder() {
        SearchQuery query = new SearchQuery("-bar foo -baz");

        assertEquals("foo", query.getQuery());
        assertEquals("foo -bar -baz", query.getSearchText());
        assertEquals(List.of("bar", "baz"), query.getExclusions());
    }

    @Test
    @DisplayName("Parses repeated exclusions singly")
    void parsesRepeatedExclusionsSingly() {
        SearchQuery query = new SearchQuery("-bar foo -baz -baz -bar");

        assertEquals("foo", query.getQuery());
        assertEquals("foo -bar -baz", query.getSearchText());
        assertEquals(List.of("bar", "baz"), query.getExclusions());
    }

    @Test
    @DisplayName("Preserves duplicate terms")
    void preservesDuplicateTerms() {
        SearchQuery query = new SearchQuery("foo bar foo foo");

        assertEquals("foo bar foo foo", query.getQuery());
        assertEquals("foo bar foo foo", query.getSearchText());
        assertTrue(query.getExclusions().isEmpty());
    }

    @Test
    @DisplayName("FromText returns new instance from given text")
    void fromTextReturnsNewInstanceFromGivenText() {
        SearchQuery query = SearchQuery.fromText("foo bar");

        assertEquals("foo bar", query.getSearchText());
    }

    @Test
    @DisplayName("Preserves empty tokens from repeated and trailing spaces")
    void preservesEmptyTokensFromRepeatedAndTrailingSpaces() {
        SearchQuery query = new SearchQuery("foo  bar ");

        assertEquals(List.of("foo", "", "bar", ""), query.getTerms());
        assertEquals("foo  bar ", query.getSearchText());
    }

    @Test
    @DisplayName("Trims every leading hyphen from exclusions")
    void trimsEveryLeadingHyphenFromExclusions() {
        SearchQuery query = new SearchQuery("foo ---bar");

        assertEquals(List.of("bar"), query.getExclusions());
        assertEquals("foo -bar", query.getSearchText());
    }

    @Test
    @DisplayName("Exclusion distinctness is case-sensitive")
    void exclusionDistinctnessIsCaseSensitive() {
        SearchQuery query = new SearchQuery("foo -bar -BAR");

        assertEquals(List.of("bar", "BAR"), query.getExclusions());
    }

    @Test
    @DisplayName("Copies and protects explicit terms and exclusions")
    void copiesAndProtectsExplicitTermsAndExclusions() {
        List<String> terms = new ArrayList<>(List.of("foo"));
        List<String> exclusions = new ArrayList<>(List.of("bar"));
        SearchQuery query = new SearchQuery(terms, exclusions);

        terms.clear();
        exclusions.clear();

        assertEquals(List.of("foo"), query.getTerms());
        assertEquals(List.of("bar"), query.getExclusions());
        assertThrows(UnsupportedOperationException.class, () -> query.getTerms().add("baz"));
        assertThrows(
                UnsupportedOperationException.class, () -> query.getExclusions().add("qux"));
    }

    @Test
    @DisplayName("Formats null collection elements like C# string.Join and interpolation")
    void formatsNullCollectionElementsLikeCSharp() {
        SearchQuery query =
                new SearchQuery(java.util.Arrays.asList("foo", null, "bar"), java.util.Arrays.asList((String) null));

        assertEquals("foo  bar", query.getQuery());
        assertEquals("foo  bar -", query.getSearchText());
    }

    private static Stream<Arguments> expectedSearchTexts() {
        return Stream.of(
                Arguments.of("foo", List.of("bar", "baz"), "foo -bar -baz"),
                Arguments.of("foo", List.of("bar"), "foo -bar"),
                Arguments.of("foo", null, "foo"));
    }

    private static void assertEmptyQuery(SearchQuery query) {
        assertTrue(query.getTerms().isEmpty());
        assertTrue(query.getExclusions().isEmpty());
        assertEquals("", query.getQuery());
        assertEquals("", query.getSearchText());
    }
}
