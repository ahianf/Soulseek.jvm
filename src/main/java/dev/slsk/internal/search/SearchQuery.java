// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * A search query.
 */
public class SearchQuery {
    private final List<String> exclusions;
    private final List<String> terms;

    /**
     * Creates a query from term and exclusion sequences.
     *
     * @param terms the search terms
     * @param exclusions the excluded terms
     */
    public SearchQuery(Iterable<String> terms, Iterable<String> exclusions) {
        this.terms = immutableCopy(terms);
        this.exclusions = immutableCopy(exclusions);
    }

    /**
     * Creates a query by splitting query text and retaining explicit
     * exclusions.
     *
     * @param query the query text
     * @param exclusions the excluded terms
     */
    public SearchQuery(String query, Iterable<String> exclusions) {
        this(query == null ? null : List.of(query.split(" ", -1)), exclusions);
    }

    /**
     * Parses full search text into terms and exclusions.
     *
     * @param searchText the full search text
     */
    public SearchQuery(String searchText) {
        String[] tokens = searchText == null ? new String[0] : searchText.split(" ", -1);
        List<String> parsedTerms = new ArrayList<>();
        Set<String> parsedExclusions = new LinkedHashSet<>();

        for (String token : tokens) {
            if (token.startsWith("-") && token.length() > 1) {
                parsedExclusions.add(trimLeadingHyphens(token));
            } else {
                parsedTerms.add(token);
            }
        }

        terms = Collections.unmodifiableList(parsedTerms);
        exclusions = Collections.unmodifiableList(new ArrayList<>(parsedExclusions));
    }

    /**
     * Returns the excluded terms.
     *
     * @return the excluded terms
     */
    public final List<String> getExclusions() {
        return exclusions;
    }

    /**
     * Returns the query text concatenated from the terms.
     *
     * @return the query text
     */
    public final String getQuery() {
        return joinLikeCSharp(terms);
    }

    /**
     * Returns the full search text.
     *
     * @return the full search text
     */
    public final String getSearchText() {
        return toString();
    }

    /**
     * Returns the search terms.
     *
     * @return the search terms
     */
    public final List<String> getTerms() {
        return terms;
    }

    /**
     * Parses a new query from full search text.
     *
     * @param searchText the full search text
     * @return the parsed query
     */
    public static SearchQuery fromText(String searchText) {
        return new SearchQuery(searchText);
    }

    /**
     * Returns the full search text.
     *
     * @return the full search text
     */
    @Override
    public String toString() {
        String query = getQuery();
        if (exclusions.isEmpty()) {
            return query;
        }

        List<String> prefixedExclusions = exclusions.stream()
                .map(exclusion -> "-" + (exclusion == null ? "" : exclusion))
                .toList();
        return query + " " + String.join(" ", prefixedExclusions);
    }

    private static List<String> immutableCopy(Iterable<String> source) {
        List<String> copy = new ArrayList<>();
        if (source != null) {
            source.forEach(copy::add);
        }
        return Collections.unmodifiableList(copy);
    }

    private static String trimLeadingHyphens(String value) {
        int firstNonHyphen = 0;
        while (firstNonHyphen < value.length() && value.charAt(firstNonHyphen) == '-') {
            firstNonHyphen++;
        }
        return value.substring(firstNonHyphen);
    }

    private static String joinLikeCSharp(List<String> values) {
        return String.join(
                " ", values.stream().map(value -> value == null ? "" : value).toList());
    }
}
