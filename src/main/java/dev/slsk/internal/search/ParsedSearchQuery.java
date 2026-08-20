// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.search;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** A search query. */
public record ParsedSearchQuery(List<String> terms, List<String> exclusions) {

    public ParsedSearchQuery {
        terms = terms == null ? List.of() : List.copyOf(terms);
        exclusions = exclusions == null ? List.of() : List.copyOf(exclusions);
    }

    public ParsedSearchQuery(String query, List<String> exclusions) {
        this(query == null ? null : List.of(query.split(" ", -1)), exclusions);
    }

    public ParsedSearchQuery(String searchText) {
        this(parse(searchText));
    }

    private ParsedSearchQuery(Parsed parsed) {
        this(parsed.terms(), parsed.exclusions());
    }

    public static ParsedSearchQuery fromText(String searchText) {
        return new ParsedSearchQuery(searchText);
    }

    public String query() {
        return String.join(" ", terms);
    }

    public String searchText() {
        return toString();
    }

    @Override
    public String toString() {
        String query = query();
        if (exclusions.isEmpty()) {
            return query;
        }
        return query + " "
                + String.join(
                        " ",
                        exclusions.stream().map(exclusion -> "-" + exclusion).toList());
    }

    private static Parsed parse(String searchText) {
        String[] tokens = searchText == null ? new String[0] : searchText.split(" ", -1);
        List<String> terms = new ArrayList<>();
        Set<String> exclusions = new LinkedHashSet<>();
        for (String token : tokens) {
            if (token.startsWith("-") && token.length() > 1) {
                exclusions.add(trimLeadingHyphens(token));
            } else {
                terms.add(token);
            }
        }
        return new Parsed(terms, List.copyOf(exclusions));
    }

    private static String trimLeadingHyphens(String value) {
        int firstNonHyphen = 0;
        while (firstNonHyphen < value.length() && value.charAt(firstNonHyphen) == '-') {
            firstNonHyphen++;
        }
        return value.substring(firstNonHyphen);
    }

    private record Parsed(List<String> terms, List<String> exclusions) {}
}
