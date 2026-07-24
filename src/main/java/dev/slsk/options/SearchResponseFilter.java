// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.options;

import dev.slsk.SearchResponse;

/** Determines whether a search response is accepted. */
@FunctionalInterface
public interface SearchResponseFilter {
    /**
     * Tests a search response.
     *
     * @param response the response
     * @return whether the response is accepted
     */
    boolean test(SearchResponse response);
}
