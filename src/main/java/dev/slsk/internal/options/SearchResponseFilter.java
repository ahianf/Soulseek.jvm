// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import dev.slsk.internal.search.SearchResponseMessage;

/** Determines whether a search response is accepted. */
@FunctionalInterface
public interface SearchResponseFilter {
    /**
     * Tests a search response.
     *
     * @param response the response
     * @return whether the response is accepted
     */
    boolean test(SearchResponseMessage response);
}
