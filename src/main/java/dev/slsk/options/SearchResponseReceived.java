// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.options;

import dev.slsk.Search;
import dev.slsk.SearchResponse;

/**
 * A received search-response callback payload.
 *
 * @param search the receiving search
 * @param response the received response
 */
public record SearchResponseReceived(Search search, SearchResponse response) {}
