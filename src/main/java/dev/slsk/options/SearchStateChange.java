// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.options;

import dev.slsk.Search;
import dev.slsk.SearchState;

/**
 * A search state-change callback payload.
 *
 * @param previousState the previous search state
 * @param search the search after the state change
 */
public record SearchStateChange(SearchState previousState, Search search) {}
