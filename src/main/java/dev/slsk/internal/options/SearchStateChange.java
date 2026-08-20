// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import dev.slsk.internal.search.Search;
import dev.slsk.internal.search.SearchPhase;

/**
 * A search state-change callback payload.
 *
 * @param previousState the previous search state
 * @param search the search after the state change
 */
public record SearchStateChange(SearchPhase previousState, Search search) {}
