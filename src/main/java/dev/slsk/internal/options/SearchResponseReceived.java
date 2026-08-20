// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import dev.slsk.internal.search.SearchResponseMessage;
import dev.slsk.internal.search.SearchSnapshot;

/**
 * A received search-response callback payload.
 *
 * @param search the receiving search
 * @param response the received response
 */
public record SearchResponseReceived(SearchSnapshot search, SearchResponseMessage response) {}
