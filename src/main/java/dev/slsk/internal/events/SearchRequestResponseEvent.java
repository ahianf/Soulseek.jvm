// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.internal.search.SearchResponseMessage;

/** Event payload for the disposition of a search-request response. */
public record SearchRequestResponseEvent(String username, int token, String query, SearchResponseMessage searchResponse)
        implements SoulseekClientEvent {}
