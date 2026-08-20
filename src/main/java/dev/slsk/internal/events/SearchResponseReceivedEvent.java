// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.internal.search.SearchResponseMessage;
import dev.slsk.internal.search.SearchSnapshot;

/** Event payload emitted when a search response is received. */
public record SearchResponseReceivedEvent(SearchResponseMessage response, SearchSnapshot search)
        implements SoulseekClientEvent {}
