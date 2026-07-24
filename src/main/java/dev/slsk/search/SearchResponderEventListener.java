// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.search;

import dev.slsk.eventargs.SoulseekClientEventArgs;

/** Handles a search-responder event. */
@FunctionalInterface
public interface SearchResponderEventListener<T extends SoulseekClientEventArgs> {
    void handle(SearchResponder sender, T eventArgs);
}
