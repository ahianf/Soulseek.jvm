// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.eventargs;

import dev.slsk.Search;
import dev.slsk.SearchStates;
import java.util.Objects;

/**
 * Event arguments raised by a search-state change.
 */
public class SearchStateChangedEventArgs extends SearchEventArgs {
    private final SearchStates previousState;

    SearchStateChangedEventArgs(SearchStates previousState, Search search) {
        super(search);
        this.previousState = Objects.requireNonNull(previousState, "previousState");
    }

    /**
     * Returns the previous search state.
     *
     * @return the previous state
     */
    public final SearchStates getPreviousState() {
        return previousState;
    }
}
