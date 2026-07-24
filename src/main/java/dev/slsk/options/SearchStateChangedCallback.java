// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.options;

/** Receives search state changes. */
@FunctionalInterface
public interface SearchStateChangedCallback {
    /**
     * Handles a search state change.
     *
     * @param change the state-change data
     */
    void onStateChanged(SearchStateChange change);
}
