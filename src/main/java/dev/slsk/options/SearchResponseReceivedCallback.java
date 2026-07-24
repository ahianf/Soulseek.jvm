// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.options;

/** Receives accepted search responses. */
@FunctionalInterface
public interface SearchResponseReceivedCallback {
    /**
     * Handles a received search response.
     *
     * @param received the response and search data
     */
    void onResponseReceived(SearchResponseReceived received);
}
