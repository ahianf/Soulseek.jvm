// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

/**
 * Receives transfer state changes.
 */
@FunctionalInterface
public interface TransferStateChangedCallback {
    /**
     * Handles a transfer state change.
     *
     * @param change the state-change data
     */
    void onStateChanged(TransferStateChange change);
}
