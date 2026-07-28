// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

/**
 * Receives transfer progress updates.
 */
@FunctionalInterface
public interface TransferProgressUpdatedCallback {
    /**
     * Handles a transfer progress update.
     *
     * @param update the progress data
     */
    void onProgressUpdated(TransferProgressUpdate update);
}
