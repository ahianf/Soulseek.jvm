// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

/**
 * Progress received while browsing a peer's shares.
 *
 * @param username the peer username
 * @param bytesTransferred the number of bytes received
 * @param bytesRemaining the number of remaining bytes
 * @param percentComplete the completion percentage
 * @param size the total response size
 */
public record BrowseProgressUpdate(
        String username, long bytesTransferred, long bytesRemaining, double percentComplete, long size) {}
