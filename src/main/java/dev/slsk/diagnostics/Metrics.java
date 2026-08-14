// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.diagnostics;

/**
 * Counters for what the client has done and is doing.
 *
 * <p>A snapshot, read synchronously and cheaply, so a metrics exporter can poll
 * it on its own schedule rather than accumulating from events and hoping it has
 * seen all of them.
 *
 * @param bytesDownloaded total bytes received across all downloads
 * @param bytesUploaded total bytes sent across all uploads
 * @param activeDownloads downloads currently moving bytes
 * @param activeUploads uploads currently moving bytes
 * @param queuedDownloads downloads waiting
 * @param queuedUploads uploads waiting
 * @param peerConnections open peer connections
 * @param activeSearches searches still running
 * @param messagesSent protocol messages written
 * @param messagesReceived protocol messages read
 */
public record Metrics(
        long bytesDownloaded,
        long bytesUploaded,
        int activeDownloads,
        int activeUploads,
        int queuedDownloads,
        int queuedUploads,
        int peerConnections,
        int activeSearches,
        long messagesSent,
        long messagesReceived) {

    /** Returns all-zero metrics. */
    public static Metrics empty() {
        return new Metrics(0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }
}
