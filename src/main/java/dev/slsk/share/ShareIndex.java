// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.share;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * What we are sharing, and when we last looked.
 *
 * <p>These counts are what the server is told and what peers see when they
 * decide whether to serve us, so keeping them accurate is not merely cosmetic:
 * a client advertising nothing is one many peers decline to upload to.
 *
 * @param directoryCount how many directories are indexed
 * @param fileCount how many files are indexed
 * @param totalBytes their combined size
 * @param lastScan when the index was last rebuilt
 * @param status whether a scan is running
 */
public record ShareIndex(
        int directoryCount, int fileCount, long totalBytes, Optional<Instant> lastScan, ScanStatus status) {

    /** Whether a share scan is running. */
    public enum ScanStatus {
        /** Never scanned. */
        NEVER_SCANNED,
        /** A scan is running. */
        SCANNING,
        /** The index is current. */
        READY,
        /** The last scan failed; the index is whatever it was before. */
        FAILED
    }

    /** Validates and returns the index. */
    public ShareIndex {
        Objects.requireNonNull(lastScan, "lastScan");
        Objects.requireNonNull(status, "status");
    }

    /** Returns the state before anything has been scanned. */
    public static ShareIndex empty() {
        return new ShareIndex(0, 0, 0, Optional.empty(), ScanStatus.NEVER_SCANNED);
    }
}
