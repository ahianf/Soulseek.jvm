// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.transfer;

/** The mutually exclusive lifecycle phase of a file transfer. */
public enum TransferPhase {
    /** No transfer phase has been assigned. */
    NONE,
    /** The transfer was requested. */
    REQUESTED,
    /** The transfer is queued locally or remotely. */
    QUEUED,
    /** The transfer connection is being prepared. */
    INITIALIZING,
    /** Bytes are moving. */
    IN_PROGRESS,
    /** The transfer has ended; its termination says why. */
    COMPLETED
}
