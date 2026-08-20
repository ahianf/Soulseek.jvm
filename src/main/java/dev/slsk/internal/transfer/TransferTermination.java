// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.transfer;

/** Why a completed transfer ended. */
public enum TransferTermination {
    /** Every requested byte transferred. */
    SUCCEEDED,
    /** The caller cancelled the transfer. */
    CANCELLED,
    /** A transfer deadline elapsed. */
    TIMED_OUT,
    /** An unexpected failure ended the transfer. */
    ERRORED,
    /** The peer rejected the transfer. */
    REJECTED,
    /** An unrecoverable protocol or data mismatch ended the transfer. */
    ABORTED
}
