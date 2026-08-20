// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.transfer;

/** Which side is currently queueing a transfer. */
public enum TransferQueueLocation {
    /** The local client is queueing the transfer. */
    LOCAL,
    /** The remote peer is queueing the transfer. */
    REMOTE
}
