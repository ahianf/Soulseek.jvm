// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

import dev.slsk.CancellationSignal;

/**
 * Grants bytes for a connection read or write, blocking if the rate limit says
 * to wait.
 *
 * <p>Called once per chunk from inside the transfer loop. It returned a future
 * that the loop awaited on the spot, which put a future and a continuation
 * between every few kilobytes and the socket for a wait the loop's own thread
 * was doing regardless.
 */
@FunctionalInterface
public interface ConnectionGovernor {
    /**
     * Grants some or all of the requested bytes.
     *
     * @param requestedBytes how many bytes the loop wants to move next
     * @param cancellationSignal ends the wait
     * @return how many bytes it may move
     */
    int grant(int requestedBytes, CancellationSignal cancellationSignal);
}
