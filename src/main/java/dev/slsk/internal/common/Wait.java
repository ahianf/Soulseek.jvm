// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

/**
 * A registered correlation wait, and the handoff of its answer.
 *
 * <p>Registering and waiting are two steps on purpose. Every request in this
 * library has the shape <em>register the wait, write the request, await the
 * answer</em>, and the order matters: a peer or the server can answer before
 * the write call returns, so a wait registered afterwards would miss its own
 * reply. Handing the caller a {@code Wait} at registration is what lets the
 * write sit between the two.
 *
 * <p>{@link #await()} is a caller-facing park: interruption cancels this wait,
 * not the independently owned operation whose answer it observes.
 *
 * @param <T> the answer type
 */
@FunctionalInterface
public interface Wait<T> {

    /**
     * Waits for this wait's answer.
     *
     * @return the answer, or {@code null} for a wait with no value
     * @throws InterruptedException if the waiting thread is interrupted
     */
    T await() throws InterruptedException;
}
