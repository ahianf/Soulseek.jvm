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
 * <p>{@link #await()} presents a failure the way {@code join()} did — a
 * {@link java.util.concurrent.CancellationException} raw, everything else
 * inside a {@link java.util.concurrent.CompletionException} — so the call
 * sites that read failures with {@link Failures#unwrap} and an
 * {@code instanceof} did not have to change when the future underneath became
 * a handoff cell.
 *
 * @param <T> the answer type
 */
@FunctionalInterface
public interface Wait<T> {

    /**
     * Waits for this wait's answer.
     *
     * <p>Uninterruptibly, restoring the interrupt on the way out. An interrupt
     * belongs to whatever the caller does next; cancellation of a wait arrives
     * through its {@link dev.slsk.CancellationSignal}, not through the
     * waiting thread.
     *
     * @return the answer, or {@code null} for a wait with no value
     */
    T await();
}
