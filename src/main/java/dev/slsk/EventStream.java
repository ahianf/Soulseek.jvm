// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.util.function.Consumer;

/**
 * The one event abstraction: a stream of events of one facet's type.
 *
 * <p>This replaces ninety-two {@code addXListener} / {@code removeXListener}
 * methods on a single interface. Each facet exposes one stream of its own sealed
 * event type, so adding an event to the library adds a record to a sealed
 * hierarchy rather than two methods to an interface, and a consumer handling the
 * stream with a {@code switch} is told by the compiler that it has appeared.
 *
 * <p><strong>A listener that throws is contained.</strong> The exception is
 * logged at warning level and the remaining listeners
 * still run. It never propagates into a read loop or a message handler, because
 * a consumer's rendering bug must not be able to take the connection down. The
 * single exception is the private-message acknowledgement, where whether a
 * listener completed cleanly is precisely the question being asked.
 *
 * <p>Events are deltas. The state they describe is always available from the
 * facet directly, so a consumer that misses every event and polls instead is
 * degraded, not broken, and one starting cold never needs event history. Use
 * {@code attach} on the facet, which returns an {@link Attachment}, when the
 * initial state and the subscription have to agree exactly.
 *
 * @param <T> the facet's event type, always a sealed interface
 */
public interface EventStream<T> {

    /**
     * Registers a listener for every event on this stream.
     *
     * @param listener the listener
     * @return a handle that unregisters it
     * @throws NullPointerException if {@code listener} is {@code null}
     */
    Subscription subscribe(Consumer<? super T> listener);

    /**
     * Registers a listener for one concrete event type.
     *
     * <p>The alternative is a listener that opens with a {@code instanceof}
     * check and ignores everything else, which every consumer would otherwise
     * write for itself.
     *
     * @param type the event type to receive
     * @param listener the listener
     * @param <U> the event type
     * @return a handle that unregisters it
     * @throws NullPointerException if either argument is {@code null}
     */
    <U extends T> Subscription subscribe(Class<U> type, Consumer<? super U> listener);
}
