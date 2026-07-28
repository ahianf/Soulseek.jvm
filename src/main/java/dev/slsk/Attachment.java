// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.util.Objects;

/**
 * A snapshot and a subscription, taken together as one atomic step.
 *
 * <p>Reading state and then subscribing has a race in the gap between the two:
 * an event that fires there is in neither the snapshot nor the stream, and it is
 * lost. Subscribing and then reading has the opposite race, delivering an event
 * that the snapshot already reflects, which for a non-idempotent projection is
 * equally wrong. Across a network this is solved with a sequence cursor and
 * best-effort replay. In one JVM it can simply be made not to happen: the state
 * is captured and the listener registered under the same lock, so the stream
 * begins exactly where the snapshot ends.
 *
 * <p>That is why this API has no replay, no sequence numbers, and no gap
 * detection. It does not need them.
 *
 * <pre>{@code
 * try (var attached = slsk.downloads().attach(sse::broadcast)) {
 *     initialRender(attached.state());   // consistent with the stream, exactly
 *     awaitShutdown();
 * }
 * }</pre>
 *
 * <p>Closing the attachment closes the subscription. The state is a value and
 * needs no cleanup.
 *
 * @param state the state at the instant the listener was registered
 * @param subscription the registered listener
 * @param <S> the snapshot type
 */
public record Attachment<S>(S state, Subscription subscription) implements AutoCloseable {

    /**
     * Validates and returns the attachment.
     *
     * @throws NullPointerException if {@code subscription} is {@code null}
     */
    public Attachment {
        Objects.requireNonNull(subscription, "subscription");
    }

    /** Unregisters the listener. Idempotent, and never throws. */
    @Override
    public void close() {
        subscription.close();
    }
}
