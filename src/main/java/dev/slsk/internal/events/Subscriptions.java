// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.events;

import dev.slsk.Subscription;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** Creates idempotent subscription handles for internal listener collections. */
public final class Subscriptions {
    private Subscriptions() {}

    /** Registers {@code listener} and returns the handle that removes it. */
    public static <T> Subscription add(Collection<T> listeners, T listener) {
        Objects.requireNonNull(listeners, "listeners");
        Objects.requireNonNull(listener, "listener");
        listeners.add(listener);
        AtomicBoolean closed = new AtomicBoolean();
        return () -> {
            if (closed.compareAndSet(false, true)) {
                listeners.remove(listener);
            }
        };
    }
}
