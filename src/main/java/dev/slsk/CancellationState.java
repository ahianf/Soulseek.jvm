// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared mutable state behind a cancellation signal and its controller.
 */
final class CancellationState {
    private static final CancellationSubscription EMPTY_SUBSCRIPTION = () -> {};

    private final boolean cancellable;
    private final Map<Subscription, Runnable> listeners = new LinkedHashMap<>();

    /**
     * Volatile rather than monitor-guarded because the read is on the hottest
     * path in the library and the state it guards is a single boolean.
     *
     * <p>{@code CancellationSignal.none()} is a process-wide singleton, so a
     * synchronized read here serialised every connection in every client on one
     * monitor: three checks per buffer chunk in the connection read loop, plus
     * two or three more per stream operation. Measured at eight threads that
     * was 15.4x slower than uncontended, and 5.1x slower than the identical
     * check on a per-instance signal.
     *
     * <p>Writers still take the monitor, so the flag flip stays ordered against
     * the listener map: {@link #cancel()} sets it and drains the map while
     * holding the lock, and {@link #register(Runnable)} reads it under the same
     * lock before deciding whether to enqueue or run immediately. A listener
     * therefore cannot be added into a map that is already being drained.
     */
    private volatile boolean cancellationRequested;

    private boolean closed;

    CancellationState(boolean cancellable) {
        this.cancellable = cancellable;
    }

    boolean isCancellationRequested() {
        return cancellationRequested;
    }

    CancellationSubscription register(Runnable listener) {
        // Short-circuit before synchronizing. A non-cancellable state can never
        // fire, so taking the shared singleton's monitor only to return the
        // empty subscription is pure contention.
        if (!cancellable) {
            return EMPTY_SUBSCRIPTION;
        }

        boolean runImmediately;
        Subscription subscription = null;

        synchronized (this) {
            if (closed) {
                throw new IllegalStateException("The cancellation signal source is closed");
            }

            runImmediately = cancellationRequested;
            if (!runImmediately) {
                subscription = new Subscription(this);
                listeners.put(subscription, listener);
            }
        }

        if (runImmediately) {
            listener.run();
            return EMPTY_SUBSCRIPTION;
        }
        return subscription;
    }

    void cancel() {
        List<Runnable> callbacks;

        synchronized (this) {
            if (closed) {
                throw new IllegalStateException("The cancellation signal source is closed");
            }
            if (cancellationRequested) {
                return;
            }

            cancellationRequested = true;
            callbacks = new ArrayList<>(listeners.values());
            listeners.clear();
        }

        RuntimeException failure = null;
        for (int index = callbacks.size() - 1; index >= 0; index--) {
            try {
                callbacks.get(index).run();
            } catch (RuntimeException exception) {
                if (failure == null) {
                    failure = exception;
                } else {
                    failure.addSuppressed(exception);
                }
            }
        }

        if (failure != null) {
            throw failure;
        }
    }

    synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        listeners.clear();
    }

    private synchronized void unsubscribe(Subscription subscription) {
        listeners.remove(subscription);
    }

    private static final class Subscription implements CancellationSubscription {
        private CancellationState state;

        private Subscription(CancellationState state) {
            this.state = state;
        }

        @Override
        public void close() {
            CancellationState current;

            synchronized (this) {
                current = state;
                state = null;
            }

            if (current != null) {
                current.unsubscribe(this);
            }
        }
    }
}
