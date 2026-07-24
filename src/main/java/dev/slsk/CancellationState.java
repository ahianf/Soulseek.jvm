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
    private boolean cancellationRequested;
    private boolean closed;

    CancellationState(boolean cancellable) {
        this.cancellable = cancellable;
    }

    synchronized boolean isCancellationRequested() {
        return cancellationRequested;
    }

    CancellationSubscription register(Runnable listener) {
        boolean runImmediately;
        Subscription subscription = null;

        synchronized (this) {
            if (!cancellable) {
                return EMPTY_SUBSCRIPTION;
            }
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
