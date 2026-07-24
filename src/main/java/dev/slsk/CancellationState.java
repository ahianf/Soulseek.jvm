// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared mutable state behind a cancellation token and its source.
 */
final class CancellationState {
    private static final CancellationRegistration EMPTY_REGISTRATION = () -> {};

    private final boolean cancellable;
    private final Map<Registration, Runnable> listeners = new LinkedHashMap<>();
    private boolean cancellationRequested;
    private boolean closed;

    CancellationState(boolean cancellable) {
        this.cancellable = cancellable;
    }

    synchronized boolean isCancellationRequested() {
        return cancellationRequested;
    }

    CancellationRegistration register(Runnable listener) {
        boolean runImmediately;
        Registration registration = null;

        synchronized (this) {
            if (!cancellable) {
                return EMPTY_REGISTRATION;
            }
            if (closed) {
                throw new IllegalStateException("The cancellation token source is closed");
            }

            runImmediately = cancellationRequested;
            if (!runImmediately) {
                registration = new Registration(this);
                listeners.put(registration, listener);
            }
        }

        if (runImmediately) {
            listener.run();
            return EMPTY_REGISTRATION;
        }
        return registration;
    }

    void cancel() {
        List<Runnable> callbacks;

        synchronized (this) {
            if (closed) {
                throw new IllegalStateException("The cancellation token source is closed");
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

    private synchronized void unregister(Registration registration) {
        listeners.remove(registration);
    }

    private static final class Registration implements CancellationRegistration {
        private CancellationState state;

        private Registration(CancellationState state) {
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
                current.unregister(this);
            }
        }
    }
}
