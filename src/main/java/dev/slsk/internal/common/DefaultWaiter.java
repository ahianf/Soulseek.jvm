// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.internal.concurrent.BoundedCleanup;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.concurrent.CancellationSubscription;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Correlates asynchronous responses with FIFO waits.
 */
public final class DefaultWaiter implements Waiter {
    static final int DEFAULT_TIMEOUT = 5_000;

    private final int defaultTimeout;
    private final Scheduler scheduler;
    private final boolean ownsScheduler;
    private final Map<WaitKey, ArrayDeque<PendingWait<?>>> waits = new HashMap<>();
    private boolean closed;

    /**
     * Creates a waiter with the source default timeout.
     */
    public DefaultWaiter() {
        this(DEFAULT_TIMEOUT);
    }

    /**
     * Creates a waiter that owns its scheduler.
     *
     * @param defaultTimeout the default timeout in milliseconds
     */
    public DefaultWaiter(int defaultTimeout) {
        this.defaultTimeout = defaultTimeout;
        this.scheduler = new Scheduler("soulseek-waiter-timeouts");
        this.ownsScheduler = true;
    }

    /**
     * Creates a waiter sharing a caller-owned scheduler.
     *
     * @param defaultTimeout the default timeout in milliseconds
     * @param scheduler the shared scheduler; not closed by this waiter
     */
    public DefaultWaiter(int defaultTimeout, Scheduler scheduler) {
        this.defaultTimeout = defaultTimeout;
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.ownsScheduler = false;
    }

    /**
     * Returns the default timeout.
     *
     * @return the timeout in milliseconds
     */
    public int getDefaultTimeout() {
        return defaultTimeout;
    }

    /**
     * Cancels the oldest wait for a key.
     *
     * @param key the wait key
     */
    public void cancel(WaitKey key) {
        PendingWait<?> wait = dequeue(key);
        if (wait != null) {
            wait.close();
            wait.settle(null, new CancellationException("The wait was cancelled"));
        }
    }

    /**
     * Cancels one specific wait — the one whose signal fired.
     *
     * <p>Not the oldest for the key. Keys like {@code CHECK_PRIVILEGES} or
     * {@code PLACE_IN_QUEUE_RESPONSE(user, file)} can hold two callers' waits
     * at once, and dequeuing the head handed caller B's cancellation to caller
     * A: A got a spurious {@code CancellationException} and B stayed waiting,
     * to later be settled with A's response. The C# waiter completes the
     * specific wait; so does this.
     */
    private void cancel(WaitKey key, PendingWait<?> wait) {
        if (wait.trySettle(null, new CancellationException("The wait was cancelled"))) {
            remove(key, wait);
            wait.close();
            wait.publish();
        }
    }

    /** Times out one specific wait — the one whose timer fired. */
    private void timeout(WaitKey key, PendingWait<?> wait) {
        if (wait.trySettle(null, new TimeoutException("The wait timed out after " + wait.timeout + " milliseconds"))) {
            remove(key, wait);
            wait.close();
            wait.publish();
        }
    }

    /**
     * Removes one specific wait from its queue, if it is still registered.
     *
     * @return whether it was there to remove — {@code false} means somebody
     *     else already settled it
     */
    private synchronized boolean remove(WaitKey key, PendingWait<?> wait) {
        ArrayDeque<PendingWait<?>> queue = waits.get(key);
        if (queue == null) {
            return false;
        }
        boolean removed = queue.remove(wait);
        if (queue.isEmpty()) {
            waits.remove(key);
        }
        return removed;
    }

    /**
     * Cancels all pending waits.
     */
    public void cancelAll() {
        List<PendingWait<?>> pending;

        synchronized (this) {
            pending = new ArrayList<>();
            for (ArrayDeque<PendingWait<?>> queue : waits.values()) {
                pending.addAll(queue);
            }
            waits.clear();
        }

        for (PendingWait<?> wait : pending) {
            wait.close();
            wait.settle(null, new CancellationException("The wait was cancelled"));
        }
    }

    /**
     * Completes the oldest wait for a key.
     *
     * @param key the wait key
     */
    public void complete(WaitKey key) {
        complete(key, null);
    }

    /**
     * Completes the oldest wait for a key with a result.
     *
     * @param key the wait key
     * @param result the result
     * @param <T> the result type
     */
    public <T> void complete(WaitKey key, T result) {
        PendingWait<?> wait = dequeue(key);
        if (wait == null) {
            return;
        }

        wait.close();
        if (result != null && !wait.resultType.isInstance(result)) {
            throw new SoulseekClientException("Failed to bind wait types for key " + key
                    + "; this is a mismatch between the types specified in "
                    + "register() and complete()");
        }

        completeUnchecked(wait, result);
    }

    /**
     * Returns whether the key has any pending waits.
     *
     * @param key the wait key
     * @return whether a wait exists
     */
    public synchronized boolean hasWait(WaitKey key) {
        return waits.containsKey(key);
    }

    /**
     * Fails the oldest wait for a key.
     *
     * @param key the wait key
     * @param exception the failure
     */
    public void fail(WaitKey key, Throwable exception) {
        Objects.requireNonNull(exception, "exception");
        PendingWait<?> wait = dequeue(key);
        if (wait != null) {
            wait.close();
            wait.settle(null, exception);
        }
    }

    /**
     * Times out the oldest wait for a key.
     *
     * @param key the wait key
     */
    public void timeout(WaitKey key) {
        PendingWait<?> wait = dequeue(key);
        if (wait != null) {
            wait.close();
            wait.settle(null, new TimeoutException("The wait timed out after " + wait.timeout + " milliseconds"));
        }
    }

    /**
     * Registers a wait.
     */
    public <T> Wait<T> register(
            WaitKey key, Class<T> resultType, Integer timeout, CancellationSignal cancellationSignal) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(resultType, "resultType");
        int effectiveTimeout = timeout == null ? defaultTimeout : timeout;
        if (effectiveTimeout < -1) {
            throw new IllegalArgumentException("timeout must be greater than or equal to -1");
        }
        CancellationSignal effectiveToken = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
        PendingWait<T> wait = new PendingWait<>(resultType, effectiveTimeout, effectiveToken);
        // The actions name the wait itself, not the key's queue head: another
        // caller's wait under the same key must not absorb this one's
        // cancellation or timeout.
        wait.actions(() -> cancel(key, wait), () -> timeout(key, wait), () -> {
            remove(key, wait);
            wait.close();
        });

        synchronized (this) {
            if (closed) {
                // Settled rather than thrown, because that is where the failure
                // used to arrive: a closed waiter handed back a failed future
                // and the caller met it at the await.
                wait.settle(null, new IllegalStateException("The waiter is closed"));
                return wait;
            }
            waits.computeIfAbsent(key, ignored -> new ArrayDeque<>()).addLast(wait);
        }

        wait.register(scheduler);
        return wait;
    }

    /**
     * Returns the pending count for a key for diagnostics and tests.
     */
    synchronized int getWaitCount(WaitKey key) {
        ArrayDeque<PendingWait<?>> queue = waits.get(key);
        return queue == null ? 0 : queue.size();
    }

    /**
     * Returns the number of correlation keys for diagnostics and tests.
     */
    synchronized int getKeyCount() {
        return waits.size();
    }

    /**
     * Cancels all waits and stops the timeout scheduler.
     */
    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }

        cancelAll();
        if (ownsScheduler) {
            scheduler.close();
        }
    }

    private synchronized PendingWait<?> dequeue(WaitKey key) {
        ArrayDeque<PendingWait<?>> queue = waits.get(key);
        if (queue == null) {
            return null;
        }

        PendingWait<?> wait = queue.pollFirst();
        if (queue.isEmpty()) {
            waits.remove(key);
        }
        return wait;
    }

    @SuppressWarnings("unchecked")
    private static <T> void completeUnchecked(PendingWait<?> wait, T result) {
        ((PendingWait<T>) wait).settle(result, null);
    }

    /**
     * A registered pending wait, and the cell its answer is handed off in.
     *
     * <p>Exactly one caller settles it, because settling is only reachable
     * through {@link DefaultWaiter#dequeue}, which takes it out of the registry
     * under the waiter's lock. The latch is what publishes the answer to
     * whoever is waiting.
     *
     * @param <T> the result type
     */
    static final class PendingWait<T> implements Wait<T>, AutoCloseable {
        private final CancellationSignal cancellationSignal;
        private final Settlement<T> settlement = new Settlement<>();
        private final Class<T> resultType;
        private final int timeout;
        private Runnable cancelAction;
        private Runnable timeoutAction;
        private CancellationSubscription cancellationSubscription;
        private Runnable interruptionCleanup;
        private boolean closed;
        private ScheduledFuture<?> timeoutTask;

        PendingWait(Class<T> resultType, int timeout, CancellationSignal cancellationSignal) {
            this.resultType = Objects.requireNonNull(resultType, "resultType");
            this.timeout = timeout;
            this.cancellationSignal = Objects.requireNonNull(cancellationSignal, "cancellationSignal");
        }

        /**
         * Names what cancellation and timeout do. Set after construction —
         * both actions reference the wait itself — and before {@link
         * #register}, which is what arms them.
         */
        void actions(Runnable cancelAction, Runnable timeoutAction, Runnable interruptionCleanup) {
            this.cancelAction = Objects.requireNonNull(cancelAction, "cancelAction");
            this.timeoutAction = Objects.requireNonNull(timeoutAction, "timeoutAction");
            this.interruptionCleanup = Objects.requireNonNull(interruptionCleanup, "interruptionCleanup");
        }

        @Override
        public T await() throws InterruptedException, TimeoutException {
            Settlement.Outcome<T> settledOutcome;
            try {
                settledOutcome = settlement.await();
            } catch (InterruptedException interrupted) {
                if (trySettle(null, interrupted)) {
                    BoundedCleanup.afterInterruption(interruptionCleanup, interrupted);
                    publish();
                    throw interrupted;
                }
                // A result was committed first. Wait only for its publication,
                // then return it with this later interrupt still visible to the
                // caller's enclosing work.
                settledOutcome = settlement.await();
                Thread.currentThread().interrupt();
            }
            if (settledOutcome.failure() != null) {
                throw Failures.rethrow(settledOutcome.failure());
            }
            return settledOutcome.value();
        }

        /** Commits and publishes an answer in one ordinary settlement path. */
        void settle(T value, Throwable error) {
            if (trySettle(value, error)) {
                publish();
            }
        }

        /** Atomically selects this outcome without publishing it yet. */
        boolean trySettle(T value, Throwable error) {
            return settlement.trySettle(value, error);
        }

        /** Publishes the already committed outcome. */
        void publish() {
            settlement.publish();
        }

        /**
         * Returns the timeout in milliseconds.
         */
        int getTimeout() {
            return timeout;
        }

        /**
         * Registers cancellation and timeout actions.
         */
        void register(Scheduler scheduler) {
            CancellationSubscription registration = cancellationSignal.register(cancelAction);

            synchronized (this) {
                if (closed) {
                    registration.close();
                    return;
                }
                cancellationSubscription = registration;
            }

            if (timeout == -1) {
                return;
            }

            // Scheduler dispatches every task onto a virtual thread, so a
            // waiter released by the timeout cannot stall the timer thread and
            // every other wait behind it.
            ScheduledFuture<?> task = scheduler.schedule(timeoutAction, timeout, TimeUnit.MILLISECONDS);
            synchronized (this) {
                if (closed) {
                    task.cancel(false);
                } else {
                    timeoutTask = task;
                }
            }
        }

        /**
         * Releases cancellation and timeout registrations.
         */
        @Override
        public void close() {
            CancellationSubscription registration;
            ScheduledFuture<?> task;

            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                registration = cancellationSubscription;
                cancellationSubscription = null;
                task = timeoutTask;
                timeoutTask = null;
            }

            if (registration != null) {
                registration.close();
            }
            if (task != null) {
                task.cancel(false);
            }
        }
    }
}
