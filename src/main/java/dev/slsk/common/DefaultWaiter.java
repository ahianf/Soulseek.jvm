// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.common;

import dev.slsk.CancellationSignal;
import dev.slsk.CancellationSubscription;
import dev.slsk.exceptions.SoulseekClientException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
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
            wait.future.cancel(false);
        }
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
            wait.future.cancel(false);
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
                    + "waitAsync() and complete()");
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
            wait.future.completeExceptionally(exception);
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
            wait.future.completeExceptionally(
                    new TimeoutException("The wait timed out after " + wait.timeout + " milliseconds"));
        }
    }

    /**
     * Adds a void wait with the default timeout.
     */
    public CompletableFuture<Void> waitAsync(WaitKey key) {
        return waitAsync(key, (Integer) null, null);
    }

    /**
     * Adds a void wait.
     */
    public CompletableFuture<Void> waitAsync(WaitKey key, Integer timeout) {
        return waitAsync(key, timeout, null);
    }

    /**
     * Adds a void wait.
     */
    public CompletableFuture<Void> waitAsync(WaitKey key, Integer timeout, CancellationSignal cancellationSignal) {
        return waitAsync(key, Void.class, timeout, cancellationSignal);
    }

    /**
     * Adds a typed wait with the default timeout.
     */
    public <T> CompletableFuture<T> waitAsync(WaitKey key, Class<T> resultType) {
        return waitAsync(key, resultType, null, null);
    }

    /**
     * Adds a typed wait.
     */
    public <T> CompletableFuture<T> waitAsync(WaitKey key, Class<T> resultType, Integer timeout) {
        return waitAsync(key, resultType, timeout, null);
    }

    /**
     * Adds a typed wait.
     */
    public <T> CompletableFuture<T> waitAsync(
            WaitKey key, Class<T> resultType, Integer timeout, CancellationSignal cancellationSignal) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(resultType, "resultType");
        int effectiveTimeout = timeout == null ? defaultTimeout : timeout;
        if (effectiveTimeout < -1) {
            throw new IllegalArgumentException("timeout must be greater than or equal to -1");
        }
        CancellationSignal effectiveToken = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
        PendingWait<T> wait =
                new PendingWait<>(resultType, effectiveTimeout, () -> cancel(key), () -> timeout(key), effectiveToken);

        synchronized (this) {
            if (closed) {
                return CompletableFuture.failedFuture(new IllegalStateException("The waiter is closed"));
            }
            waits.computeIfAbsent(key, ignored -> new ArrayDeque<>()).addLast(wait);
        }

        wait.future.whenComplete((result, exception) -> {
            if (wait.future.isCancelled()) {
                remove(wait, key);
            }
        });
        wait.register(scheduler);
        return wait.future;
    }

    /**
     * Adds a void wait that uses the source's maximum timeout.
     */
    public CompletableFuture<Void> waitIndefinitelyAsync(WaitKey key) {
        return waitIndefinitelyAsync(key, (CancellationSignal) null);
    }

    /**
     * Adds a void wait that uses the source's maximum timeout.
     */
    public CompletableFuture<Void> waitIndefinitelyAsync(WaitKey key, CancellationSignal cancellationSignal) {
        return waitAsync(key, Void.class, Integer.MAX_VALUE, cancellationSignal);
    }

    /**
     * Adds a typed wait that uses the source's maximum timeout.
     */
    public <T> CompletableFuture<T> waitIndefinitelyAsync(WaitKey key, Class<T> resultType) {
        return waitIndefinitelyAsync(key, resultType, null);
    }

    /**
     * Adds a typed wait that uses the source's maximum timeout.
     */
    public <T> CompletableFuture<T> waitIndefinitelyAsync(
            WaitKey key, Class<T> resultType, CancellationSignal cancellationSignal) {
        return waitAsync(key, resultType, Integer.MAX_VALUE, cancellationSignal);
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

    private void remove(PendingWait<?> wait, WaitKey key) {
        synchronized (this) {
            ArrayDeque<PendingWait<?>> queue = waits.get(key);
            if (queue == null || !queue.remove(wait)) {
                return;
            }
            if (queue.isEmpty()) {
                waits.remove(key);
            }
        }
        wait.close();
    }

    @SuppressWarnings("unchecked")
    private static <T> void completeUnchecked(PendingWait<?> wait, T result) {
        ((CompletableFuture<T>) wait.future).complete(result);
    }

    /**
     * A registered pending wait.
     *
     * @param <T> the result type
     */
    static final class PendingWait<T> implements AutoCloseable {
        private final Runnable cancelAction;
        private final CancellationSignal cancellationSignal;
        private final CompletableFuture<T> future = new CompletableFuture<>();
        private final Class<T> resultType;
        private final int timeout;
        private final Runnable timeoutAction;
        private CancellationSubscription cancellationSubscription;
        private boolean closed;
        private ScheduledFuture<?> timeoutTask;

        PendingWait(
                Class<T> resultType,
                int timeout,
                Runnable cancelAction,
                Runnable timeoutAction,
                CancellationSignal cancellationSignal) {
            this.resultType = Objects.requireNonNull(resultType, "resultType");
            this.timeout = timeout;
            this.cancelAction = Objects.requireNonNull(cancelAction, "cancelAction");
            this.timeoutAction = Objects.requireNonNull(timeoutAction, "timeoutAction");
            this.cancellationSignal = Objects.requireNonNull(cancellationSignal, "cancellationSignal");
        }

        /**
         * Returns the wait future.
         */
        CompletableFuture<T> getFuture() {
            return future;
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
            // blocking continuation chained on the wait future cannot stall the
            // timer thread and every other wait behind it.
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
