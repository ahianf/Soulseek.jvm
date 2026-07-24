// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.common;

import dev.slsk.CancellationSignal;
import dev.slsk.CancellationSubscription;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Implements the token-bucket rate-limiting algorithm.
 */
public final class TokenBucket implements AutoCloseable {
    private final ScheduledExecutorService scheduler;
    private final ScheduledFuture<?> resetTask;
    private final ArrayDeque<Request> requests = new ArrayDeque<>();
    private long capacity;
    private long currentCount;
    private boolean closed;

    /**
     * Creates and starts a token bucket.
     *
     * @param capacity the bucket capacity
     * @param interval the replenishment interval in milliseconds
     */
    public TokenBucket(long capacity, int interval) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be greater than or equal to 1");
        }
        if (interval < 1) {
            throw new IllegalArgumentException("interval must be greater than or equal to 1");
        }

        this.capacity = capacity;
        currentCount = capacity;
        scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "soulseek-token-bucket");
            thread.setDaemon(true);
            return thread;
        });
        resetTask = scheduler.scheduleAtFixedRate(this::reset, interval, interval, TimeUnit.MILLISECONDS);
    }

    /**
     * Returns the bucket capacity.
     *
     * @return the capacity
     */
    public synchronized long getCapacity() {
        return capacity;
    }

    /**
     * Returns the current token count for diagnostics and tests.
     *
     * @return the current count
     */
    synchronized long getCurrentCount() {
        return currentCount;
    }

    /**
     * Asynchronously retrieves tokens.
     *
     * @param count the requested token count
     * @return a future containing the provided token count
     */
    public CompletableFuture<Integer> getAsync(int count) {
        return getAsync(count, CancellationSignal.none());
    }

    /**
     * Asynchronously retrieves tokens.
     *
     * @param count the requested token count
     * @param cancellationSignal the cancellation signal
     * @return a future containing the provided token count
     */
    public CompletableFuture<Integer> getAsync(int count, CancellationSignal cancellationSignal) {
        Objects.requireNonNull(cancellationSignal, "cancellationSignal");

        if (cancellationSignal.isCancellationRequested()) {
            return CompletableFuture.failedFuture(new CancellationException("The operation was cancelled"));
        }

        int limitedCount = Math.min(count, (int) Math.min(Integer.MAX_VALUE, getCapacity()));

        synchronized (this) {
            if (closed) {
                return closedFuture();
            }

            if (requests.isEmpty() && currentCount != 0) {
                int available = (int) Math.min(currentCount, limitedCount);
                currentCount -= available;
                return CompletableFuture.completedFuture(available);
            }

            Request request = new Request(limitedCount);
            boolean becomesActive = requests.isEmpty();
            requests.addLast(request);

            if (becomesActive) {
                request.active = true;
            } else {
                request.registration = cancellationSignal.register(() -> cancelFromToken(request));
            }

            request.future.whenComplete((result, exception) -> {
                if (request.future.isCancelled()) {
                    cancelFromFuture(request);
                }
            });
            return request.future;
        }
    }

    /**
     * Returns unused tokens to the bucket.
     *
     * @param count the tokens to return
     */
    public synchronized void returnTokens(int count) {
        currentCount += Math.min(Math.max(count, 0), capacity);
    }

    /**
     * Changes the capacity and clamps the current count.
     *
     * @param capacity the new capacity
     */
    public synchronized void setCapacity(long capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be greater than or equal to 1");
        }

        this.capacity = capacity;
        currentCount = Math.min(currentCount, capacity);
    }

    /**
     * Stops replenishment and releases all pending requests.
     */
    @Override
    public void close() {
        ArrayDeque<Request> pending;

        synchronized (this) {
            if (closed) {
                return;
            }

            closed = true;
            pending = new ArrayDeque<>(requests);
            requests.clear();
        }

        resetTask.cancel(false);
        scheduler.shutdownNow();

        IllegalStateException exception = new IllegalStateException("The token bucket is closed");
        for (Request request : pending) {
            request.closeRegistration();
            request.future.completeExceptionally(exception);
        }
    }

    private synchronized void reset() {
        if (closed) {
            return;
        }

        currentCount = capacity;
        drainRequests();
    }

    private void drainRequests() {
        while (!requests.isEmpty() && currentCount != 0) {
            Request request = requests.removeFirst();
            request.closeRegistration();

            if (request.future.isDone()) {
                activateFirstRequest();
                continue;
            }

            int available = (int) Math.min(currentCount, request.count);
            currentCount -= available;
            request.future.complete(available);
            activateFirstRequest();
        }
    }

    private void activateFirstRequest() {
        while (!requests.isEmpty()) {
            Request request = requests.getFirst();
            request.closeRegistration();
            if (request.future.isDone()) {
                requests.removeFirst();
                continue;
            }
            request.active = true;
            return;
        }
    }

    private void cancelFromToken(Request request) {
        synchronized (this) {
            if (request.active || !requests.remove(request)) {
                return;
            }
        }

        request.future.completeExceptionally(new CancellationException("The operation was cancelled"));
    }

    private void cancelFromFuture(Request request) {
        synchronized (this) {
            if (!requests.remove(request)) {
                return;
            }
            request.closeRegistration();
            if (request.active) {
                activateFirstRequest();
            }
        }
    }

    private static CompletableFuture<Integer> closedFuture() {
        return CompletableFuture.failedFuture(new IllegalStateException("The token bucket is closed"));
    }

    private static final class Request {
        private final int count;
        private final CompletableFuture<Integer> future = new CompletableFuture<>();
        private boolean active;
        private CancellationSubscription registration;

        private Request(int count) {
            this.count = count;
        }

        private void closeRegistration() {
            if (registration != null) {
                registration.close();
                registration = null;
            }
        }
    }
}
