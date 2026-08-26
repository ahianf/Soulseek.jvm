// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.concurrent.CancellationSubscription;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Implements the token-bucket rate-limiting algorithm.
 */
public final class TokenBucket implements AutoCloseable {

    private final Scheduler scheduler;
    private final boolean ownsScheduler;

    /**
     * The single pending replenishment wake-up, or {@code null} when nothing is
     * waiting on one.
     *
     * <p>Armed only while {@link #requests} is non-empty, and aimed at the
     * instant the next token actually accrues. An idle bucket holds no timer at
     * all.
     *
     * <p>This replaced a fixed-rate tick at a tenth of the configured interval.
     * Two buckets at the 100 ms default cost 200 scheduler dispatches a second
     * — each one a fresh virtual thread — and the overwhelming majority of them
     * drained an empty queue. The sub-interval tick was there to make the
     * bucket "refill in proportion to elapsed time rather than all at once",
     * but that property comes from the elapsed-time arithmetic in
     * {@link #accrue()}, not from how often it runs; a deadline-driven wake-up
     * paces more precisely than a 10 ms quantum did.
     */
    private ScheduledFuture<?> wakeup;

    private final ArrayDeque<Request> requests = new ArrayDeque<>();
    private final long intervalNanos;
    private long capacity;
    private long currentCount;
    private long lastRefillNanos;

    /**
     * Fractional credit carried between accruals, in token-nanoseconds.
     *
     * <p>Without it, integer division starves small buckets: a capacity of 1
     * over a 25 ms interval must retain partial credit between accruals or it
     * can round every small increment down to zero and never refill at all.
     */
    private long refillCredit;

    private boolean closed;

    /**
     * Creates and starts a token bucket.
     *
     * @param capacity the bucket capacity
     * @param interval the replenishment interval
     */
    public TokenBucket(long capacity, Duration interval) {
        this(capacity, interval, null);
    }

    /**
     * Creates and starts a token bucket.
     *
     * @param capacity the bucket capacity
     * @param interval the replenishment interval
     * @param scheduler the shared scheduler, or {@code null} to own one
     */
    public TokenBucket(long capacity, Duration interval, Scheduler scheduler) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be greater than or equal to 1");
        }
        Objects.requireNonNull(interval, "interval");
        if (!interval.isPositive()) {
            throw new IllegalArgumentException("interval must be greater than zero");
        }

        this.capacity = capacity;
        try {
            this.intervalNanos = interval.toNanos();
        } catch (ArithmeticException tooLarge) {
            throw new IllegalArgumentException("interval is too large", tooLarge);
        }
        currentCount = capacity;
        lastRefillNanos = System.nanoTime();
        this.ownsScheduler = scheduler == null;
        this.scheduler = scheduler == null ? new Scheduler("soulseek-token-bucket") : scheduler;
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
     * Returns the current token count for metrics and tests.
     *
     * @return the current count
     */
    synchronized long getCurrentCount() {
        return currentCount;
    }

    /**
     * Returns how many callers are queued behind the current balance.
     *
     * <p>For tests. A queued caller used to be observable through the future it
     * was handed; a blocking one is not observable from outside at all, and the
     * ordering rules here are worth asserting deterministically rather than by
     * sleeping.
     *
     * @return the queue depth
     */
    synchronized int getQueuedCount() {
        return requests.size();
    }

    /**
     * Retrieves tokens, blocking until the bucket can grant some.
     *
     * @param count the requested token count
     * @return the granted token count
     */
    public int get(int count) {
        return get(count, CancellationSignal.none());
    }

    /**
     * Retrieves tokens, blocking until the bucket can grant some.
     *
     * <p>This returned a future, and the caller — the per-chunk loop inside a
     * connection read or write — awaited it immediately, every chunk. On a
     * metered transfer that is a future and a continuation per few kilobytes,
     * for a wait the calling thread was going to do anyway.
     *
     * <p>The queue behind it is unchanged and is the whole of the fairness rule:
     * a request that cannot be satisfied outright goes to the back, only the
     * head is served, and a request that is not at the head can be cancelled out
     * of the queue. What changed is that a queued caller parks on its own
     * request rather than on a future somebody else completes.
     *
     * @param count the requested token count
     * @param cancellationSignal the cancellation signal
     * @return the granted token count
     * @throws CancellationException if cancellation is requested first
     * @throws IllegalStateException if the bucket is closed
     */
    public int get(int count, CancellationSignal cancellationSignal) {
        Objects.requireNonNull(cancellationSignal, "cancellationSignal");

        if (cancellationSignal.isCancellationRequested()) {
            throw new CancellationException("The operation was cancelled");
        }

        int limitedCount = Math.min(count, (int) Math.min(Integer.MAX_VALUE, getCapacity()));

        Request request;
        synchronized (this) {
            if (closed) {
                throw new IllegalStateException("The token bucket is closed");
            }

            // Nothing has been adding tokens in the background, so the balance
            // is earned here, on demand, before it is read.
            accrue();

            if (requests.isEmpty() && currentCount != 0) {
                int available = (int) Math.min(currentCount, limitedCount);
                currentCount -= available;
                return available;
            }

            request = new Request(limitedCount);
            boolean becomesActive = requests.isEmpty();
            requests.addLast(request);

            if (becomesActive) {
                request.active = true;
            } else {
                request.registration = cancellationSignal.register(() -> cancelFromToken(request));
            }

            armWakeup();
        }

        return request.awaitGrant(() -> cancelInterrupted(request));
    }

    /**
     * Returns unused tokens to the bucket.
     *
     * @param count the tokens to return
     */
    public void returnTokens(int count) {
        List<Grant> grants;

        synchronized (this) {
            currentCount += Math.min(Math.max(count, 0), capacity);
            // Returned tokens can satisfy a waiter straight away. The fixed-rate
            // tick used to notice on its next pass; with no tick, the drain has
            // to happen here or a waiter sits until its accrual deadline for
            // tokens that are already in the bucket.
            grants = drainRequests();
            armWakeup();
        }

        publish(grants);
    }

    /**
     * Changes the capacity and clamps the current count.
     *
     * @param capacity the new capacity
     */
    public void setCapacity(long capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be greater than or equal to 1");
        }

        List<Grant> grants;

        synchronized (this) {
            this.capacity = capacity;
            currentCount = Math.min(currentCount, capacity);
            // A raised capacity can release a waiter immediately, and it changes
            // the accrual rate that any armed wake-up was aimed at.
            grants = drainRequests();
            armWakeup();
        }

        publish(grants);
    }

    /**
     * Stops replenishment and releases all pending requests.
     */
    @Override
    public void close() {
        ArrayDeque<Request> pending;
        ScheduledFuture<?> armed;

        synchronized (this) {
            if (closed) {
                return;
            }

            closed = true;
            pending = new ArrayDeque<>(requests);
            requests.clear();
            armed = wakeup;
            wakeup = null;
        }

        if (armed != null) {
            armed.cancel(false);
        }
        if (ownsScheduler) {
            scheduler.close();
        }

        for (Request request : pending) {
            request.closeRegistration();
            request.settle(new IllegalStateException("The token bucket is closed"));
        }
    }

    /**
     * Services the armed wake-up: accrue, release whatever the new balance
     * allows, then re-arm if anyone is still waiting.
     *
     * <p>Reached only from {@link #armWakeup()}, and only while a request is
     * queued. Every other route into the bucket accrues on demand.
     */
    private void refill() {
        List<Grant> grants;

        synchronized (this) {
            if (closed) {
                return;
            }
            // This wake-up has been consumed; armWakeup() decides whether the
            // queue still warrants another.
            wakeup = null;
            accrue();
            grants = drainRequests();
            armWakeup();
        }

        publish(grants);
    }

    /**
     * Adds the tokens that elapsed time has earned since the last accrual.
     *
     * <p>Refilling to full capacity once per interval makes
     * the transmit rate bursty: a whole interval's allowance becomes available
     * at one instant and is consumed as fast as the socket will take it. Tokens
     * are added in proportion to elapsed time instead, so a peer sees a steady
     * rate rather than a sawtooth. The average over an interval is unchanged,
     * and the wire format is untouched.
     *
     * <p>Must be called while holding the monitor.
     */
    private void accrue() {
        long now = System.nanoTime();
        long elapsedNanos = now - lastRefillNanos;
        if (elapsedNanos <= 0) {
            return;
        }
        lastRefillNanos = now;

        // A whole interval earns exactly one capacity, so anything longer fills
        // the bucket whatever the arithmetic says. Taking that shortcut also
        // keeps the multiply below from overflowing: accrual is driven by
        // demand now rather than by a tick, so an idle bucket can go hours
        // between calls and capacity is already scaled by 1024.
        if (elapsedNanos >= intervalNanos) {
            currentCount = capacity;
            refillCredit = 0;
            return;
        }

        long earned;
        try {
            long credit = Math.addExact(refillCredit, Math.multiplyExact(capacity, elapsedNanos));
            earned = credit / intervalNanos;
            refillCredit = credit % intervalNanos;
        } catch (ArithmeticException overflow) {
            BigInteger credit = BigInteger.valueOf(capacity)
                    .multiply(BigInteger.valueOf(elapsedNanos))
                    .add(BigInteger.valueOf(refillCredit));
            BigInteger[] divided = credit.divideAndRemainder(BigInteger.valueOf(intervalNanos));
            earned = divided[0].longValueExact();
            refillCredit = divided[1].longValueExact();
        }
        earned = Math.max(0, earned);
        currentCount = earned >= capacity - currentCount ? capacity : currentCount + earned;
        if (currentCount == capacity) {
            // Full: stop banking credit that would burst on the next drain.
            refillCredit = 0;
        }
    }

    /**
     * Arms one wake-up for the instant the next token accrues, or drops the
     * pending one once nothing is waiting.
     *
     * <p>{@link #drainRequests} returns having either emptied the queue or
     * emptied the bucket, so a request still queued here is waiting on accrual
     * rather than on its turn.
     *
     * <p>An already-armed wake-up is left alone. Credit only grows and the
     * capacity only moves under {@link #setCapacity}, so a standing deadline
     * can be early but never late; an early one accrues nothing, grants
     * nothing, and re-arms. That keeps this a no-op on the hot path, where
     * every granted read re-enters through {@link #get(int, CancellationSignal)}.
     *
     * <p>Must be called while holding the monitor.
     */
    private void armWakeup() {
        if (closed) {
            return;
        }

        if (requests.isEmpty()) {
            if (wakeup != null) {
                wakeup.cancel(false);
                wakeup = null;
            }
            return;
        }

        if (wakeup == null) {
            wakeup = scheduler.schedule(this::refill, nanosUntilNextToken(), TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Returns how long until the bucket earns its next whole token.
     *
     * <p>Accrual banks {@code capacity} credit per elapsed nanosecond and pays
     * out a token per {@code intervalNanos} of credit, so the wait is the
     * outstanding credit over that rate, rounded up. Never zero: the accrual
     * <p>Must be called while holding the monitor.
     */
    private long nanosUntilNextToken() {
        long needed = intervalNanos - refillCredit;
        if (needed <= 0) {
            return 1;
        }
        return Math.max(1, ((needed - 1) / capacity) + 1);
    }

    /**
     * Hands decided grants to the callers waiting for them.
     *
     * <p>Must be called after the monitor is dropped. It matters less now that
     * settling only counts a latch down, but it still wakes a waiter that will
     * immediately want this lock, and the rule that no library lock is held
     * across a handoff is worth keeping whole.
     */
    private static void publish(List<Grant> grants) {
        for (Grant grant : grants) {
            grant.request.settle(grant.amount);
        }
    }

    /**
     * Decides which pending requests the current tokens satisfy.
     *
     * <p>Returns the grants rather than applying them, so the caller can
     * complete the futures outside the lock. Must be called while holding the
     * monitor.
     */
    private List<Grant> drainRequests() {
        List<Grant> grants = new ArrayList<>();
        while (!requests.isEmpty() && currentCount != 0) {
            Request request = requests.removeFirst();
            request.closeRegistration();

            if (request.isSettled()) {
                activateFirstRequest();
                continue;
            }

            int available = (int) Math.min(currentCount, request.count);
            currentCount -= available;
            grants.add(new Grant(request, available));
            activateFirstRequest();
        }
        return grants;
    }

    /** A decided-but-not-yet-published token grant. */
    private record Grant(Request request, int amount) {}

    private void activateFirstRequest() {
        while (!requests.isEmpty()) {
            Request request = requests.getFirst();
            request.closeRegistration();
            if (request.isSettled()) {
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

        request.settle(new CancellationException("The operation was cancelled"));
    }

    /** Removes a request whose own waiting thread was interrupted. */
    private void cancelInterrupted(Request request) {
        synchronized (this) {
            if (!requests.remove(request)) {
                return;
            }
            request.closeRegistration();
            if (request.active) {
                activateFirstRequest();
            }
            armWakeup();
        }
    }

    /**
     * One caller's place in the queue, and the cell it is parked on.
     *
     * <p>This was a {@link CompletableFuture}, which gave the queue a second way
     * to be cancelled — the caller could cancel the future directly — and the
     * bucket had to watch for that and unpick the queue itself. A blocking
     * caller has no such handle, so the signal is the only route in and the
     * bookkeeping for the other one is gone.
     */
    private static final class Request {
        private final int count;
        private final Settlement<Integer> settlement = new Settlement<>();
        private boolean active;
        private CancellationSubscription registration;

        private Request(int count) {
            this.count = count;
        }

        /** Whether this request has already been decided, one way or the other. */
        private boolean isSettled() {
            return settlement.isSettled();
        }

        private boolean settle(int amount) {
            return settlement.succeed(amount);
        }

        private boolean settle(RuntimeException reason) {
            return settlement.fail(reason);
        }

        /** Blocks until this request is decided, then reports the decision. */
        private int awaitGrant(Runnable cancelAction) {
            Settlement.Outcome<Integer> outcome;
            try {
                outcome = settlement.await();
            } catch (InterruptedException interrupted) {
                CancellationException cancelled = new CancellationException("The wait for tokens was interrupted");
                if (settlement.fail(cancelled)) {
                    cancelAction.run();
                    throw cancelled;
                }
                // A grant committed first. Preserve the later interrupt for
                // the worker's enclosing transfer loop.
                try {
                    outcome = settlement.await();
                } catch (InterruptedException impossible) {
                    throw new AssertionError(impossible);
                }
                Thread.currentThread().interrupt();
            }
            if (outcome.failure() != null) {
                throw (RuntimeException) outcome.failure();
            }
            return outcome.value();
        }

        private void closeRegistration() {
            if (registration != null) {
                registration.close();
                registration = null;
            }
        }
    }
}
