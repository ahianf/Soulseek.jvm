// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.connection.ConnectionState;
import dev.slsk.exceptions.LoginRejectedException;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Gets the server connection back after it drops, without the consumer asking.
 *
 * <p>{@link dev.slsk.Connection} promises that "connecting, dropping and
 * connecting again all happen underneath". This is the part that keeps the
 * promise. Until it existed a client that lost its socket stayed
 * {@link ConnectionState.Offline} until the process was restarted, and the
 * failure it stays offline for is not rare: a vanished network takes the
 * kernel's TCP retransmit budget — around five minutes with no bytes moving —
 * to surface as a read error, and by then every peer socket has gone too.
 *
 * <p><strong>Always on, with no knob.</strong> A policy on the builder was the
 * other option and is deliberately not taken: it is additive later, whereas a
 * default of "stay offline forever" is a defect every embedder has to write the
 * same loop to work around.
 *
 * <h2>What stops it</h2>
 *
 * Three things, and nothing else:
 *
 * <ul>
 *   <li>{@link LoginRejectedException} — a wrong password does not become right
 *       by waiting, and retrying it is abuse from the server's side. This is why
 *       {@code Rejected} is structurally not a kind of {@code Reconnecting}.
 *   <li>An explicit {@code disconnect()} or {@code connect()} from the consumer.
 *       Their intent outranks ours.
 *   <li>{@link #close()}.
 * </ul>
 *
 * <p>Everything else retries forever. A network that has been gone for six hours
 * is still a network that can come back, and the alternative — giving up after
 * <em>n</em> attempts — converges on exactly the behaviour this class exists to
 * remove.
 *
 * <h2>Backoff</h2>
 *
 * <p>Doubling from two seconds to a sixty-second ceiling, then flat: 2, 4, 8,
 * 16, 32, 60, 60, … Each delay is then drawn uniformly from
 * {@code [floor, computed]} — full jitter — because every client that loses a
 * connection to the same server loses it at the same instant, and an
 * unjittered schedule would have all of them knock in lockstep. The floor keeps
 * a drawn delay from collapsing to zero and busy-looping against a server that
 * is refusing instantly.
 */
final class ReconnectSupervisor implements AutoCloseable {

    /** The first delay, and the base the doubling starts from. */
    private static final Duration INITIAL_DELAY = Duration.ofSeconds(2);

    /** The ceiling the doubling stops at. */
    private static final Duration MAX_DELAY = Duration.ofSeconds(60);

    /** The smallest delay jitter may draw, so a retry never becomes a spin. */
    private static final Duration MIN_DELAY = Duration.ofMillis(500);

    /** Performs one connect attempt, blocking, throwing on failure. */
    @FunctionalInterface
    interface Connector {
        void connect() throws RuntimeException;
    }

    private final Connector connector;
    private final Runnable onStateChanged;
    private final DiagnosticSink diagnostics;

    private final Duration initialDelay;
    private final Duration maxDelay;
    private final Duration minDelay;

    /** Set while a retry thread is alive, so arming twice does not race two in. */
    private final AtomicBoolean running = new AtomicBoolean();

    private final AtomicBoolean closed = new AtomicBoolean();

    /** The retry thread, held so {@link #close()} can stop it. */
    private final AtomicReference<Thread> worker = new AtomicReference<>();

    /**
     * The state to report while waiting, or {@code null} when not waiting.
     *
     * <p>{@code DefaultConnection} maps engine bits to a {@link ConnectionState}
     * and the engine has no bit for "waiting to try again". This is the overlay
     * that supplies the one state the engine cannot describe.
     */
    private final AtomicReference<ConnectionState.Reconnecting> pending = new AtomicReference<>();

    /** Which attempt is in flight, so a {@code Connecting} can carry its number. */
    private volatile int attempt = 1;

    ReconnectSupervisor(Connector connector, Runnable onStateChanged, DiagnosticSink diagnostics) {
        this(connector, onStateChanged, diagnostics, INITIAL_DELAY, MAX_DELAY, MIN_DELAY);
    }

    /**
     * Creates a supervisor with the backoff spelled out, for tests that would
     * otherwise spend real seconds asleep.
     */
    ReconnectSupervisor(
            Connector connector,
            Runnable onStateChanged,
            DiagnosticSink diagnostics,
            Duration initialDelay,
            Duration maxDelay,
            Duration minDelay) {
        this.connector = Objects.requireNonNull(connector, "connector");
        this.onStateChanged = Objects.requireNonNull(onStateChanged, "onStateChanged");
        this.diagnostics = Objects.requireNonNull(diagnostics, "diagnostics");
        this.initialDelay = Objects.requireNonNull(initialDelay, "initialDelay");
        this.maxDelay = Objects.requireNonNull(maxDelay, "maxDelay");
        this.minDelay = Objects.requireNonNull(minDelay, "minDelay");
    }

    /**
     * Starts retrying, unless a retry is already under way.
     *
     * <p>Idempotent by design rather than by accident: a failed attempt raises
     * its own disconnect, which arrives back here while the loop that caused it
     * is still running.
     *
     * @param cause why the connection was lost
     */
    void arm(Throwable cause) {
        if (closed.get() || !running.compareAndSet(false, true)) {
            return;
        }
        Thread thread = Thread.ofVirtual().name("soulseek-reconnect").start(() -> retryUntilOnline(cause));
        worker.set(thread);
    }

    /**
     * Stops retrying and forgets that it was.
     *
     * <p>Called when the consumer connects or disconnects on its own: whatever
     * this class was about to do, the consumer has just said what it wants
     * instead.
     */
    void cancel() {
        running.set(false);
        Thread thread = worker.getAndSet(null);
        if (thread != null) {
            thread.interrupt();
        }
        if (pending.getAndSet(null) != null) {
            onStateChanged.run();
        }
        attempt = 1;
    }

    /**
     * Returns the state to report instead of {@code Offline}, if any.
     *
     * @return the pending {@code Reconnecting}, or {@code null} when not waiting
     */
    ConnectionState.Reconnecting pending() {
        return pending.get();
    }

    /**
     * Returns which attempt is in flight, for a {@code Connecting} to carry.
     *
     * @return the attempt number, counting from one
     */
    int attempt() {
        return attempt;
    }

    /**
     * Returns whether a retry loop is alive.
     *
     * @return {@code true} while retrying
     */
    boolean retrying() {
        return running.get();
    }

    private void retryUntilOnline(Throwable initialCause) {
        Throwable cause = initialCause;
        int number = 1;
        try {
            while (running.get() && !closed.get()) {
                number++;
                attempt = number;
                Duration delay = delayFor(number);
                ConnectionState.Reconnecting waiting =
                        new ConnectionState.Reconnecting(number, Instant.now().plus(delay), cause);
                pending.set(waiting);
                onStateChanged.run();
                diagnostics.info("Reconnecting to the server in " + delay.toMillis() + " ms (attempt " + number + "): "
                        + Failures.message(cause));

                Thread.sleep(delay);

                if (!running.get() || closed.get()) {
                    return;
                }
                // Cleared before the attempt, not after: the engine publishes
                // CONNECTING from inside connect(), and a stale overlay would
                // report Reconnecting over the top of it.
                pending.set(null);
                try {
                    connector.connect();
                    diagnostics.info("Reconnected to the server on attempt " + number);
                    return;
                } catch (LoginRejectedException rejected) {
                    diagnostics.warning("The server rejected the login; not reconnecting again", rejected);
                    return;
                } catch (IllegalStateException conflict) {
                    // The engine refuses a connect while one is in flight or
                    // already up. Either way someone else now owns the socket.
                    diagnostics.debug(
                            "Stopped reconnecting; the connection is no longer ours: " + Failures.message(conflict));
                    return;
                } catch (RuntimeException failure) {
                    cause = failure;
                    diagnostics.debug("Reconnect attempt " + number + " failed: " + Failures.message(failure), failure);
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } finally {
            running.set(false);
            worker.compareAndSet(Thread.currentThread(), null);
            if (pending.getAndSet(null) != null) {
                onStateChanged.run();
            }
        }
    }

    /**
     * Doubles to the ceiling, then draws uniformly from {@code [floor, computed]}.
     *
     * @param number which attempt the delay precedes, counting from one
     * @return how long to wait
     */
    Duration delayFor(int number) {
        long capped = maxDelay.toMillis();
        // Shift rather than pow, and stop shifting well before it overflows.
        int doublings = Math.min(number - 2, 32);
        if (doublings >= 0 && doublings < 32) {
            long scaled = initialDelay.toMillis() << doublings;
            if (scaled > 0) {
                capped = Math.min(capped, scaled);
            }
        }
        long floor = Math.min(minDelay.toMillis(), capped);
        return Duration.ofMillis(ThreadLocalRandom.current().nextLong(floor, capped + 1));
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        running.set(false);
        Thread thread = worker.getAndSet(null);
        if (thread != null) {
            thread.interrupt();
            try {
                // Bounded: the thread is either sleeping, which the interrupt
                // ends at once, or inside a connect that carries its own
                // timeouts. close() must not become the slowest of those.
                thread.join(Duration.ofSeconds(1));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        pending.set(null);
    }
}
