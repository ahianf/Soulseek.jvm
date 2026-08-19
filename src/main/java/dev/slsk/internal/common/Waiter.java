// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import dev.slsk.internal.concurrent.CancellationSignal;
import java.time.Duration;

/**
 * Correlates responses with keyed waits.
 *
 * <p>Registration is separate from waiting, and the ten {@code waitAsync}
 * overloads that used to be here are three: a wait is a key, an expected type,
 * a deadline and a cancellation signal, and the rest were C# default parameters
 * kept as Java source.
 */
public interface Waiter extends AutoCloseable {
    Duration getDefaultTimeout();

    void cancel(WaitKey key);

    void cancelAll();

    void complete(WaitKey key);

    <T> void complete(WaitKey key, T result);

    boolean hasWait(WaitKey key);

    void fail(WaitKey key, Throwable exception);

    void timeout(WaitKey key);

    /**
     * Registers a wait for a typed answer.
     *
     * <p>The request that provokes the answer must be written <em>after</em>
     * this returns, or the answer can arrive before anything is listening for
     * it.
     *
     * @param key correlates the answer
     * @param resultType the expected answer type; {@code Void.class} for none
     * @param timeout the deadline, or {@code null} for none
     * @param cancellationSignal cancels the wait, or {@code null}
     * @param <T> the answer type
     * @return the registered wait
     */
    <T> Wait<T> register(WaitKey key, Class<T> resultType, Duration timeout, CancellationSignal cancellationSignal);

    /** Registers a wait for an answer that carries no value. */
    default Wait<Void> register(WaitKey key, Duration timeout, CancellationSignal cancellationSignal) {
        return register(key, Void.class, timeout, cancellationSignal);
    }

    /** Registers a wait that never times out, and schedules no timer. */
    default <T> Wait<T> registerIndefinitely(WaitKey key, Class<T> resultType, CancellationSignal cancellationSignal) {
        return register(key, resultType, null, cancellationSignal);
    }

    @Override
    void close();
}
