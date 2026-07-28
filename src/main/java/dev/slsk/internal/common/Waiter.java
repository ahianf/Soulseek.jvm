// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import dev.slsk.CancellationSignal;
import java.util.concurrent.CompletableFuture;

/** Correlates asynchronous responses with keyed waits. */
public interface Waiter extends AutoCloseable {
    int getDefaultTimeout();

    void cancel(WaitKey key);

    void cancelAll();

    void complete(WaitKey key);

    <T> void complete(WaitKey key, T result);

    boolean hasWait(WaitKey key);

    void fail(WaitKey key, Throwable exception);

    void timeout(WaitKey key);

    CompletableFuture<Void> waitAsync(WaitKey key);

    CompletableFuture<Void> waitAsync(WaitKey key, Integer timeout);

    CompletableFuture<Void> waitAsync(WaitKey key, Integer timeout, CancellationSignal cancellationSignal);

    <T> CompletableFuture<T> waitAsync(WaitKey key, Class<T> resultType);

    <T> CompletableFuture<T> waitAsync(WaitKey key, Class<T> resultType, Integer timeout);

    <T> CompletableFuture<T> waitAsync(
            WaitKey key, Class<T> resultType, Integer timeout, CancellationSignal cancellationSignal);

    CompletableFuture<Void> waitIndefinitelyAsync(WaitKey key);

    CompletableFuture<Void> waitIndefinitelyAsync(WaitKey key, CancellationSignal cancellationSignal);

    <T> CompletableFuture<T> waitIndefinitelyAsync(WaitKey key, Class<T> resultType);

    <T> CompletableFuture<T> waitIndefinitelyAsync(
            WaitKey key, Class<T> resultType, CancellationSignal cancellationSignal);

    @Override
    void close();
}
