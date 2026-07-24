// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.common;

import dev.slsk.CancellationToken;
import java.util.concurrent.CompletableFuture;

/** Correlates asynchronous responses with keyed waits. */
public interface IWaiter extends AutoCloseable {
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

    CompletableFuture<Void> waitAsync(WaitKey key, Integer timeout, CancellationToken cancellationToken);

    <T> CompletableFuture<T> waitAsync(WaitKey key, Class<T> resultType);

    <T> CompletableFuture<T> waitAsync(WaitKey key, Class<T> resultType, Integer timeout);

    <T> CompletableFuture<T> waitAsync(
            WaitKey key, Class<T> resultType, Integer timeout, CancellationToken cancellationToken);

    CompletableFuture<Void> waitIndefinitelyAsync(WaitKey key);

    CompletableFuture<Void> waitIndefinitelyAsync(WaitKey key, CancellationToken cancellationToken);

    <T> CompletableFuture<T> waitIndefinitelyAsync(WaitKey key, Class<T> resultType);

    <T> CompletableFuture<T> waitIndefinitelyAsync(
            WaitKey key, Class<T> resultType, CancellationToken cancellationToken);

    @Override
    void close();
}
