// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

/**
 * Shared virtual-thread executor for blocking network I/O.
 *
 * <p>The C# source performs socket reads and writes with genuinely
 * non-blocking overlapped I/O ({@code NetworkStream.ReadAsync}), so an idle
 * connection holds no thread. This Java port implements those operations with
 * blocking {@code InputStream}/{@code OutputStream} calls. Running them on the
 * default {@link java.util.concurrent.ForkJoinPool#commonPool() common pool}
 * pins one bounded worker per connection that is parked waiting for its next
 * message; once the pool's parallelism ({@code availableProcessors - 1}) is
 * reached, every further socket read is starved. Peer, distributed and
 * transfer connections all sit in a continuous read loop, so a handful of
 * connections is enough to stall the whole client: searches return only a few
 * responses and downloads never establish their transfer connection.
 *
 * <p>Virtual threads restore the source behaviour. A blocking read on a virtual
 * thread unmounts its carrier while it waits, so thousands of idle connections
 * cost almost nothing. All blocking socket accept/connect/read/write work and
 * the per-connection read loop are dispatched here instead of on the common
 * pool.
 */
public final class NetworkExecutor {
    private static final ExecutorService EXECUTOR = Executors.newThreadPerTaskExecutor(virtualThreadFactory());

    private NetworkExecutor() {}

    /**
     * Returns the shared virtual-thread executor used for blocking network I/O.
     *
     * @return the executor
     */
    public static ExecutorService executor() {
        return EXECUTOR;
    }

    /**
     * Runs a blocking task off the calling thread and reports what it throws.
     *
     * <p>The failure handler is not optional, and that is the point of this
     * existing. What this replaced was {@code CompletableFuture.runAsync} into a
     * future the caller discarded: the dispatch was right — a read loop must not
     * wait for a write, a connect or a share catalog — but the future was the
     * only thing carrying the failure back, and discarding it dropped the
     * failure with it. Whoever dispatches now says where the failure goes.
     *
     * @param task the task to run
     * @param onFailure what to do with anything it throws
     */
    public static void dispatch(IoTask task, Consumer<Throwable> onFailure) {
        EXECUTOR.execute(() -> {
            try {
                task.run();
            } catch (Throwable failure) {
                onFailure.accept(failure);
            }
        });
    }

    /** A dispatched blocking task; checked failures go to the dispatch handler. */
    @FunctionalInterface
    public interface IoTask {
        void run() throws Exception;
    }

    private static ThreadFactory virtualThreadFactory() {
        return Thread.ofVirtual().name("soulseek-network-", 0).factory();
    }
}
