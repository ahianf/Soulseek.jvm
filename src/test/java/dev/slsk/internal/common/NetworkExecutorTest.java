// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Guards the properties of one client-owned {@link NetworkExecutor}.
 *
 * <p>Each instance remains thread-per-task over virtual threads, while its
 * lifecycle is isolated from every other client in the JVM.
 */
@Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class NetworkExecutorTest {
    private final NetworkExecutor networkExecutor = new NetworkExecutor();

    @AfterEach
    void closeExecutor() {
        networkExecutor.close();
    }

    @Test
    void runsEveryTaskOnAFreshNamedVirtualThread() throws Exception {
        AtomicReference<Thread> observed = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);

        networkExecutor.dispatch(
                () -> {
                    observed.set(Thread.currentThread());
                    done.countDown();
                },
                failure -> done.countDown());

        assertTrue(done.await(5, TimeUnit.SECONDS), "the dispatched task never ran");
        Thread thread = observed.get();
        assertTrue(thread.isVirtual(), "network tasks must run on virtual threads");
        assertTrue(thread.getName().startsWith("soulseek-network-"), "unexpected thread name: " + thread.getName());
    }

    @Test
    void isNotBounded() throws Exception {
        // A bounded executor with fewer than N workers can never have all N of
        // these blocked tasks started at once; thread-per-task always can. N is
        // far above any plausible platform pool size.
        int tasks = 4 * Runtime.getRuntime().availableProcessors() + 64;
        CountDownLatch allStarted = new CountDownLatch(tasks);
        CountDownLatch release = new CountDownLatch(1);
        Set<Thread> threads = ConcurrentHashMap.newKeySet();

        try {
            for (int i = 0; i < tasks; i++) {
                networkExecutor.executor().execute(() -> {
                    threads.add(Thread.currentThread());
                    allStarted.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            assertTrue(
                    allStarted.await(5, TimeUnit.SECONDS),
                    "NetworkExecutor could not run " + tasks + " blocked tasks concurrently");
            assertEquals(tasks, threads.size(), "expected one fresh thread per task");
        } finally {
            release.countDown();
        }
    }

    @Test
    void doesNotReuseThreadsAcrossSequentialTasks() throws Exception {
        // Distinguishes thread-per-task from an unbounded *pooled* executor,
        // which would pass the concurrency check but keep workers alive.
        assertNotSame(runOneTask(), runOneTask(), "a completed task's thread must die with it");
    }

    @Test
    void dispatchReportsWhatTheTaskThrows() throws Exception {
        RuntimeException thrown = new RuntimeException("boom");
        AtomicReference<Throwable> reported = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);

        networkExecutor.dispatch(
                () -> {
                    throw thrown;
                },
                failure -> {
                    reported.set(failure);
                    done.countDown();
                });

        assertTrue(done.await(5, TimeUnit.SECONDS), "the failure handler never ran");
        assertSame(thrown, reported.get());
    }

    @Test
    void closingOneClientExecutorDoesNotPoisonAnother() throws Exception {
        NetworkExecutor other = new NetworkExecutor();
        try {
            networkExecutor.close();
            CountDownLatch ran = new CountDownLatch(1);
            other.executor().execute(ran::countDown);
            assertTrue(ran.await(5, TimeUnit.SECONDS));
        } finally {
            other.close();
        }
    }

    private Thread runOneTask() throws Exception {
        AtomicReference<Thread> observed = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        networkExecutor.executor().execute(() -> {
            observed.set(Thread.currentThread());
            done.countDown();
        });
        assertTrue(done.await(5, TimeUnit.SECONDS), "the task never ran");
        // Let the thread finish its task so identity comparison is meaningful.
        observed.get().join(TimeUnit.SECONDS.toMillis(5));
        return observed.get();
    }
}
