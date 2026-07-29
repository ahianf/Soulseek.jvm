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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Guards the property that lets {@link NetworkExecutor} stay static.
 *
 * <p>The executor is shared by every client in the process, and that is safe
 * only while it is thread-per-task over virtual threads: no workers, no queue,
 * no bounded parallelism, so two clients have nothing to contend over. If it is
 * ever changed to a bounded executor, one client's saturated connections would
 * starve another's — the exact failure its own javadoc was written about. These
 * tests fail loudly, by name, at the moment someone makes that change.
 *
 * <p>None of them shuts the executor down. It is a JVM-wide singleton, and
 * closing it here would poison every test that runs after this one — which is
 * itself the harmful side of static sharing, exercised in reverse.
 */
@Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class NetworkExecutorTest {

    @Test
    void runsEveryTaskOnAFreshNamedVirtualThread() throws Exception {
        AtomicReference<Thread> observed = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);

        NetworkExecutor.dispatch(
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
                NetworkExecutor.executor().execute(() -> {
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
                    "NetworkExecutor could not run " + tasks + " blocked tasks concurrently: it has "
                            + "become a bounded executor, and static sharing across clients is now "
                            + "harmful (see the D15 amendment). Make it per-client before bounding it.");
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

        NetworkExecutor.dispatch(
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

    private static Thread runOneTask() throws Exception {
        AtomicReference<Thread> observed = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        NetworkExecutor.executor().execute(() -> {
            observed.set(Thread.currentThread());
            done.countDown();
        });
        assertTrue(done.await(5, TimeUnit.SECONDS), "the task never ran");
        // Let the thread finish its task so identity comparison is meaningful.
        observed.get().join(TimeUnit.SECONDS.toMillis(5));
        return observed.get();
    }
}
