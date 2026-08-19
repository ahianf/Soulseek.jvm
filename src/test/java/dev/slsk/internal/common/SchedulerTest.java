// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SchedulerTest {

    @Test
    @DisplayName("a rejected dispatch does not wedge the fixed-rate tick")
    void fixedRateTickSurvivesWorkerRejection() throws Exception {
        RejectingWorker worker = new RejectingWorker();
        CountDownLatch ran = new CountDownLatch(1);
        try (Scheduler scheduler = new Scheduler("scheduler-test", worker)) {
            worker.rejecting = true;
            ScheduledFuture<?> handle = scheduler.scheduleAtFixedRate(ran::countDown, 0, 10, TimeUnit.MILLISECONDS);

            // At least one tick must be rejected while the worker is closed;
            // that is the tick that used to leave the running flag stuck.
            assertTrue(worker.rejected.await(5, TimeUnit.SECONDS), "no tick was ever rejected");

            worker.rejecting = false;
            assertTrue(ran.await(5, TimeUnit.SECONDS), "the tick never ran once the worker recovered");
            handle.cancel(false);
        } finally {
            worker.shutdownNow();
        }
    }

    /** Delegates to a real executor, but rejects while the flag is up. */
    private static final class RejectingWorker extends AbstractExecutorService {
        private final ExecutorService delegate = Executors.newVirtualThreadPerTaskExecutor();
        final CountDownLatch rejected = new CountDownLatch(1);
        volatile boolean rejecting;

        @Override
        public void execute(Runnable command) {
            if (rejecting) {
                rejected.countDown();
                throw new RejectedExecutionException("closed for the test");
            }
            delegate.execute(command);
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            return delegate.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }
    }
}
