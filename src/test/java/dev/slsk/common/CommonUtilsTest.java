// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.common;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CommonUtilsTest {
    @Test
    @DisplayName("DequeueAndCloseAll drains and closes every value")
    void dequeueAndCloseAllDrainsAndClosesEveryValue() {
        ConcurrentLinkedQueue<CountingCloseable> queue = new ConcurrentLinkedQueue<>();
        CountingCloseable first = new CountingCloseable();
        CountingCloseable second = new CountingCloseable();
        queue.add(first);
        queue.add(second);

        CommonUtils.dequeueAndCloseAll(queue);

        assertTrue(queue.isEmpty());
        assertEquals(1, first.closeCount.get());
        assertEquals(1, second.closeCount.get());
    }

    @Test
    @DisplayName("RemoveAndCloseAll drains and closes every value")
    void removeAndCloseAllDrainsAndClosesEveryValue() {
        ConcurrentHashMap<Integer, CountingCloseable> map = new ConcurrentHashMap<>();
        CountingCloseable first = new CountingCloseable();
        CountingCloseable second = new CountingCloseable();
        map.put(1, first);
        map.put(2, second);

        CommonUtils.removeAndCloseAll(map);

        assertTrue(map.isEmpty());
        assertEquals(1, first.closeCount.get());
        assertEquals(1, second.closeCount.get());
    }

    @Test
    @DisplayName("Close failures propagate and stop draining")
    void closeFailuresPropagateAndStopDraining() {
        ConcurrentLinkedQueue<AutoCloseable> queue = new ConcurrentLinkedQueue<>();
        RuntimeException failure = new RuntimeException("failure");
        queue.add(() -> {
            throw failure;
        });
        queue.add(() -> {});

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> CommonUtils.dequeueAndCloseAll(queue));

        assertEquals(failure, thrown);
        assertEquals(1, queue.size());
    }

    @Test
    @DisplayName("Forget accepts successful and failed futures")
    void forgetAcceptsSuccessfulAndFailedFutures() {
        assertDoesNotThrow(() -> CommonUtils.forget(CompletableFuture.completedFuture(null)));
        assertDoesNotThrow(() -> CommonUtils.forget(CompletableFuture.failedFuture(new RuntimeException("failure"))));
    }

    @Test
    @DisplayName("ToMd5Hash matches Soulseek login vectors")
    void toMd5HashMatchesVectors() {
        assertEquals("d41d8cd98f00b204e9800998ecf8427e", CommonUtils.toMd5Hash(""));
        assertEquals("5d41402abc4b2a76b9719d911017c592", CommonUtils.toMd5Hash("hello"));
        assertEquals("45bfb2ac344e0fee8b89047858fae25a", CommonUtils.toMd5Hash("påsswörd"));
        assertThrows(NullPointerException.class, () -> CommonUtils.toMd5Hash(null));
    }

    private static final class CountingCloseable implements AutoCloseable {
        private final AtomicInteger closeCount = new AtomicInteger();

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }
    }
}
