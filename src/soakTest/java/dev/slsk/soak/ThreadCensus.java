// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.soak;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Counts platform threads, by name, for the scenarios that assert the library
 * uses a bounded number of them.
 *
 * <p>Virtual threads are deliberately invisible here. {@code
 * Thread.getAllStackTraces()} reports platform threads only, which is exactly
 * the population under test: the fork's thesis is that connection count should
 * drive virtual-thread count and never platform-thread count.
 */
public final class ThreadCensus {

    /**
     * Name prefixes the library uses for its platform threads.
     *
     * <p>{@code soulseek-connection-timer} is gone from this list because it is
     * gone from the library: the connection sweep ran on a static two-thread
     * pool of that name and now runs on the client's own
     * {@code soulseek-client-timer}, which this already counts. Those were the
     * two platform threads a JVM with no client open still paid for.
     */
    public static final List<String> LIBRARY_PREFIXES = List.of(
            "soulseek-network-",
            "soulseek-client-timer",
            "soulseek-client-cleanup",
            "soulseek-distributed-status",
            "soulseek-search-timeout-",
            "soulseek-token-bucket",
            "soulseek-waiter-timeouts");

    private ThreadCensus() {}

    /** Returns the current platform threads. */
    public static Set<Thread> platformThreads() {
        return Thread.getAllStackTraces().keySet();
    }

    /** Returns the total platform thread count. */
    public static int platformThreadCount() {
        return platformThreads().size();
    }

    /** Returns the number of platform threads whose name starts with the prefix. */
    public static int countByPrefix(String prefix) {
        return (int) platformThreads().stream()
                .map(Thread::getName)
                .filter(name -> name.startsWith(prefix))
                .count();
    }

    /** Returns the names of platform threads matching the prefix, for failure messages. */
    public static List<String> namesByPrefix(String prefix) {
        return platformThreads().stream()
                .map(Thread::getName)
                .filter(name -> name.startsWith(prefix))
                .sorted()
                .toList();
    }

    /** Returns a per-prefix count of the library's platform threads. */
    public static Map<String, Integer> libraryCensus() {
        Map<String, Integer> census = new TreeMap<>();
        for (String prefix : LIBRARY_PREFIXES) {
            census.put(prefix, countByPrefix(prefix));
        }
        return census;
    }

    /** Returns the total number of library platform threads across all known prefixes. */
    public static int libraryThreadCount() {
        return libraryCensus().values().stream().mapToInt(Integer::intValue).sum();
    }

    /** Renders the current library census for a failure message. */
    public static String describe() {
        return libraryCensus().entrySet().stream()
                .filter(entry -> entry.getValue() > 0)
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(", ", "{", "}"));
    }

    /**
     * Waits for the library's platform thread count to fall to or below a
     * bound. Shutdown is asynchronous, so leak assertions need a grace period
     * rather than an immediate read.
     *
     * @return the final count, whether or not it met the bound
     */
    public static int awaitLibraryThreadsAtMost(int bound, long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        int count = libraryThreadCount();
        while (count > bound && System.nanoTime() < deadline) {
            Thread.sleep(25);
            count = libraryThreadCount();
        }
        return count;
    }

    /**
     * Waits for the platform threads matching a prefix to fall to or below a
     * bound.
     *
     * @return the final count, whether or not it met the bound
     */
    public static int awaitCountAtMost(String prefix, int bound, long timeout, TimeUnit unit)
            throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        int count = countByPrefix(prefix);
        while (count > bound && System.nanoTime() < deadline) {
            Thread.sleep(25);
            count = countByPrefix(prefix);
        }
        return count;
    }
}
