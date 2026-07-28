// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Regression guard for the async-to-blocking-on-a-bounded-pool bug class.
 *
 * <p>The C# source awaits socket and correlation operations without holding a
 * thread. This port implements those waits by blocking a thread and completing
 * a {@link java.util.concurrent.CompletableFuture}. When such blocking work is
 * dispatched on {@link java.util.concurrent.ForkJoinPool#commonPool()} — the
 * implicit executor of the single-argument {@code CompletableFuture.supplyAsync}
 * and {@code CompletableFuture.runAsync} overloads — every parked connection
 * pins one of the pool's {@code availableProcessors - 1} workers and starves
 * all other reads. This is invisible to the rest of the suite because those
 * tests use only a couple of connections, well under the pool's parallelism.
 *
 * <p>The convention is therefore: main code must never use the no-executor
 * overloads. Blocking work is routed through {@link NetworkExecutor} (virtual
 * threads); when the two-argument overload is used it must name an explicit
 * executor. This test scans the main sources and fails on any
 * {@code CompletableFuture.supplyAsync}/{@code runAsync} call that supplies no
 * executor.
 */
class CommonPoolDispatchGuardTest {
    private static final Path MAIN_SOURCES = Path.of("src", "main", "java");

    @Test
    @DisplayName("main code never dispatches blocking work on the common pool")
    void mainCodeNeverUsesNoExecutorCompletableFutureOverloads() {
        assertTrue(
                Files.isDirectory(MAIN_SOURCES),
                "Expected to run from the module root with " + MAIN_SOURCES + " present");

        List<String> violations = new ArrayList<>();
        try (Stream<Path> files = Files.walk(MAIN_SOURCES)) {
            files.filter(path -> path.toString().endsWith(".java"))
                    .forEach(path -> violations.addAll(findViolations(readString(path), path.toString())));
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }

        if (!violations.isEmpty()) {
            fail("Blocking work must not be dispatched on ForkJoinPool.commonPool(); "
                    + "route it through NetworkExecutor or pass an explicit executor.\n  "
                    + String.join("\n  ", violations));
        }
    }

    @Test
    @DisplayName("the guard detects a no-executor call and ignores an explicit-executor call")
    void detectorFlagsOnlyTheNoExecutorOverload() {
        String bad = "x = CompletableFuture.supplyAsync(() -> read());"
                + "a = future.thenApplyAsync(value -> read(value));"
                + "b = future.thenCombineAsync(other, (l, r) -> read(l, r));"
                + "c = future.whenCompleteAsync((value, failure) -> close());";
        String good = "x = CompletableFuture.supplyAsync(() -> read(), executor);"
                + "y = CompletableFuture.runAsync(task, delayedExecutor(100, MILLISECONDS));"
                + "z = NetworkExecutor.supplyAsync(() -> read());"
                + "a = future.thenApplyAsync(value -> read(value), NetworkExecutor.executor());"
                + "b = future.thenCombineAsync(other, (l, r) -> read(l, r), executor);"
                + "c = future.thenApply(value -> value);";
        assertEquals(4, findViolations(bad, "bad").size(), "every no-executor async overload must be flagged");
        assertTrue(findViolations(good, "good").isEmpty(), "explicit-executor and synchronous stages must pass");
    }

    /**
     * Static factories whose no-executor overload runs on the common pool. Matched on the declaring type so a helper
     * of the same name on {@code NetworkExecutor} is not flagged.
     */
    private static final List<String> FACTORIES = List.of("supplyAsync", "runAsync");

    /**
     * Dependent-stage combinators whose no-executor overload also runs on the common pool. These take one more
     * argument than the arity listed here once an executor is supplied.
     */
    private static final List<Map.Entry<String, Integer>> COMBINATORS = List.of(
            Map.entry("thenApplyAsync", 1),
            Map.entry("thenAcceptAsync", 1),
            Map.entry("thenRunAsync", 1),
            Map.entry("thenComposeAsync", 1),
            Map.entry("whenCompleteAsync", 1),
            Map.entry("handleAsync", 1),
            Map.entry("exceptionallyAsync", 1),
            Map.entry("exceptionallyComposeAsync", 1),
            Map.entry("acceptEitherAsync", 2),
            Map.entry("applyToEitherAsync", 2),
            Map.entry("runAfterBothAsync", 2),
            Map.entry("runAfterEitherAsync", 2),
            Map.entry("thenAcceptBothAsync", 2),
            Map.entry("thenCombineAsync", 2));

    private static List<String> findViolations(String source, String label) {
        String stripped = stripCommentsAndLiterals(source);
        List<String> violations = new ArrayList<>();
        for (String method : FACTORIES) {
            collect(stripped, label, "CompletableFuture." + method + "(", 1, violations);
        }
        for (Map.Entry<String, Integer> combinator : COMBINATORS) {
            collect(stripped, label, "." + combinator.getKey() + "(", combinator.getValue(), violations);
        }
        return violations;
    }

    private static void collect(
            String stripped, String label, String needle, int noExecutorArity, List<String> violations) {
        int from = 0;
        int at;
        while ((at = stripped.indexOf(needle, from)) >= 0) {
            int openParen = at + needle.length() - 1;
            if (topLevelArgumentCount(stripped, openParen) == noExecutorArity) {
                violations.add(label + ": " + needle.substring(needle.startsWith(".") ? 1 : 0, needle.length() - 1)
                        + "(...) with no executor argument (runs on the common pool)");
            }
            from = openParen + 1;
        }
    }

    /** Counts the top-level comma-separated arguments of a call given its opening paren index. */
    private static int topLevelArgumentCount(String source, int openParenIndex) {
        int depth = 0;
        int commas = 0;
        boolean sawContent = false;
        for (int index = openParenIndex; index < source.length(); index++) {
            char character = source.charAt(index);
            switch (character) {
                case '(', '[' -> depth++;
                case ')', ']' -> {
                    depth--;
                    if (depth == 0) {
                        return sawContent ? commas + 1 : 0;
                    }
                }
                case ',' -> {
                    if (depth == 1) {
                        commas++;
                    }
                }
                default -> {
                    if (depth == 1 && !Character.isWhitespace(character)) {
                        sawContent = true;
                    }
                }
            }
        }
        return sawContent ? commas + 1 : 0;
    }

    /**
     * Replaces string literals, char literals, and comments with spaces so their
     * contents cannot be mistaken for commas, parentheses, or the scanned call.
     */
    private static String stripCommentsAndLiterals(String source) {
        StringBuilder builder = new StringBuilder(source.length());
        int index = 0;
        int length = source.length();
        while (index < length) {
            char character = source.charAt(index);
            if (character == '/' && index + 1 < length && source.charAt(index + 1) == '/') {
                while (index < length && source.charAt(index) != '\n') {
                    index++;
                }
            } else if (character == '/' && index + 1 < length && source.charAt(index + 1) == '*') {
                index += 2;
                while (index + 1 < length && !(source.charAt(index) == '*' && source.charAt(index + 1) == '/')) {
                    index++;
                }
                index += 2;
                builder.append(' ');
            } else if (character == '"' || character == '\'') {
                char quote = character;
                index++;
                while (index < length && source.charAt(index) != quote) {
                    if (source.charAt(index) == '\\' && index + 1 < length) {
                        index++;
                    }
                    index++;
                }
                index++;
                builder.append(' ');
            } else {
                builder.append(character);
                index++;
            }
        }
        return builder.toString();
    }

    private static String readString(Path path) {
        try {
            return Files.readString(path);
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }
}
