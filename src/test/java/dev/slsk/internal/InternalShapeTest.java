// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * What the internals are shaped like, asserted rather than described.
 *
 * <p>Two claims from the 2.0 definition of done, both of which a refactor could
 * quietly undo and neither of which any behavioural test would notice.
 *
 * <p>The first is that the library is blocking on the inside. It was
 * future-shaped: 43 files and several hundred composition sites, a public API
 * that blocked wrapped around an implementation that did not. Every one of those
 * is gone, and the way to keep them gone is to refuse the type rather than to
 * review each use of it. This subsumes the common-pool dispatch guard that
 * preceded it — {@code CompletableFuture.supplyAsync} with no executor cannot
 * appear in code that cannot name {@code CompletableFuture}.
 *
 * <p>The second is that no method telescopes. {@code TransferEngine} declared 53
 * overloads of four methods and {@code enqueue} alone had 24 that no caller
 * reached; both towers came down in Phase 4, and D14's other half took ten
 * unreachable {@code search} overloads with it. Eight is the line.
 */
class InternalShapeTest {

    private static final Path MAIN_SOURCES = Path.of("src", "main", "java");
    private static final Path MESSAGING_SOURCES = MAIN_SOURCES.resolve(Path.of("dev", "slsk", "internal", "messaging"));
    private static final Path NETWORK_SOURCES = MAIN_SOURCES.resolve(Path.of("dev", "slsk", "internal", "network"));
    private static final Pattern ADD_REMOVE_LISTENER = Pattern.compile("\\b(?:add|remove)[A-Z]\\w*Listener\\s*\\(");
    private static final Pattern SENDER_CALLBACK = Pattern.compile("\\bhandle\\s*\\(\\s*Object\\s+sender\\b");

    /**
     * How many overloads of one name a type may declare.
     *
     * <p>Constructors are counted separately and are not held to this: three
     * option types telescope theirs — {@code SearchOptions} at 13,
     * {@code TransferOptions} at 10, {@code ConnectionOptions} at 9 — and
     * reshaping them is neither this goal's subject nor reachable from outside
     * the module.
     */
    private static final int OVERLOAD_LIMIT = 8;

    @Test
    @DisplayName("no CompletableFuture anywhere in src/main/java")
    void theInternalsAreBlocking() {
        List<String> violations = new ArrayList<>();
        for (Path file : mainSources()) {
            String source = stripCommentsAndLiterals(readString(file));
            int at = source.indexOf("CompletableFuture");
            while (at >= 0) {
                violations.add(file + ": CompletableFuture at offset " + at);
                at = source.indexOf("CompletableFuture", at + 1);
            }
        }

        if (!violations.isEmpty()) {
            fail("The internals are blocking. A future here is either a race that needs a "
                    + "primitive — FirstSuccess, ConnectionCell, Settlement — or a try/catch "
                    + "written as a composition.\n  " + String.join("\n  ", violations));
        }
    }

    @Test
    @DisplayName("no CompletionException anywhere in src/main/java")
    void failuresTravelUnwrapped() {
        List<String> violations = new ArrayList<>();
        for (Path file : mainSources()) {
            String source = stripCommentsAndLiterals(readString(file));
            int at = source.indexOf("CompletionException");
            while (at >= 0) {
                violations.add(file + ": CompletionException at offset " + at);
                at = source.indexOf("CompletionException", at + 1);
            }
        }

        if (!violations.isEmpty()) {
            fail("Failures travel as themselves. The join() presentation protocol — wrap in a "
                    + "CompletionException, unwrap at the call site — died with the futures; a "
                    + "failure that must cross a settle boundary goes through Failures.rethrow.\n  "
                    + String.join("\n  ", violations));
        }
    }

    @Test
    @DisplayName("the scan reads code and ignores prose about it")
    void theScanIgnoresJavadoc() {
        String prose = "/** This replaced a {@code CompletableFuture.allOf} over the arms. */\n"
                + "// was CompletableFuture<Void>\n"
                + "String name = \"CompletableFuture\";\n"
                + "void settle() {}";
        String code = "CompletableFuture<Void> pending = new CompletableFuture<>();";
        assertEquals(-1, stripCommentsAndLiterals(prose).indexOf("CompletableFuture"), "prose must not be flagged");
        assertTrue(stripCommentsAndLiterals(code).contains("CompletableFuture"), "code must be flagged");
    }

    @Test
    @DisplayName("no production type declares more than eight overloads of any one method")
    void noMethodTelescopes() {
        List<String> violations = new ArrayList<>();
        for (Class<?> type : mainTypes()) {
            Map<String, Integer> counts = new HashMap<>();
            for (Method method : type.getDeclaredMethods()) {
                if (method.isSynthetic() || method.isBridge()) {
                    continue;
                }
                counts.merge(method.getName(), 1, Integer::sum);
            }
            counts.forEach((name, count) -> {
                if (count > OVERLOAD_LIMIT) {
                    violations.add(type.getName() + "." + name + " has " + count + " overloads");
                }
            });
        }

        if (!violations.isEmpty()) {
            fail("A method with more than " + OVERLOAD_LIMIT + " overloads is a tower of defaults; "
                    + "give it one shape and let the callers say what they mean.\n  "
                    + String.join("\n  ", violations));
        }
    }

    @Test
    @DisplayName("internal network callbacks carry identity in their payload")
    void callbacksDoNotTakeDotNetSenderArguments() {
        assertNoInternalCallbackShape(
                SENDER_CALLBACK,
                "A callback payload carries its source identity; do not restore handle(Object sender, ...).\n  ");
    }

    @Test
    @DisplayName("internal network listeners are scoped subscriptions")
    void listenerRegistrationDoesNotReturnToAddRemovePairs() {
        assertNoInternalCallbackShape(
                ADD_REMOVE_LISTENER,
                "Listener lifetimes are represented by Subscription; do not restore paired add/remove methods.\n  ");
    }

    private static void assertNoInternalCallbackShape(Pattern forbidden, String message) {
        List<String> violations = new ArrayList<>();
        for (Path file : mainSources()) {
            if (!file.startsWith(NETWORK_SOURCES) && !file.startsWith(MESSAGING_SOURCES)) {
                continue;
            }
            Matcher matcher = forbidden.matcher(stripCommentsAndLiterals(readString(file)));
            while (matcher.find()) {
                violations.add(file + ": forbidden callback shape at offset " + matcher.start());
            }
        }
        if (!violations.isEmpty()) {
            fail(message + String.join("\n  ", violations));
        }
    }

    /** Every source file under {@code src/main/java}. */
    private static List<Path> mainSources() {
        assertTrue(
                Files.isDirectory(MAIN_SOURCES),
                "Expected to run from the module root with " + MAIN_SOURCES + " present");
        try (Stream<Path> files = Files.walk(MAIN_SOURCES)) {
            return files.filter(path -> path.toString().endsWith(".java"))
                    .filter(path -> !path.getFileName().toString().equals("module-info.java"))
                    .sorted()
                    .toList();
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    /**
     * Every type under {@code src/main/java}, loaded.
     *
     * <p>Loaded from source paths rather than scanned off the classpath, so a
     * stale class file cannot make this pass. Nested types come with their
     * owners; a source file with no class is a build that did not happen.
     */
    private static List<Class<?>> mainTypes() {
        List<Class<?>> types = new ArrayList<>();
        for (Path file : mainSources()) {
            String name = MAIN_SOURCES.relativize(file).toString();
            String binary =
                    name.substring(0, name.length() - ".java".length()).replace(java.io.File.separatorChar, '.');
            if (binary.endsWith(".package-info")) {
                continue;
            }
            Class<?> type;
            try {
                type = Class.forName(binary);
            } catch (ClassNotFoundException exception) {
                throw new AssertionError("main source with no class: " + file, exception);
            }
            collect(type, types);
        }
        // A scan that found nothing asserts nothing. The tree is ~270 types;
        // this catches a walk that silently stopped resolving them.
        assertTrue(types.size() > 200, "expected the whole main tree, found " + types.size() + " types");
        return types;
    }

    private static void collect(Class<?> type, List<Class<?>> types) {
        types.add(type);
        for (Class<?> nested : type.getDeclaredClasses()) {
            if (!nested.isSynthetic()) {
                collect(nested, types);
            }
        }
    }

    /**
     * Replaces comments, string literals and char literals with spaces.
     *
     * <p>Sixteen mentions of {@code CompletableFuture} survive in the internals,
     * every one of them javadoc saying what the shape below it replaced. Those
     * are the point of the comments; the scan has to read code and not prose.
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
