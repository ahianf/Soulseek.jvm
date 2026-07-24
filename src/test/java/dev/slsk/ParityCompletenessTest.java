// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class ParityCompletenessTest {
    private static final List<Pattern> MAIN_PLACEHOLDERS = List.of(
            Pattern.compile("\\bTO" + "DO\\b"),
            Pattern.compile("\\bFIX" + "ME\\b"),
            Pattern.compile("\\bX" + "XX\\b"),
            Pattern.compile("\\bstub\\b", Pattern.CASE_INSENSITIVE),
            Pattern.compile("\\bnot\\s+implemented\\b", Pattern.CASE_INSENSITIVE),
            Pattern.compile("Unsupported" + "OperationException"));
    private static final List<Pattern> DISABLED_TESTS =
            List.of(Pattern.compile("@Dis" + "abled\\b"), Pattern.compile("@Ig" + "nore\\b"));

    @Test
    void productionSourcesContainNoParityPlaceholders() throws IOException {
        assertNoMatches(Path.of("src", "main", "java"), MAIN_PLACEHOLDERS);
    }

    @Test
    void testSourcesContainNoDisabledTests() throws IOException {
        for (Path root : List.of(Path.of("src", "test", "java"), Path.of("src", "integrationTest", "java"))) {
            assertNoMatches(root, DISABLED_TESTS);
        }
    }

    private static void assertNoMatches(Path root, List<Pattern> patterns) throws IOException {
        List<String> failures = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(root)) {
            paths.filter(path -> path.toString().endsWith(".java"))
                    .sorted()
                    .forEach(path -> inspect(path, patterns, failures));
        }
        assertTrue(failures.isEmpty(), () -> String.join(System.lineSeparator(), failures));
    }

    private static void inspect(Path path, List<Pattern> patterns, List<String> failures) {
        try {
            String source = Files.readString(path);
            for (Pattern pattern : patterns) {
                if (pattern.matcher(source).find()) {
                    failures.add(path + ": matches " + pattern);
                }
            }
        } catch (IOException exception) {
            failures.add(path + ": " + exception.getMessage());
        }
    }
}
