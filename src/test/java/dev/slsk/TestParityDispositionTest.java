// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class TestParityDispositionTest {
    private static final String BEGIN = "<!-- BEGIN GENERATED UNIT TEST DISPOSITION -->";
    private static final String END = "<!-- END GENERATED UNIT TEST DISPOSITION -->";
    private static final Pattern DESTINATION = Pattern.compile("`([^`]+\\.java)`");
    private static final Pattern XUNIT_DECLARATION = Pattern.compile("^\\s*\\[(?:Fact|Theory)\\b", Pattern.MULTILINE);
    private static final Pattern XUNIT_DATA =
            Pattern.compile("^\\s*\\[(?:InlineData|MemberData|ClassData)\\b", Pattern.MULTILINE);

    @Test
    void everyOriginalUnitTestClassHasAResolvableDisposition() throws IOException {
        Path csharpRoot = Path.of("..", "tests", "Soulseek.Tests.Unit");
        Set<String> documentedSources = new HashSet<>();
        boolean generated = false;
        int rows = 0;
        int declarations = 0;

        for (String line : Files.readAllLines(Path.of("docs", "test-parity.md"))) {
            if (line.equals(BEGIN)) {
                generated = true;
                continue;
            }
            if (line.equals(END)) {
                break;
            }
            if (!generated || !line.startsWith("| `")) {
                continue;
            }

            String[] cells = line.substring(2).split(" \\| ", -1);
            assertTrue(cells.length >= 6, line);
            String source = unquote(cells[0]);
            String className = unquote(cells[1]);
            int expectedDeclarations = Integer.parseInt(cells[2]);
            assertEquals("passing", cells[4], line);
            assertTrue(documentedSources.add(source), line);

            Path sourcePath = csharpRoot.resolve(source);
            assertTrue(Files.isRegularFile(sourcePath), line);
            String sourceText = Files.readString(sourcePath);
            assertTrue(
                    Pattern.compile("\\bpublic\\s+(?:sealed\\s+)?class\\s+" + Pattern.quote(className) + "\\b")
                            .matcher(sourceText)
                            .find(),
                    line);
            assertEquals(
                    expectedDeclarations,
                    XUNIT_DECLARATION.matcher(sourceText).results().count(),
                    line);

            Matcher destinations = DESTINATION.matcher(cells[3]);
            int destinationCount = 0;
            while (destinations.find()) {
                destinationCount++;
                assertTrue(Files.isRegularFile(Path.of("src", "test", "java", destinations.group(1))), line);
            }
            assertTrue(destinationCount > 0, line);
            rows++;
            declarations += expectedDeclarations;
        }

        assertEquals(160, rows);
        assertEquals(2_025, declarations);
        assertEquals(testBearingSources(csharpRoot), documentedSources);
        assertEquals(306, dataDeclarationCount(csharpRoot));
    }

    private static Set<String> testBearingSources(Path root) throws IOException {
        Set<String> result = new HashSet<>();
        try (Stream<Path> files = Files.walk(root)) {
            files.filter(path -> path.toString().endsWith(".cs"))
                    .filter(path -> !path.toString().contains("/bin/"))
                    .filter(path -> !path.toString().contains("/obj/"))
                    .forEach(path -> {
                        try {
                            String text = Files.readString(path);
                            if (XUNIT_DECLARATION.matcher(text).find()) {
                                result.add(normalize(root.relativize(path)));
                            }
                        } catch (IOException exception) {
                            throw new TestInventoryReadException(exception);
                        }
                    });
        } catch (TestInventoryReadException exception) {
            throw exception.getCause();
        }
        return result;
    }

    private static long dataDeclarationCount(Path root) throws IOException {
        try (Stream<Path> files = Files.walk(root)) {
            return files.filter(path -> path.toString().endsWith(".cs"))
                    .mapToLong(path -> {
                        try {
                            return XUNIT_DATA
                                    .matcher(Files.readString(path))
                                    .results()
                                    .count();
                        } catch (IOException exception) {
                            throw new TestInventoryReadException(exception);
                        }
                    })
                    .sum();
        } catch (TestInventoryReadException exception) {
            throw exception.getCause();
        }
    }

    private static String normalize(Path path) {
        return path.toString().replace('\\', '/');
    }

    private static String unquote(String value) {
        String trimmed = value.trim();
        return trimmed.startsWith("`") && trimmed.endsWith("`") ? trimmed.substring(1, trimmed.length() - 1) : trimmed;
    }

    private static final class TestInventoryReadException extends RuntimeException {
        private TestInventoryReadException(IOException cause) {
            super(cause);
        }

        @Override
        public synchronized IOException getCause() {
            return (IOException) super.getCause();
        }
    }
}
