// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

class TestParityDispositionTest {
    private static final String BEGIN = "<!-- BEGIN GENERATED UNIT TEST DISPOSITION -->";
    private static final String END = "<!-- END GENERATED UNIT TEST DISPOSITION -->";
    private static final Pattern DESTINATION = Pattern.compile("`([^`]+\\.java)`");
    private static final Pattern XUNIT_DECLARATION = Pattern.compile("^\\s*\\[(?:Fact|Theory)\\b", Pattern.MULTILINE);
    private static final Pattern XUNIT_DATA =
            Pattern.compile("^\\s*\\[(?:InlineData|MemberData|ClassData)\\b", Pattern.MULTILINE);
    private static final Pattern XUNIT_DISPLAY_NAME = Pattern.compile("\\[Fact\\(DisplayName = \"([^\"]+)\"\\)\\]");
    private static final Pattern JUNIT_DISPLAY_NAME = Pattern.compile("@DisplayName\\(\"([^\"]+)\"\\)");

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
