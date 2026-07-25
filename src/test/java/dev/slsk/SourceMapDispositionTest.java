// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class SourceMapDispositionTest {
    private static final Pattern JAVA_PATH = Pattern.compile("`([^`]+(?:\\.java|pom\\.xml))`");
    private static final Pattern CSHARP_PATH = Pattern.compile("^\\| `([^`]+\\.cs)`");

    @Test
    void everyCsharpSourceHasAResolvableJavaDisposition() throws IOException {
        int rows = 0;
        for (String line : Files.readAllLines(Path.of("docs", "source-map.md"))) {
            if (!line.startsWith("| `")) {
                continue;
            }
            String[] cells = line.substring(2).split(" \\| ", -1);
            assertTrue(cells.length >= 4, line);
            String status = cells[2];
            assertNotEquals("unmapped", status, line);
            assertTrue(status.equals("ported") || status.equals("replaced"), line);

            Matcher paths = JAVA_PATH.matcher(cells[1]);
            boolean found = false;
            while (paths.find()) {
                found = true;
                String value = paths.group(1);
                Path path = value.equals("pom.xml") ? Path.of(value) : Path.of("src", "main", "java", value);
                assertTrue(Files.isRegularFile(path), line);
            }
            assertTrue(found, line);
            rows++;
        }
        assertEquals(273, rows);
    }

    @Test
    void everyOracleSourceFileIsMappedExactlyOnce() throws IOException {
        Path sourceRoot = CsharpOracle.requireSourceRoot();
        Set<String> mapped = new LinkedHashSet<>();
        List<String> duplicates = new ArrayList<>();

        for (String line : Files.readAllLines(Path.of("docs", "source-map.md"))) {
            Matcher source = CSHARP_PATH.matcher(line);
            if (!source.find()) {
                continue;
            }
            String value = source.group(1);
            if (!mapped.add(value)) {
                duplicates.add(value);
            }
        }

        assertTrue(duplicates.isEmpty(), () -> "Source map rows are duplicated: " + duplicates);

        Set<String> present = oracleSourceFiles(sourceRoot);

        Set<String> unmapped = new TreeSet<>(present);
        unmapped.removeAll(mapped);
        assertTrue(unmapped.isEmpty(), () -> "The pinned C# tree contains sources with no source-map row: " + unmapped);

        Set<String> stale = new TreeSet<>(mapped);
        stale.removeAll(present);
        assertTrue(
                stale.isEmpty(),
                () -> "The source map names C# sources that no longer exist in the pinned tree: " + stale);
    }

    private static Set<String> oracleSourceFiles(Path sourceRoot) throws IOException {
        try (Stream<Path> files = Files.walk(sourceRoot)) {
            return files.filter(path -> path.toString().endsWith(".cs"))
                    .filter(path -> !path.toString().contains(File.separator + "bin" + File.separator))
                    .filter(path -> !path.toString().contains(File.separator + "obj" + File.separator))
                    .map(path -> sourceRoot.relativize(path).toString().replace('\\', '/'))
                    .collect(Collectors.toCollection(TreeSet::new));
        }
    }
}
