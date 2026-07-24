// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class SourceMapDispositionTest {
    private static final Pattern JAVA_PATH = Pattern.compile("`([^`]+(?:\\.java|pom\\.xml))`");

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
}
