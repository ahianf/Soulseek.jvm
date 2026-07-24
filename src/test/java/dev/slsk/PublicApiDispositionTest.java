// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class PublicApiDispositionTest {
    @Test
    void everyGeneratedDispositionNamesAnExistingJavaSymbol() throws IOException, ClassNotFoundException {
        boolean generated = false;
        String javaTypeName = null;
        Class<?> javaType = null;
        int rows = 0;

        for (String line : Files.readAllLines(Path.of("docs", "api-mapping.md"))) {
            if (line.equals("<!-- BEGIN GENERATED SYMBOL DISPOSITION -->")) {
                generated = true;
                continue;
            }
            if (line.equals("<!-- END GENERATED SYMBOL DISPOSITION -->")) {
                break;
            }
            if (!generated || !line.startsWith("| `")) {
                continue;
            }

            String[] cells = line.substring(2).split(" \\| ", -1);
            assertTrue(cells.length >= 4, line);
            String csharp = unquote(cells[0]);
            String java = unquote(cells[1]);
            String status = cells[2];
            assertTrue(status.equals("tested") || status.equals("exception"), line);

            if (csharp.startsWith("T:")) {
                javaTypeName = java;
                javaType = Class.forName(javaTypeName);
            } else {
                assertFalse(java.equals("—"), line);
                assertTrue(java.startsWith(javaTypeName + "."), line);
                verifyMember(javaType, javaTypeName, java, line);
            }
            rows++;
        }

        assertEquals(809, rows);
        verifyJavaPublicApiDocumentation();
    }

    private static void verifyJavaPublicApiDocumentation() throws IOException, ClassNotFoundException {
        String documentation = Files.readString(Path.of("docs", "public-api.md"));
        int publicTypes = 0;

        for (String packageName : List.of(
                "dev.slsk", "dev.slsk.diagnostics", "dev.slsk.events", "dev.slsk.exceptions", "dev.slsk.options")) {
            Path packagePath = Path.of("src", "main", "java", packageName.replace('.', '/'));
            try (Stream<Path> paths = Files.list(packagePath)) {
                for (Path path : paths.filter(candidate -> candidate.toString().endsWith(".java"))
                        .sorted()
                        .toList()) {
                    String filename = path.getFileName().toString();
                    String simpleName = filename.substring(0, filename.length() - ".java".length());
                    if (simpleName.equals("package-info")) {
                        continue;
                    }
                    Class<?> type = Class.forName(packageName + "." + simpleName);
                    if (!Modifier.isPublic(type.getModifiers())) {
                        continue;
                    }
                    assertTrue(
                            documentation.contains("`" + simpleName + "`"),
                            () -> "Missing public Java type " + type.getName() + " from docs/public-api.md");
                    publicTypes++;
                }
            }
        }

        assertEquals(140, publicTypes);
    }

    private static void verifyMember(Class<?> type, String typeName, String mapping, String row) {
        for (String candidate : mapping.split(" / ")) {
            String member =
                    candidate.startsWith(typeName + ".") ? candidate.substring(typeName.length() + 1) : candidate;
            if (member.startsWith("<init>")) {
                assertTrue(type.getDeclaredConstructors().length > 0, row);
                continue;
            }
            int parameters = member.indexOf('(');
            if (parameters < 0) {
                assertTrue(
                        Arrays.stream(type.getFields())
                                .map(field -> field.getName())
                                .anyMatch(member::equals),
                        row);
                continue;
            }
            String methodName = member.substring(0, parameters);
            assertTrue(Arrays.stream(type.getMethods()).map(Method::getName).anyMatch(methodName::equals), row);
        }
    }

    private static String unquote(String value) {
        String trimmed = value.trim();
        return trimmed.startsWith("`") && trimmed.endsWith("`") ? trimmed.substring(1, trimmed.length() - 1) : trimmed;
    }
}
