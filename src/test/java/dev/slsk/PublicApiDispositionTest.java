// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class PublicApiDispositionTest {
    private static final Pattern CSHARP_PUBLIC_TYPE = Pattern.compile(
            "^\\s*public\\s+(?:static\\s+|sealed\\s+|abstract\\s+|partial\\s+|readonly\\s+|unsafe\\s+)*"
                    + "(?:class|interface|struct|enum|record)\\s+([A-Za-z_][A-Za-z0-9_]*)",
            Pattern.MULTILINE);

    private static final Pattern INTERNAL_TYPE = Pattern.compile(
            "^\\s*internal\\s+(?:static\\s+|sealed\\s+|abstract\\s+|partial\\s+|readonly\\s+|unsafe\\s+)*"
                    + "(?:class|interface|struct|enum|record)\\s+[A-Za-z_][A-Za-z0-9_]*",
            Pattern.MULTILINE);

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

    @Test
    void everyExportedOracleTypeHasAMappingRow() throws IOException {
        Path sourceRoot = CsharpOracle.requireSourceRoot();
        Set<String> mapped = new LinkedHashSet<>();

        for (String line : Files.readAllLines(Path.of("docs", "api-mapping.md"))) {
            if (!line.startsWith("| `T:")) {
                continue;
            }
            mapped.add(unquote(line.substring(2).split(" \\| ", -1)[0]).substring("T:".length()));
        }

        Set<String> exported = exportedOracleTypes(sourceRoot);

        Set<String> unmapped = new TreeSet<>(exported);
        unmapped.removeAll(mapped);
        assertTrue(unmapped.isEmpty(), () -> "The pinned C# tree exports types with no api-mapping row: " + unmapped);

        Set<String> stale = new TreeSet<>(mapped);
        stale.removeAll(exported);
        assertTrue(
                stale.isEmpty(),
                () -> "The api mapping names C# types that the pinned tree no longer exports: " + stale);
    }

    /**
     * Collects the externally visible types of the pinned C# tree.
     *
     * <p>A {@code public} type nested inside an {@code internal} container is not part of the assembly's exported
     * surface. Only {@code Messaging/MessageCode.cs} takes that shape, so a file's first {@code internal} type
     * declaration is treated as opening a non-exported container for everything that follows it in that file.
     */
    private static Set<String> exportedOracleTypes(Path sourceRoot) throws IOException {
        Set<String> exported = new TreeSet<>();
        try (Stream<Path> files = Files.walk(sourceRoot)) {
            for (Path path : files.filter(candidate -> candidate.toString().endsWith(".cs"))
                    .filter(candidate -> !candidate.toString().contains(File.separator + "bin" + File.separator))
                    .filter(candidate -> !candidate.toString().contains(File.separator + "obj" + File.separator))
                    .sorted()
                    .toList()) {
                String namespaceName = null;
                boolean withinInternalContainer = false;
                for (String line : Files.readAllLines(path)) {
                    if (namespaceName == null && line.startsWith("namespace ")) {
                        namespaceName = line.substring("namespace ".length()).trim();
                        continue;
                    }
                    if (INTERNAL_TYPE.matcher(line).find()) {
                        withinInternalContainer = true;
                        continue;
                    }
                    Matcher type = CSHARP_PUBLIC_TYPE.matcher(line);
                    if (!withinInternalContainer && type.find()) {
                        exported.add(namespaceName + "." + type.group(1));
                    }
                }
            }
        }
        return exported;
    }

    private static void verifyJavaPublicApiDocumentation() throws IOException, ClassNotFoundException {
        String documentation = Files.readString(Path.of("docs", "public-api.md"));
        int catalogStart = documentation.indexOf("## Public type catalog");
        int catalogEnd = documentation.indexOf("## Related documents", catalogStart);
        assertTrue(catalogStart >= 0);
        assertTrue(catalogEnd > catalogStart);
        String catalog = documentation.substring(catalogStart, catalogEnd);
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
                            countOccurrences(catalog, "`" + simpleName + "`") == 1,
                            () -> "Missing public Java type " + type.getName()
                                    + " exactly once from the public type catalog");
                    publicTypes++;
                }
            }
        }

        // 144 since Phase 4: TransferHandle replaced the nested enqueue future,
        // and DownloadRequest/UploadRequest/SearchRequest replaced the overload
        // cross products.
        assertEquals(144, publicTypes);
    }

    private static int countOccurrences(String text, String value) {
        int count = 0;
        int offset = 0;
        while ((offset = text.indexOf(value, offset)) >= 0) {
            count++;
            offset += value.length();
        }
        return count;
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
