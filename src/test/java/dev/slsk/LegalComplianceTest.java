// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class LegalComplianceTest {
    private static final String COPYRIGHT = "SPDX-FileCopyrightText:";
    private static final String SPDX = "SPDX-License-Identifier: GPL-3.0-only";

    @Test
    void everyJavaSourceCarriesRequiredLegalNotices() throws IOException {
        List<String> failures = new ArrayList<>();
        List<Path> roots = List.of(Path.of("src"));

        for (Path root : roots) {
            try (Stream<Path> paths = Files.walk(root)) {
                paths.filter(path -> path.toString().endsWith(".java"))
                        .sorted()
                        .forEach(path -> auditJavaSource(path, failures));
            }
        }

        assertTrue(failures.isEmpty(), () -> String.join(System.lineSeparator(), failures));
    }

    private static void auditJavaSource(Path path, List<String> failures) {
        try {
            String text = Files.readString(path);
            int headerEnd = text.indexOf("package ");
            if (headerEnd < 0 && path.getFileName().toString().equals("module-info.java")) {
                headerEnd = text.indexOf("module ");
            }
            if (headerEnd < 0) {
                failures.add(path + ": missing package or module declaration");
                return;
            }
            String header = text.substring(0, headerEnd);
            require(path, header, COPYRIGHT, failures);
            require(path, header, SPDX, failures);
        } catch (IOException exception) {
            failures.add(path + ": " + exception.getMessage());
        }
    }

    private static void require(Path path, String text, String expected, List<String> failures) {
        if (!text.contains(expected)) {
            failures.add(path + ": missing " + expected);
        }
    }
}
