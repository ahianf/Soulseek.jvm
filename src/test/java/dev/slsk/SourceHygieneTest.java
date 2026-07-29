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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Properties of the source text itself, which no compiler checks.
 */
class SourceHygieneTest {

    @Test
    @DisplayName("no source file is binary, so grep, diff and review all work on all of it")
    void noSourceFileContainsAControlByte() throws IOException {
        // git decides a file is binary by looking for a NUL in its first eight
        // kilobytes, and once it has decided, `git grep`, `git diff` and every
        // review tool downstream of them stop showing its contents. Two files
        // used a raw NUL as a composite-key delimiter and were binary for that
        // reason alone. A typed key costs a record; this is what it bought.
        List<String> offenders = new ArrayList<>();
        for (Path root : List.of(Path.of("src", "main", "java"), Path.of("src", "test", "java"))) {
            try (Stream<Path> paths = Files.walk(root)) {
                paths.filter(path -> path.toString().endsWith(".java")).sorted().forEach(path -> {
                    try {
                        if (Files.readString(path).indexOf('\0') >= 0) {
                            offenders.add(path.toString());
                        }
                    } catch (IOException failure) {
                        offenders.add(path + ": " + failure);
                    }
                });
            }
        }

        assertTrue(
                offenders.isEmpty(),
                "these sources contain a NUL byte and git classifies them as binary:"
                        + System.lineSeparator()
                        + String.join(System.lineSeparator(), offenders));
    }
}
