// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Keeps capability models from accumulating beside the public entry points. */
class PackageLayoutTest {

    private static final Path ROOT = Path.of("src", "main", "java", "dev", "slsk");

    @Test
    @DisplayName("the public root contains only entry points and shared control contracts")
    void thePublicRootStaysNarrow() throws IOException {
        assertEquals(
                Set.of(
                        "Attachment",
                        "Chat",
                        "Connection",
                        "Diagnostics",
                        "Downloads",
                        "EventStream",
                        "Me",
                        "PrivateRooms",
                        "Rooms",
                        "Search",
                        "Shares",
                        "Soulseek",
                        "SoulseekBuilder",
                        "Subscription",
                        "Uploads",
                        "Users"),
                directTypes(ROOT));
    }

    private static Set<String> directTypes(Path directory) throws IOException {
        Set<String> names = new TreeSet<>();
        try (Stream<Path> files = Files.list(directory)) {
            files.map(path -> path.getFileName().toString())
                    .filter(name -> name.endsWith(".java"))
                    .filter(name -> !name.equals("package-info.java"))
                    .map(name -> name.substring(0, name.length() - ".java".length()))
                    .forEach(names::add);
        }
        return names;
    }
}
