// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IOAdapterTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    @DisplayName("Exists reflects filesystem state")
    void existsReflectsFilesystemState() throws Exception {
        IOAdapter adapter = new IOAdapter();
        Path existing = temporaryDirectory.resolve("existing.bin");
        Path missing = temporaryDirectory.resolve("missing.bin");
        Files.write(existing, new byte[] {1});

        assertTrue(adapter.exists(existing.toString()));
        assertFalse(adapter.exists(missing.toString()));
    }

    @Test
    @DisplayName("File channel honors open options and seeking")
    void fileChannelHonorsOptionsAndSeeking() throws Exception {
        IOAdapter adapter = new IOAdapter();
        Path file = temporaryDirectory.resolve("transfer.bin");

        try (FileChannel channel = adapter.getFileChannel(
                file.toString(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(new byte[] {1, 2, 3}));
            channel.position(1);
            channel.write(ByteBuffer.wrap(new byte[] {9}));
        }

        assertEquals(java.util.List.of((byte) 1, (byte) 9, (byte) 3), toList(Files.readAllBytes(file)));
        assertEquals(3, adapter.getFileInfo(file.toString()).size());
    }

    private static java.util.List<Byte> toList(byte[] bytes) {
        java.util.ArrayList<Byte> result = new java.util.ArrayList<>();
        for (byte value : bytes) {
            result.add(value);
        }
        return result;
    }
}
