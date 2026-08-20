// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.share;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.slsk.search.FileAttributeType;
import dev.slsk.search.FileAttributes;
import dev.slsk.search.SearchFile;
import dev.slsk.internal.messaging.messages.SearchResponseCodec;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CatalogsTest {

    @Test
    void carriesPublicFileAttributesToTheWireFile() {
        var source = new SearchFile(
                "Music\\song.flac",
                123L,
                new FileAttributes(Map.of(
                        FileAttributeType.LENGTH, 90,
                        FileAttributeType.SAMPLE_RATE, 44_100,
                        FileAttributeType.BIT_DEPTH, 16)));

        File wire = Catalogs.file(source);

        assertEquals(90, wire.length());
        assertEquals(44_100, wire.sampleRate());
        assertEquals(16, wire.bitDepth());
    }

    @Test
    void searchResponseCarriesAttributesAcrossTheWire() {
        var source = new SearchFile(
                "Music\\song.mp3",
                123L,
                new FileAttributes(Map.of(
                        FileAttributeType.BIT_RATE, 320,
                        FileAttributeType.LENGTH, 240,
                        FileAttributeType.VARIABLE_BIT_RATE, 0)));

        var outbound = Catalogs.searchResponse("me", 42, List.of(source), true, 1_000, 0);
        File decoded = SearchResponseCodec.fromByteArray(outbound.toByteArray()).files().getFirst();

        assertEquals(320, decoded.bitRate());
        assertEquals(240, decoded.length());
        assertEquals(false, decoded.variableBitRate());
    }
}
