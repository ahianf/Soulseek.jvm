// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.MeshState;
import dev.slsk.Metrics;
import dev.slsk.Soulseek;
import dev.slsk.Username;
import dev.slsk.events.MeshEvent;
import dev.slsk.internal.options.SoulseekClientOptions;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DefaultDiagnosticsTest {

    private static Soulseek client() {
        return DefaultSoulseek.create("alice", "password", 157, new SoulseekClientOptions());
    }

    @Test
    @DisplayName("mesh state reads as one snapshot, not seven listener fragments")
    void meshIsReadableAsASnapshot() {
        try (Soulseek slsk = client()) {
            MeshState mesh = slsk.diagnostics().mesh();
            assertFalse(mesh.hasParent());
            assertEquals(0, mesh.childCount());
            assertFalse(mesh.isConnected(), "no parent and not a branch root is disconnected");
        }
    }

    @Test
    void metricsAreReadableSynchronously() {
        try (Soulseek slsk = client()) {
            Metrics metrics = slsk.diagnostics().metrics();
            assertEquals(0, metrics.bytesDownloaded());
            assertEquals(0, metrics.activeDownloads());
        }
    }

    @Test
    @DisplayName("protocolTrace is an idempotent intent")
    void protocolTraceIsIdempotent() {
        try (Soulseek slsk = client()) {
            slsk.diagnostics().protocolTrace(true);
            slsk.diagnostics().protocolTrace(true);
            slsk.diagnostics().protocolTrace(false);
            slsk.diagnostics().protocolTrace(false);
        }
    }

    @Test
    void exposesDiagnosticAndMeshStreams() {
        try (Soulseek slsk = client()) {
            try (var first = slsk.diagnostics().events().subscribe(event -> {});
                    var second = slsk.diagnostics().meshEvents().subscribe(event -> {})) {
                assertTrue(true);
            }
        }
    }

    @Test
    void meshStateChildrenAreImmutable() {
        MeshState mesh = new MeshState(
                true,
                Optional.of(Username.of("parent")),
                List.of(Username.of("child")),
                false,
                2,
                Optional.of(Username.of("root")));
        assertEquals(1, mesh.childCount());
        assertTrue(mesh.isConnected());
        assertThrows(UnsupportedOperationException.class, () -> mesh.children().add(Username.of("x")));
    }

    @Test
    @DisplayName("a branch root with no parent is still connected to the mesh")
    void branchRootCountsAsConnected() {
        MeshState root = new MeshState(false, Optional.empty(), List.of(), true, 0, Optional.empty());
        assertTrue(root.isConnected());
    }

    @Test
    @DisplayName("a switch over MeshEvent needs no default")
    void meshEventIsExhaustivelySwitchable() {
        MeshState empty = new MeshState(false, Optional.empty(), List.of(), false, 0, Optional.empty());
        MeshEvent event = new MeshEvent.StateChanged(empty, empty, Instant.EPOCH);
        String rendered =
                switch (event) {
                    case MeshEvent.StateChanged changed ->
                        "changed to " + changed.to().childCount() + " children";
                };
        assertEquals("changed to 0 children", rendered);
    }
}
