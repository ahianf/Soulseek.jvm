// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

import dev.slsk.diagnostics.MeshState;
import java.time.Instant;
import java.util.Objects;

/**
 * Changes to our position in the distributed search mesh.
 *
 * <p>One event, where there were seven listeners: parent adopted, parent
 * disconnected, child added, child disconnected, promoted to branch root,
 * demoted from branch root, and network reset. Each delivered a fragment, and a
 * consumer wanting to render the mesh registered all seven and reassembled the
 * whole from the pieces. Since the thing being rendered is the state and not the
 * transitions, the state is what is published.
 */
public sealed interface MeshEvent extends SoulseekEvent {

    /**
     * Our position in the mesh changed.
     *
     * @param from the previous state
     * @param to the current state
     * @param at when
     */
    record StateChanged(MeshState from, MeshState to, Instant at) implements MeshEvent {
        public StateChanged {
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(to, "to");
            Objects.requireNonNull(at, "at");
        }
    }
}
