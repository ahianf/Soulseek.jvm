// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.diagnostics.MeshState;
import dev.slsk.diagnostics.Metrics;
import dev.slsk.events.MeshEvent;

/**
 * What the library is doing, and how it is placed on the network.
 *
 * <p>Read-only throughout. Library operation is logged through SLF4J; this
 * facet contains observable network state and counters rather than logs.
 */
public interface Diagnostics {

    /**
     * Returns current counters.
     *
     * @return a snapshot
     */
    Metrics metrics();

    /**
     * Returns our position in the distributed search mesh.
     *
     * @return a snapshot
     */
    MeshState mesh();

    /**
     * Returns the stream of mesh state changes.
     *
     * @return the event stream
     */
    EventStream<MeshEvent> meshEvents();

}
