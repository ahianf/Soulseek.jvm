// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.diagnostics.MeshState;
import dev.slsk.diagnostics.Metrics;
import dev.slsk.events.DiagnosticEvent;
import dev.slsk.events.MeshEvent;

/**
 * What the library is doing, and how it is placed on the network.
 *
 * <p>Read-only throughout. Nothing here changes the client's behaviour except
 * {@link #protocolTrace(boolean)}, which changes only how much it says.
 */
public interface Diagnostics {

    /**
     * Returns the stream of diagnostic messages, including contained listener
     * faults.
     *
     * @return the event stream
     */
    EventStream<DiagnosticEvent> events();

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

    /**
     * Turns per-message protocol tracing on or off.
     *
     * <p>Expensive and very loud. An idempotent intent: enabling it twice is
     * enabling it.
     *
     * @param enabled whether to trace
     */
    void protocolTrace(boolean enabled);
}
