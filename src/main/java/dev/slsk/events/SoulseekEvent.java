// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.events;

import java.time.Instant;

/**
 * Anything the library reports as having happened.
 *
 * <p>Every event carries {@link #at()}, so a consumer can order and age events
 * without the library having to say so twice, and every facet's event type
 * extends this. The hierarchies below it are sealed: a consumer projecting them
 * writes one {@code switch}, and when a new event type is added the compiler
 * points at every projection that has not accounted for it. That is the whole
 * reason the events are types rather than a listener method each.
 *
 * <p>Events are deltas on state the facet already exposes. Nothing here is a
 * historical record: the library does not accumulate chat scrollback, past
 * searches, or completed transfers, and a consumer that wants history keeps its
 * own. What the library holds is what is true now, and what it publishes is how
 * that changed.
 */
public interface SoulseekEvent {

    /**
     * Returns when this happened.
     *
     * @return the instant the event was raised
     */
    Instant at();
}
