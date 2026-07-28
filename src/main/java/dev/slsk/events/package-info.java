// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

/**
 * What the library reports: one sealed event hierarchy per facet, all rooted at
 * {@link dev.slsk.events.SoulseekEvent}.
 *
 * <p>A consumer receives these through {@link dev.slsk.EventStream}, reached
 * from the facet that owns the state they describe. They are deltas — the state
 * itself is always available from the facet synchronously, so a consumer that
 * misses every event and polls instead is degraded rather than broken.
 */
package dev.slsk.events;
