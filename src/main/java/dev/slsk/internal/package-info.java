// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

/**
 * Implementation of the Soulseek.jvm library. Not exported, not API, and no
 * part of it is covered by any compatibility promise.
 *
 * <p>The root of this package is the composition and orchestration layer:
 * client facets, domains, queues, runs, registries, and supervisors. Leaf
 * protocol and state types live under the subsystem that owns them, including
 * {@code connection}, {@code events}, {@code messaging}, {@code network},
 * {@code room}, {@code search}, {@code share}, {@code transfer}, and {@code
 * user}. Cross-cutting implementation primitives live under {@code common}
 * and {@code options}.
 *
 * <p>Because nothing here is exported, the no-prune rule does not apply:
 * unexported code with no caller is ordinary dead code. What is <em>not</em>
 * permitted is losing a capability. {@code
 * src/test/resources/capability-baseline.txt} records everything the library
 * could do before the rewrite, and the facet-exhaustiveness test holds this
 * package to it.
 */
package dev.slsk.internal;
