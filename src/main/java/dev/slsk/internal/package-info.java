// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

/**
 * Implementation of the Soulseek.jvm library. Not exported, not API, and no
 * part of it is covered by any compatibility promise.
 *
 * <p>This is where the ported implementation lives. Its shape still mirrors the
 * C# original it came from, because that is where it came from; that is a fact
 * about its history and not a constraint on its future. The 1.0 facets are
 * built over these types, and the internal architecture is rewritten after 1.0
 * ships.
 *
 * <p>Because nothing here is exported, the no-prune rule does not apply:
 * unexported code with no caller is ordinary dead code. What is <em>not</em>
 * permitted is losing a capability. {@code
 * src/test/resources/capability-baseline.txt} records everything the library
 * could do before the rewrite, and the facet-exhaustiveness test holds this
 * package to it.
 */
package dev.slsk.internal;
