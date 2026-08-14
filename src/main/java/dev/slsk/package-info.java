// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

/**
 * Client entry points and shared control contracts for Soulseek.jvm.
 *
 * <p>This root package is intentionally narrow: it contains {@link Soulseek},
 * its capability facets, and the cancellation, event-stream, attachment, and
 * subscription contracts shared by those facets. Immutable requests and
 * snapshots live in the capability package that owns them, such as {@code
 * dev.slsk.search}, {@code dev.slsk.transfer}, and {@code
 * dev.slsk.user}.
 */
package dev.slsk;
