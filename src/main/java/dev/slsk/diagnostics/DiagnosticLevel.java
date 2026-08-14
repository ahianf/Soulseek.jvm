// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.diagnostics;

/**
 * How much the library says about what it is doing.
 *
 * <p>Ordered from silent to loudest, so a filter is a comparison.
 */
public enum DiagnosticLevel {

    /** Say nothing. */
    NONE,

    /** Only things that went wrong. */
    WARNING,

    /** Notable events. */
    INFO,

    /** Enough to follow the library's decisions. */
    DEBUG,

    /** Everything, including per-message detail. */
    TRACE
}
