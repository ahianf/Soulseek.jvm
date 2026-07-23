// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.diagnostics;

/**
 * Diagnostic message level.
 */
public enum DiagnosticLevel {
    /** No diagnostic messages. */
    NONE(0),

    /** Warning messages. */
    WARNING(1),

    /** Informational messages. */
    INFO(2),

    /** Debug messages. */
    DEBUG(3),

    /** Trace messages. */
    TRACE(4);

    private final int value;

    DiagnosticLevel(int value) {
        this.value = value;
    }

    /**
     * Returns the level value.
     *
     * @return the level value
     */
    public int getValue() {
        return value;
    }

    /**
     * Returns the level represented by a value.
     *
     * @param value the level value
     * @return the matching level
     * @throws IllegalArgumentException when the value is unknown
     */
    public static DiagnosticLevel fromValue(int value) {
        for (DiagnosticLevel level : values()) {
            if (level.value == value) {
                return level;
            }
        }
        throw new IllegalArgumentException("Unknown diagnostic level: " + value);
    }
}
