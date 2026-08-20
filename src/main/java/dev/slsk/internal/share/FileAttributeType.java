// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.share;

import java.util.Optional;

/**
 * File attribute type.
 */
public enum FileAttributeType {
    /** Bit rate in kbps. */
    BIT_RATE(0),

    /** Length in seconds. */
    LENGTH(1),

    /** Variable bit rate flag: zero for constant, one for variable. */
    VARIABLE_BIT_RATE(2),

    /** Sample rate in kHz. */
    SAMPLE_RATE(4),

    /** Bit depth in bits. */
    BIT_DEPTH(5);

    private final int value;

    FileAttributeType(int value) {
        this.value = value;
    }

    /**
     * Returns the protocol value.
     *
     * @return the protocol value
     */
    public int getValue() {
        return value;
    }

    /**
     * Returns the type represented by a protocol value.
     *
     * @param value the protocol value
     * @return the matching type
     * @throws IllegalArgumentException when the value is unknown
     */
    public static FileAttributeType fromValue(int value) {
        return tryFromValue(value)
                .orElseThrow(() -> new IllegalArgumentException("Unknown file attribute type: " + value));
    }

    /**
     * Returns the type represented by a protocol value, if this client knows
     * it.
     *
     * <p>Peers run clients newer than this one, and the C# source reads their
     * attribute types tolerantly. Throwing instead meant one nonstandard
     * attribute discarded the entire search or browse response it arrived in.
     *
     * @param value the protocol value
     * @return the matching type
     */
    public static Optional<FileAttributeType> tryFromValue(int value) {
        for (FileAttributeType type : values()) {
            if (type.value == value) {
                return Optional.of(type);
            }
        }
        return Optional.empty();
    }
}
