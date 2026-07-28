// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

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
        for (FileAttributeType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown file attribute type: " + value);
    }
}
