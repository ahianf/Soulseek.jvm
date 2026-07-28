// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

/**
 * The attribute codes Soulseek defines for a shared file.
 *
 * <p>The codes are wire values and are not contiguous — there is no 3 — so this
 * carries the number rather than relying on ordinal position.
 */
public enum FileAttributeType {

    /** Kilobits per second. */
    BIT_RATE(0),

    /** Playing time, in seconds. */
    LENGTH(1),

    /** Non-zero if the file is variable bit rate. */
    VARIABLE_BIT_RATE(2),

    /** Hertz. */
    SAMPLE_RATE(4),

    /** Bits per sample. */
    BIT_DEPTH(5);

    private final int code;

    FileAttributeType(int code) {
        this.code = code;
    }

    /**
     * Returns the wire code.
     *
     * @return the code
     */
    public int code() {
        return code;
    }

    /**
     * Returns the type for a wire code.
     *
     * @param code the code
     * @return the type, or {@code null} if this library does not model it
     */
    public static FileAttributeType fromCode(int code) {
        for (FileAttributeType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        return null;
    }
}
