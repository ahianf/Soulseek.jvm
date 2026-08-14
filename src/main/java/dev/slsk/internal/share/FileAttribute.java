// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.share;

import java.util.Objects;

/**
 * A file attribute.
 */
public class FileAttribute {
    private final FileAttributeType type;
    private final int value;

    /**
     * Creates a file attribute.
     *
     * @param type the attribute type
     * @param value the attribute value
     */
    public FileAttribute(FileAttributeType type, int value) {
        this.type = Objects.requireNonNull(type, "type");
        this.value = value;
    }

    /**
     * Returns the attribute type.
     *
     * @return the attribute type
     */
    public final FileAttributeType getType() {
        return type;
    }

    /**
     * Returns the attribute value.
     *
     * @return the attribute value
     */
    public final int getValue() {
        return value;
    }
}
