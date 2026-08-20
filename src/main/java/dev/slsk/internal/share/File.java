// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.share;

import java.util.List;

/** A file within search and browse results. */
public record File(int code, String filename, long size, String extension, List<FileAttribute> attributes) {
    public File {
        attributes = attributes == null ? List.of() : List.copyOf(attributes);
    }

    /** Creates a file with no attributes. */
    public File(int code, String filename, long size, String extension) {
        this(code, filename, size, extension, List.of());
    }

    /** Returns the number of file attributes. */
    public int attributeCount() {
        return attributes.size();
    }

    /** Returns the bit-depth attribute, if present. */
    public Integer bitDepth() {
        return value(FileAttributeType.BIT_DEPTH);
    }

    /** Returns the bit-rate attribute, if present. */
    public Integer bitRate() {
        return value(FileAttributeType.BIT_RATE);
    }

    /** Returns whether the variable-bit-rate attribute is nonzero, if present. */
    public Boolean variableBitRate() {
        Integer value = value(FileAttributeType.VARIABLE_BIT_RATE);
        return value == null ? null : value != 0;
    }

    /** Returns the length attribute, if present. */
    public Integer length() {
        return value(FileAttributeType.LENGTH);
    }

    /** Returns the sample-rate attribute, if present. */
    public Integer sampleRate() {
        return value(FileAttributeType.SAMPLE_RATE);
    }

    private Integer value(FileAttributeType type) {
        return attributes.stream()
                .filter(attribute -> attribute.type() == type)
                .map(FileAttribute::value)
                .reduce((first, replacement) -> replacement)
                .orElse(null);
    }
}
