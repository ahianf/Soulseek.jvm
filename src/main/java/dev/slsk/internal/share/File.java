// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.share;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A file within search and browse results.
 */
public class File {
    private final int attributeCount;
    private final List<FileAttribute> attributes;
    private final Integer bitDepth;
    private final Integer bitRate;
    private final int code;
    private final String extension;
    private final String filename;
    private final Boolean variableBitRate;
    private final Integer length;
    private final Integer sampleRate;
    private final long size;

    /**
     * Creates a file with no attributes.
     *
     * @param code the file code
     * @param filename the file name
     * @param size the file size in bytes
     * @param extension the file extension
     */
    public File(int code, String filename, long size, String extension) {
        this(code, filename, size, extension, null);
    }

    /**
     * Creates a file.
     *
     * @param code the file code
     * @param filename the file name
     * @param size the file size in bytes
     * @param extension the file extension
     * @param attributeList the optional sequence of file attributes
     */
    public File(
            int code, String filename, long size, String extension, Iterable<? extends FileAttribute> attributeList) {
        this.code = code;
        this.filename = filename;
        this.size = size;
        this.extension = extension;

        List<FileAttribute> copiedAttributes = new ArrayList<>();
        if (attributeList != null) {
            attributeList.forEach(copiedAttributes::add);
        }
        attributes = Collections.unmodifiableList(copiedAttributes);
        attributeCount = attributes.size();

        Integer resolvedBitDepth = null;
        Integer resolvedBitRate = null;
        Boolean resolvedVariableBitRate = null;
        Integer resolvedLength = null;
        Integer resolvedSampleRate = null;

        for (FileAttribute attribute : attributes) {
            switch (attribute.getType()) {
                case BIT_DEPTH -> resolvedBitDepth = attribute.getValue();
                case BIT_RATE -> resolvedBitRate = attribute.getValue();
                case VARIABLE_BIT_RATE -> resolvedVariableBitRate = attribute.getValue() != 0;
                case LENGTH -> resolvedLength = attribute.getValue();
                case SAMPLE_RATE -> resolvedSampleRate = attribute.getValue();
            }
        }

        bitDepth = resolvedBitDepth;
        bitRate = resolvedBitRate;
        variableBitRate = resolvedVariableBitRate;
        length = resolvedLength;
        sampleRate = resolvedSampleRate;
    }

    /**
     * Returns the number of file attributes.
     *
     * @return the number of file attributes
     */
    public final int getAttributeCount() {
        return attributeCount;
    }

    /**
     * Returns the file attributes as an immutable snapshot.
     *
     * @return the file attributes
     */
    public final List<FileAttribute> getAttributes() {
        return attributes;
    }

    /**
     * Returns the bit-depth attribute, if present.
     *
     * @return the bit depth, or {@code null}
     */
    public final Integer getBitDepth() {
        return bitDepth;
    }

    /**
     * Returns the bit-rate attribute, if present.
     *
     * @return the bit rate, or {@code null}
     */
    public final Integer getBitRate() {
        return bitRate;
    }

    /**
     * Returns the file code.
     *
     * @return the file code
     */
    public final int getCode() {
        return code;
    }

    /**
     * Returns the file extension.
     *
     * @return the file extension
     */
    public final String getExtension() {
        return extension;
    }

    /**
     * Returns the file name.
     *
     * @return the file name
     */
    public final String getFilename() {
        return filename;
    }

    /**
     * Returns whether the variable-bit-rate attribute is nonzero, if present.
     *
     * @return {@code true} for variable bit rate, {@code false} for constant
     *     bit rate, or {@code null} when the attribute is absent
     */
    public final Boolean isVariableBitRate() {
        return variableBitRate;
    }

    /**
     * Returns the length attribute, if present.
     *
     * @return the length, or {@code null}
     */
    public final Integer getLength() {
        return length;
    }

    /**
     * Returns the sample-rate attribute, if present.
     *
     * @return the sample rate, or {@code null}
     */
    public final Integer getSampleRate() {
        return sampleRate;
    }

    /**
     * Returns the file size in bytes.
     *
     * @return the file size
     */
    public final long getSize() {
        return size;
    }
}
