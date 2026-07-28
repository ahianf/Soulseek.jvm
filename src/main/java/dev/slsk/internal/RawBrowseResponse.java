// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import java.io.InputStream;
import java.util.Objects;

/** A raw browse response presented as binary stream data. */
public class RawBrowseResponse extends BrowseResponse {
    private final long length;
    private final InputStream stream;

    /**
     * Creates a raw browse response.
     *
     * <p>The stream is closed after the response is written.</p>
     */
    public RawBrowseResponse(long length, InputStream stream) {
        if (length <= 0) {
            throw new IllegalArgumentException("The response length must be greater than zero");
        }
        this.stream = Objects.requireNonNull(stream, "The specified input stream is null");
        this.length = length;
    }

    /** Returns the response length in bytes. */
    public long getLength() {
        return length;
    }

    /** Returns the raw input stream. */
    public InputStream getStream() {
        return stream;
    }
}
