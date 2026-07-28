// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import java.io.IOException;

/**
 * Optional capability for upload streams with an explicit byte position.
 *
 * <p>Java's {@link java.io.InputStream} has no general seek contract. An
 * upload stream may implement this interface to support automatic resume
 * seeking and exact final-position reporting.</p>
 */
public interface PositionableInputStream {
    /** Returns the current byte position. */
    long getPosition() throws IOException;

    /** Moves to an absolute byte position. */
    void setPosition(long position) throws IOException;
}
