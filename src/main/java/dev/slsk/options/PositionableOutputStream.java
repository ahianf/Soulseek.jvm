// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.options;

import java.io.IOException;

/**
 * Optional capability for download streams with an explicit byte position.
 *
 * <p>Java's {@link java.io.OutputStream} has no general seek contract. A
 * download stream may implement this interface to support automatic resume
 * seeking and exact final-position reporting.</p>
 */
public interface PositionableOutputStream {
    /** Returns the current byte position. */
    long getPosition() throws IOException;

    /** Moves to an absolute byte position. */
    void setPosition(long position) throws IOException;
}
