// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging;

/**
 * A numeric Soulseek protocol message code.
 */
public interface ProtocolCode {
    /**
     * Returns the protocol value.
     *
     * @return the numeric code
     */
    int getValue();

    /**
     * Returns the encoded code width.
     *
     * @return one or four bytes
     */
    int getByteLength();
}
