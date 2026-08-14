// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.transfer;

/**
 * File transfer direction.
 */
public enum TransferDirection {
    /** Download from a remote peer to the local client. */
    DOWNLOAD(0),

    /** Upload from the local client to a remote peer. */
    UPLOAD(1);

    private final int value;

    TransferDirection(int value) {
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
     * Returns the direction represented by a protocol value.
     *
     * @param value the protocol value
     * @return the matching direction
     * @throws IllegalArgumentException when the value is unknown
     */
    public static TransferDirection fromValue(int value) {
        for (TransferDirection direction : values()) {
            if (direction.value == value) {
                return direction;
            }
        }
        throw new IllegalArgumentException("Unknown transfer direction: " + value);
    }
}
