// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/** Represents errors involving chat rooms. */
public class RoomException extends SoulseekClientException {
    private static final long serialVersionUID = 1L;

    public RoomException() {
        super();
    }

    public RoomException(String message) {
        super(message);
    }

    public RoomException(String message, Throwable cause) {
        super(message, cause);
    }
}
