// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.exceptions;

/** Indicates that joining a requested chat room is forbidden. */
public class RoomJoinForbiddenException extends RoomException {
    private static final long serialVersionUID = 1L;

    public RoomJoinForbiddenException() {
        super();
    }

    public RoomJoinForbiddenException(String message) {
        super(message);
    }

    public RoomJoinForbiddenException(String message, Throwable cause) {
        super(message, cause);
    }
}
