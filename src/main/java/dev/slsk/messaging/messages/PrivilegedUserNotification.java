// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageCode;

/** Parses notification of a newly privileged user. */
public final class PrivilegedUserNotification implements IncomingMessage {
    private PrivilegedUserNotification() {}

    /** Parses the privileged username. */
    public static String fromByteArray(byte[] bytes) {
        return ServerMessageParser.reader(bytes, MessageCode.Server.ADD_PRIVILEGED_USER, "PrivilegedUserNotification")
                .readString();
    }
}
