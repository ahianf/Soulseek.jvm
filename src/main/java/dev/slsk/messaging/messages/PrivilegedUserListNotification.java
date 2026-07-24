// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageCode;
import java.util.List;

/** Parses the server's privileged-user list. */
public final class PrivilegedUserListNotification implements IIncomingMessage {

    private PrivilegedUserListNotification() {}

    /** Parses the immutable privileged-user list. */
    public static List<String> fromByteArray(byte[] bytes) {
        return ServerStringListNotification.parse(
                bytes, MessageCode.Server.PRIVILEGED_USERS, "PrivilegedUserListNotification");
    }
}
