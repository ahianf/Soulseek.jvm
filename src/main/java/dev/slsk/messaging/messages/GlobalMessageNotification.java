// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageCode;

/** Parses an incoming global administrator message. */
public final class GlobalMessageNotification implements IncomingMessage {
    private GlobalMessageNotification() {}

    /** Parses the global message text. */
    public static String fromByteArray(byte[] bytes) {
        return ServerMessageParser.reader(bytes, MessageCode.Server.GLOBAL_ADMIN_MESSAGE, "GlobalMessageNotification")
                .readString();
    }
}
