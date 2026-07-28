// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;

/** Acknowledges receipt of a privilege notification. */
public final class AcknowledgePrivilegeNotificationCommand extends IntegerServerMessage {
    public AcknowledgePrivilegeNotificationCommand(int id) {
        super(MessageCode.Server.ACKNOWLEDGE_NOTIFY_PRIVILEGES, id);
    }

    public int getId() {
        return value();
    }
}
