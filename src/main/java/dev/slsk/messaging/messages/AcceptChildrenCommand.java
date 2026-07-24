// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageCode;

/** Reports whether distributed child connections are accepted. */
public final class AcceptChildrenCommand extends ByteServerMessage {
    public AcceptChildrenCommand(boolean accepted) {
        super(MessageCode.Server.ACCEPT_CHILDREN, accepted);
    }

    public boolean isAccepted() {
        return value();
    }
}
