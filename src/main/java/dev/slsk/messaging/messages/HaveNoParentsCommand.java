// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageCode;

/** Reports whether a distributed parent connection is needed. */
public final class HaveNoParentsCommand extends ByteServerMessage {
    public HaveNoParentsCommand(boolean haveNoParents) {
        super(MessageCode.Server.HAVE_NO_PARENTS, haveNoParents);
    }

    public boolean hasNoParents() {
        return value();
    }
}
