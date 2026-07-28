// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;

/** Reports the current distributed child depth. */
public final class ChildDepthCommand extends IntegerServerMessage {
    public ChildDepthCommand(int depth) {
        super(MessageCode.Server.CHILD_DEPTH, depth);
    }

    public int getDepth() {
        return value();
    }
}
