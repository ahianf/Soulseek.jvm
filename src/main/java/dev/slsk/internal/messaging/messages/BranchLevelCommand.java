// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;

/** Reports the current distributed branch level. */
public final class BranchLevelCommand extends IntegerServerMessage {
    public BranchLevelCommand(int level) {
        super(MessageCode.Server.BRANCH_LEVEL, level);
    }

    public int getLevel() {
        return value();
    }
}
