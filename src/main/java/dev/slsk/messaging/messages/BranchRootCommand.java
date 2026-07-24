// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageCode;

/** Reports the current distributed branch-root username. */
public final class BranchRootCommand extends StringServerMessage {
    public BranchRootCommand(String username) {
        super(MessageCode.Server.BRANCH_ROOT, username);
    }

    public String getUsername() {
        return value();
    }
}
