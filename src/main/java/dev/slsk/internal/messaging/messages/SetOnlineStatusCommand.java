// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.UserPresence;
import dev.slsk.internal.messaging.MessageCode;
import java.util.Objects;

/** Reports the local user's current presence. */
public final class SetOnlineStatusCommand extends IntegerServerMessage {
    private final UserPresence status;

    public SetOnlineStatusCommand(UserPresence status) {
        super(
                MessageCode.Server.SET_ONLINE_STATUS,
                Objects.requireNonNull(status, "status").getValue());
        this.status = status;
    }

    public UserPresence getStatus() {
        return status;
    }
}
