// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;

/** Removes a user from the server-side watch list. */
public final class UnwatchUserCommand extends StringServerMessage {
    public UnwatchUserCommand(String username) {
        super(MessageCode.Server.UNWATCH_USER, username);
    }

    public String getUsername() {
        return value();
    }
}
