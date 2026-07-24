// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageCode;

/** Adds a user to the server-side watch list. */
public final class WatchUserRequest extends StringServerMessage {
    public WatchUserRequest(String username) {
        super(MessageCode.Server.WATCH_USER, username);
    }

    public String getUsername() {
        return value();
    }
}
