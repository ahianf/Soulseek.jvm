// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageCode;

/** Requests a peer's presence status. */
public final class UserStatusRequest extends StringServerMessage {
    public UserStatusRequest(String username) {
        super(MessageCode.Server.GET_STATUS, username);
    }

    public String getUsername() {
        return value();
    }
}
