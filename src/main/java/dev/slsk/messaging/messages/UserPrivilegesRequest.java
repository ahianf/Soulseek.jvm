// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageCode;

/** Requests a user's privilege status. */
public final class UserPrivilegesRequest extends StringServerMessage {
    public UserPrivilegesRequest(String username) {
        super(MessageCode.Server.USER_PRIVILEGES, username);
    }

    public String getUsername() {
        return value();
    }
}
