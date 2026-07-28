// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;

/** Requests a peer's statistics. */
public final class UserStatisticsRequest extends StringServerMessage {
    public UserStatisticsRequest(String username) {
        super(MessageCode.Server.GET_USER_STATS, username);
    }

    public String getUsername() {
        return value();
    }
}
