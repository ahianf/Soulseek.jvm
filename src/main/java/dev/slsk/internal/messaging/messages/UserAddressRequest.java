// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;

/** Requests a peer's address. */
public final class UserAddressRequest extends StringServerMessage {
    public UserAddressRequest(String username) {
        super(MessageCode.Server.GET_PEER_ADDRESS, username);
    }

    public String getUsername() {
        return value();
    }
}
