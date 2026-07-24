// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;

/** Requests information about a user from a peer. */
public class UserInfoRequest implements OutgoingMessage {
    /** Creates a user-info request. */
    public UserInfoRequest() {}

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder().writeCode(MessageCode.Peer.INFO_REQUEST).build();
    }
}
