// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;

/** Requests the shared file list from a peer. */
public final class BrowseRequestMessage implements OutgoingMessage {
    /** Creates a browse request. */
    public BrowseRequestMessage() {}

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder().writeCode(MessageCode.Peer.BROWSE_REQUEST).build();
    }
}
