// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;

/** Requests the shared file list from a peer. */
public final class BrowseRequest implements IOutgoingMessage {
    /** Creates a browse request. */
    public BrowseRequest() {}

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder().writeCode(MessageCode.Peer.BROWSE_REQUEST).build();
    }
}
