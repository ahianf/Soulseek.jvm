// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;

/** Toggles receipt of private-room invitations. */
public final class PrivateRoomToggle implements IncomingMessage, OutgoingMessage {

    private final boolean acceptInvitations;

    /** Creates a private-room invitation toggle. */
    public PrivateRoomToggle(boolean acceptInvitations) {
        this.acceptInvitations = acceptInvitations;
    }

    /** Returns whether private-room invitations are accepted. */
    public boolean isAcceptInvitations() {
        return acceptInvitations;
    }

    /** Parses a private-room invitation toggle. */
    public static PrivateRoomToggle fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader =
                ServerMessageParser.reader(bytes, MessageCode.Server.PRIVATE_ROOM_TOGGLE, "PrivateRoomToggle");
        return new PrivateRoomToggle(reader.readByte() > 0);
    }

    /** Serializes this private-room invitation toggle. */
    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Server.PRIVATE_ROOM_TOGGLE)
                .writeByte(acceptInvitations ? 1 : 0)
                .build();
    }
}
