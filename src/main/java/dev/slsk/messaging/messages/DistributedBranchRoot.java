// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.exceptions.MessageException;
import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;

/** Informs distributed children of the current branch root. */
public final class DistributedBranchRoot implements IncomingMessage, OutgoingMessage {
    private final String username;

    /** Creates a branch-root message. */
    public DistributedBranchRoot(String username) {
        this.username = username;
    }

    /** Returns the branch-root username. */
    public String getUsername() {
        return username;
    }

    /**
     * Parses a branch-root message.
     *
     * @param bytes the framed message
     * @return the parsed message
     */
    public static DistributedBranchRoot fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Distributed> reader = new MessageReader<>(bytes, MessageCode.Distributed.class);
        MessageCode.Distributed code = reader.readCode();
        if (code != MessageCode.Distributed.BRANCH_ROOT) {
            throw new MessageException("Message Code mismatch creating DistributedBranchRoot "
                    + "(expected: 5, received: " + code.getValue() + ")");
        }

        return new DistributedBranchRoot(reader.readString());
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Distributed.BRANCH_ROOT)
                .writeString(username)
                .build();
    }
}
