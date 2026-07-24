// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.exceptions.MessageException;
import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;

/** Informs distributed children of the current branch level. */
public final class DistributedBranchLevel implements IIncomingMessage, IOutgoingMessage {
    private final int level;

    /** Creates a branch-level message. */
    public DistributedBranchLevel(int level) {
        this.level = level;
    }

    /** Returns the current branch level. */
    public int getLevel() {
        return level;
    }

    /**
     * Parses a branch-level message.
     *
     * @param bytes the framed message
     * @return the parsed message
     */
    public static DistributedBranchLevel fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Distributed> reader = new MessageReader<>(bytes, MessageCode.Distributed.class);
        MessageCode.Distributed code = reader.readCode();
        if (code != MessageCode.Distributed.BRANCH_LEVEL) {
            throw new MessageException("Message Code mismatch creating DistributedBranchLevel "
                    + "(expected: 4, received: " + code.getValue() + ")");
        }

        return new DistributedBranchLevel(reader.readInteger());
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Distributed.BRANCH_LEVEL)
                .writeInteger(level)
                .build();
    }
}
