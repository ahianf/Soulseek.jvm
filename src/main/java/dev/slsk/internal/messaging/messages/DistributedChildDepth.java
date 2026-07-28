// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.exceptions.MessageException;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;

/** Informs a distributed parent of its child's depth. */
public final class DistributedChildDepth implements IncomingMessage, OutgoingMessage {
    private final int depth;

    /** Creates a child-depth message. */
    public DistributedChildDepth(int depth) {
        this.depth = depth;
    }

    /** Returns the child's current depth. */
    public int getDepth() {
        return depth;
    }

    /**
     * Parses a child-depth message.
     *
     * @param bytes the framed message
     * @return the parsed message
     */
    public static DistributedChildDepth fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Distributed> reader = new MessageReader<>(bytes, MessageCode.Distributed.class);
        MessageCode.Distributed code = reader.readCode();
        if (code != MessageCode.Distributed.CHILD_DEPTH) {
            throw new MessageException("Message Code mismatch creating DistributedChildDepth "
                    + "(expected: 7, received: " + code.getValue() + ")");
        }

        return new DistributedChildDepth(reader.readInteger());
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Distributed.CHILD_DEPTH)
                .writeInteger(depth)
                .build();
    }
}
