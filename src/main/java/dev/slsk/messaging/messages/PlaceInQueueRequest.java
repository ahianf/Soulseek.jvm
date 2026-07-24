// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.exceptions.MessageException;
import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;

/** Requests the place of a file in a peer's queue. */
public final class PlaceInQueueRequest implements IIncomingMessage, IOutgoingMessage {
    private final String filename;

    /** Creates a place-in-queue request. */
    public PlaceInQueueRequest(String filename) {
        this.filename = filename;
    }

    /** Returns the filename to check. */
    public String getFilename() {
        return filename;
    }

    /** Parses a place-in-queue request. */
    public static PlaceInQueueRequest fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(bytes, MessageCode.Peer.class);
        MessageCode.Peer code = reader.readCode();
        if (code != MessageCode.Peer.PLACE_IN_QUEUE_REQUEST) {
            throw new MessageException("Message Code mismatch creating PlaceInQueueRequest response "
                    + "(expected: 51, received: " + code.getValue() + ")");
        }

        return new PlaceInQueueRequest(reader.readString());
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Peer.PLACE_IN_QUEUE_REQUEST)
                .writeString(filename)
                .build();
    }
}
