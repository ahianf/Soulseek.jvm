// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.exceptions.MessageException;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;

/** Reports the current place of a file in a peer's queue. */
public final class PlaceInQueueResponse implements IncomingMessage, OutgoingMessage {
    private final String filename;
    private final int placeInQueue;

    /** Creates a place-in-queue response. */
    public PlaceInQueueResponse(String filename, int placeInQueue) {
        this.filename = filename;
        this.placeInQueue = placeInQueue;
    }

    /** Returns the checked filename. */
    public String getFilename() {
        return filename;
    }

    /** Returns the current place in the peer's queue. */
    public int getPlaceInQueue() {
        return placeInQueue;
    }

    /** Parses a place-in-queue response. */
    public static PlaceInQueueResponse fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(bytes, MessageCode.Peer.class);
        MessageCode.Peer code = reader.readCode();
        if (code != MessageCode.Peer.PLACE_IN_QUEUE_RESPONSE) {
            throw new MessageException("Message Code mismatch creating PlaceInQueueResponse "
                    + "(expected: 44, received: " + code.getValue() + ")");
        }

        String parsedFilename = reader.readString();
        int parsedPlace = reader.readInteger();
        return new PlaceInQueueResponse(parsedFilename, parsedPlace);
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Peer.PLACE_IN_QUEUE_RESPONSE)
                .writeString(filename)
                .writeInteger(placeInQueue)
                .build();
    }
}
