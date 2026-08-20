// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.exceptions.MessageException;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import dev.slsk.internal.user.UserInfoMessage;
import java.util.Objects;

/** Serializes and parses user-info response messages. */
public final class UserInfoResponseFactory {
    private UserInfoResponseFactory() {}

    /** Parses a user-info response. */
    public static UserInfoMessage fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(bytes, MessageCode.Peer.class);
        MessageCode.Peer code = reader.readCode();
        if (code != MessageCode.Peer.INFO_RESPONSE) {
            throw new MessageException("Message Code mismatch creating UserInfoMessage " + "(expected: 16, received: "
                    + code.getValue() + ")");
        }

        String description = reader.readString();
        boolean hasPicture = reader.readByte() > 0;
        byte[] picture = null;
        if (hasPicture) {
            picture = reader.readBytes(reader.readInteger());
        }
        int uploadSlots = reader.readInteger();
        int queueLength = reader.readInteger();
        boolean hasFreeUploadSlot = reader.readByte() > 0;
        return new UserInfoMessage(description, uploadSlots, queueLength, hasFreeUploadSlot, picture);
    }

    /** Serializes a user-info response. */
    public static byte[] toByteArray(UserInfoMessage userInfo) {
        Objects.requireNonNull(userInfo, "userInfo");
        MessageBuilder builder = new MessageBuilder()
                .writeCode(MessageCode.Peer.INFO_RESPONSE)
                .writeString(userInfo.description())
                .writeByte(userInfo.hasPicture() ? 1 : 0);
        if (userInfo.hasPicture()) {
            builder.writeInteger(userInfo.picture().length).writeBytes(userInfo.picture());
        }
        return builder.writeInteger(userInfo.uploadSlots())
                .writeInteger(userInfo.queueLength())
                .writeByte(userInfo.freeUploadSlot() ? 1 : 0)
                .build();
    }
}
