// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.UserInfo;
import dev.slsk.exceptions.MessageException;
import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;
import java.util.Objects;

/** Serializes and parses user-info response messages. */
public final class UserInfoResponseFactory {
    private UserInfoResponseFactory() {}

    /** Parses a user-info response. */
    public static UserInfo fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Peer> reader = new MessageReader<>(bytes, MessageCode.Peer.class);
        MessageCode.Peer code = reader.readCode();
        if (code != MessageCode.Peer.INFO_RESPONSE) {
            throw new MessageException(
                    "Message Code mismatch creating UserInfo " + "(expected: 16, received: " + code.getValue() + ")");
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
        return new UserInfo(description, uploadSlots, queueLength, hasFreeUploadSlot, picture);
    }

    /** Serializes a user-info response. */
    public static byte[] toByteArray(UserInfo userInfo) {
        Objects.requireNonNull(userInfo, "userInfo");
        MessageBuilder builder = new MessageBuilder()
                .writeCode(MessageCode.Peer.INFO_RESPONSE)
                .writeString(userInfo.getDescription())
                .writeByte(userInfo.isHasPicture() ? 1 : 0);
        if (userInfo.isHasPicture()) {
            builder.writeInteger(userInfo.getPicture().length).writeBytes(userInfo.getPicture());
        }
        return builder.writeInteger(userInfo.getUploadSlots())
                .writeInteger(userInfo.getQueueLength())
                .writeByte(userInfo.isHasFreeUploadSlot() ? 1 : 0)
                .build();
    }
}
