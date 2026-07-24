// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;

/** Requests a direct or indirect peer connection. */
public final class ConnectToPeerRequest implements IOutgoingMessage {
    private final int token;
    private final String type;
    private final String username;

    public ConnectToPeerRequest(int token, String username, String type) {
        this.token = token;
        this.username = username;
        this.type = type;
    }

    public int getToken() {
        return token;
    }

    public String getType() {
        return type;
    }

    public String getUsername() {
        return username;
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Server.CONNECT_TO_PEER)
                .writeInteger(token)
                .writeString(username)
                .writeString(type)
                .build();
    }
}
