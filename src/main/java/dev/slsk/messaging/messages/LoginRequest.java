// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.common.CommonUtils;
import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;

/** Logs in to the Soulseek server. */
public final class LoginRequest implements IOutgoingMessage {
    private static final int VERSION = 170;

    private final String hash;
    private final int minorVersion;
    private final String password;
    private final String username;

    public LoginRequest(int minorVersion, String username, String password) {
        this.minorVersion = minorVersion;
        this.username = username;
        this.password = password;
        hash = CommonUtils.toMd5Hash(nullToEmpty(username) + nullToEmpty(password));
    }

    public String getHash() {
        return hash;
    }

    public int getMinorVersion() {
        return minorVersion;
    }

    public String getPassword() {
        return password;
    }

    public String getUsername() {
        return username;
    }

    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] toByteArray() {
        return new MessageBuilder()
                .writeCode(MessageCode.Server.LOGIN)
                .writeString(username)
                .writeString(password)
                .writeInteger(VERSION)
                .writeString(hash)
                .writeInteger(minorVersion)
                .build();
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}
