// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import java.net.InetAddress;

/** The response to a login request. */
public final class LoginResponse implements IncomingMessage {
    private final String hash;
    private final InetAddress ipAddress;
    private final boolean supporter;
    private final String message;
    private final boolean succeeded;

    /** Creates a failed or minimally populated login response. */
    public LoginResponse(boolean succeeded, String message) {
        this(succeeded, message, null, null, null);
    }

    /** Creates a login response with a client address. */
    public LoginResponse(boolean succeeded, String message, InetAddress ipAddress) {
        this(succeeded, message, ipAddress, null, null);
    }

    /** Creates a login response. */
    public LoginResponse(boolean succeeded, String message, InetAddress ipAddress, String hash, Boolean isSupporter) {
        this.succeeded = succeeded;
        this.message = message;
        this.ipAddress = ipAddress;
        this.hash = hash;
        this.supporter = isSupporter != null && isSupporter;
    }

    public String getHash() {
        return hash;
    }

    public InetAddress getIpAddress() {
        return ipAddress;
    }

    public boolean isSupporter() {
        return supporter;
    }

    public String getMessage() {
        return message;
    }

    public boolean isSucceeded() {
        return succeeded;
    }

    /** Parses a login response. */
    public static LoginResponse fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader =
                ServerMessageParser.reader(bytes, MessageCode.Server.LOGIN, "LoginResponse", false);
        boolean succeeded = reader.readByte() == 1;
        String message = reader.readString();
        if (!succeeded) {
            return new LoginResponse(false, message);
        }
        InetAddress ipAddress = ServerAddressCodec.readIpv4(reader);
        String hash = reader.readString();
        boolean supporter = reader.readByte() == 1;
        return new LoginResponse(true, message, ipAddress, hash, supporter);
    }
}
