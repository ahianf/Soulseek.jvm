// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Objects;

/** A server response soliciting a peer connection. */
public final class ConnectToPeerResponse implements IncomingMessage {
    private final InetAddress ipAddress;
    private final InetSocketAddress ipEndpoint;
    private final boolean privileged;
    private final int port;
    private final int token;
    private final String type;
    private final String username;

    /** Creates a response from an address and port. */
    public ConnectToPeerResponse(
            String username, String type, InetAddress ipAddress, int port, int token, boolean isPrivileged) {
        this(username, type, new InetSocketAddress(ipAddress, port), token, isPrivileged);
    }

    /** Creates a response from an endpoint. */
    public ConnectToPeerResponse(
            String username, String type, InetSocketAddress endpoint, int token, boolean isPrivileged) {
        this.username = username;
        this.type = type;
        this.token = token;
        this.ipEndpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.privileged = isPrivileged;
        this.ipAddress = endpoint.getAddress();
        this.port = endpoint.getPort();
    }

    public InetAddress getIpAddress() {
        return ipAddress;
    }

    public InetSocketAddress getIpEndpoint() {
        return ipEndpoint;
    }

    public boolean isPrivileged() {
        return privileged;
    }

    public int getPort() {
        return port;
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

    /** Parses a connection response. */
    public static ConnectToPeerResponse fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader =
                ServerMessageParser.reader(bytes, MessageCode.Server.CONNECT_TO_PEER, "ConnectToPeerResponse", false);
        String username = reader.readString();
        String type = reader.readString();
        InetAddress address = ServerAddressCodec.readIpv4(reader);
        int port = reader.readInteger();
        int token = reader.readInteger();
        boolean privileged = reader.readByte() > 0;
        return new ConnectToPeerResponse(username, type, address, port, token, privileged);
    }
}
