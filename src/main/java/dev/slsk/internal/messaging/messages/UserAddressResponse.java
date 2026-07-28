// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Objects;

/** The response to a request for a peer's network address. */
public final class UserAddressResponse implements IncomingMessage {
    private final InetAddress ipAddress;
    private final InetSocketAddress ipEndpoint;
    private final int port;
    private final String username;

    /** Creates an address response from an address and port. */
    public UserAddressResponse(String username, InetAddress ipAddress, int port) {
        this(username, new InetSocketAddress(ipAddress, port));
    }

    /** Creates an address response from an endpoint. */
    public UserAddressResponse(String username, InetSocketAddress endpoint) {
        this.username = username;
        this.ipEndpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.ipAddress = endpoint.getAddress();
        this.port = endpoint.getPort();
    }

    public InetAddress getIpAddress() {
        return ipAddress;
    }

    public InetSocketAddress getIpEndpoint() {
        return ipEndpoint;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    /** Parses an address response. */
    public static UserAddressResponse fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader =
                ServerMessageParser.reader(bytes, MessageCode.Server.GET_PEER_ADDRESS, "UserAddressResponse");
        String username = reader.readString();
        InetAddress address = ServerAddressCodec.readIpv4(reader);
        return new UserAddressResponse(username, address, reader.readInteger());
    }
}
