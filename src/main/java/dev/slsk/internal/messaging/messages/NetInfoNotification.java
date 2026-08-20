// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/** An incoming list of distributed parent candidates. */
public record NetInfoNotification(int parentCount, List<NetInfoParent> parents) implements IncomingMessage {

    public NetInfoNotification {
        parents = List.copyOf(parents);
    }

    public static NetInfoNotification fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader =
                ServerMessageParser.reader(bytes, MessageCode.Server.NET_INFO, "NetInfoNotification");
        int parentCount = reader.readInteger();
        List<NetInfoParent> parents = new ArrayList<>();
        for (int index = 0; index < parentCount; index++) {
            String username = reader.readString();
            InetAddress address = ServerAddressCodec.readIpv4(reader);
            int port = reader.readInteger();
            parents.add(new NetInfoParent(username, address, port));
        }
        return new NetInfoNotification(parentCount, parents);
    }
}
