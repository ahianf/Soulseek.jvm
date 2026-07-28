// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** An incoming list of distributed parent candidates. */
public final class NetInfoNotification implements IncomingMessage {
    private final int parentCount;
    private final List<NetInfoParent> parents;

    /** Creates a network-info notification. */
    public NetInfoNotification(int parentCount, Iterable<? extends NetInfoParent> parents) {
        this.parentCount = parentCount;
        Objects.requireNonNull(parents, "parents");
        List<NetInfoParent> copy = new ArrayList<>();
        parents.forEach(copy::add);
        this.parents = Collections.unmodifiableList(copy);
    }

    public int getParentCount() {
        return parentCount;
    }

    public List<NetInfoParent> getParents() {
        return parents;
    }

    /** Parses a network-info notification. */
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
