// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import java.net.InetAddress;

/** Reports the current distributed parent's IP address. */
public final class ParentsIPCommand implements OutgoingMessage {
    private final InetAddress ipAddress;

    public ParentsIPCommand() {
        this(null);
    }

    public ParentsIPCommand(InetAddress ipAddress) {
        this.ipAddress = ipAddress;
    }

    public InetAddress getIpAddress() {
        return ipAddress;
    }

    @Override
    public byte[] toByteArray() {
        byte[] bytes = ipAddress == null ? new byte[0] : ipAddress.getAddress();
        for (int left = 0, right = bytes.length - 1; left < right; left++, right--) {
            byte swap = bytes[left];
            bytes[left] = bytes[right];
            bytes[right] = swap;
        }
        return new MessageBuilder()
                .writeCode(MessageCode.Server.PARENTS_IP)
                .writeBytes(bytes)
                .build();
    }
}
