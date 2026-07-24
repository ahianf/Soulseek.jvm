// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageReader;
import dev.slsk.messaging.ProtocolCode;
import java.net.InetAddress;
import java.net.UnknownHostException;

/** Reads the protocol's reversed four-byte IPv4 representation. */
final class ServerAddressCodec {
    private ServerAddressCodec() {}

    static <T extends Enum<T> & ProtocolCode> InetAddress readIpv4(MessageReader<T> reader) {
        byte[] address = reader.readBytes(4);
        for (int left = 0, right = address.length - 1; left < right; left++, right--) {
            byte temporary = address[left];
            address[left] = address[right];
            address[right] = temporary;
        }
        try {
            return InetAddress.getByAddress(address);
        } catch (UnknownHostException exception) {
            throw new AssertionError("Four-byte IPv4 addresses are always valid", exception);
        }
    }
}
