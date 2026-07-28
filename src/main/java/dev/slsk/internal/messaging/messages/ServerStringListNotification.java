// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Shared parser for count-prefixed server string lists. */
final class ServerStringListNotification {
    private ServerStringListNotification() {}

    static List<String> parse(byte[] bytes, MessageCode.Server code, String messageName) {
        MessageReader<MessageCode.Server> reader = ServerMessageParser.reader(bytes, code, messageName, false);
        int count = reader.readInteger();
        List<String> values = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            values.add(reader.readString());
        }
        return Collections.unmodifiableList(values);
    }
}
