// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;

/** The bidirectional password-change command. */
public final class NewPassword extends StringServerMessage implements IncomingMessage {

    /** Creates a password-change message. */
    public NewPassword(String password) {
        super(MessageCode.Server.NEW_PASSWORD, password);
    }

    /** Returns the new password. */
    public String getPassword() {
        return value();
    }

    /** Parses a password-change message. */
    public static NewPassword fromByteArray(byte[] bytes) {
        MessageReader<MessageCode.Server> reader =
                ServerMessageParser.reader(bytes, MessageCode.Server.NEW_PASSWORD, "NewPassword");
        return new NewPassword(reader.readString());
    }
}
