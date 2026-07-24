// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;

/** Shared mechanical serialization for scalar server commands. */
abstract class ServerMessageBase implements OutgoingMessage {
    private final MessageCode.Server code;

    ServerMessageBase(MessageCode.Server code) {
        this.code = code;
    }

    final MessageBuilder builder() {
        return new MessageBuilder().writeCode(code);
    }
}

abstract class EmptyServerMessage extends ServerMessageBase {
    EmptyServerMessage(MessageCode.Server code) {
        super(code);
    }

    @Override
    public final byte[] toByteArray() {
        return builder().build();
    }
}

abstract class IntegerServerMessage extends ServerMessageBase {
    private final int value;

    IntegerServerMessage(MessageCode.Server code, int value) {
        super(code);
        this.value = value;
    }

    final int value() {
        return value;
    }

    @Override
    public final byte[] toByteArray() {
        return builder().writeInteger(value).build();
    }
}

abstract class ByteServerMessage extends ServerMessageBase {
    private final boolean value;

    ByteServerMessage(MessageCode.Server code, boolean value) {
        super(code);
        this.value = value;
    }

    final boolean value() {
        return value;
    }

    @Override
    public final byte[] toByteArray() {
        return builder().writeByte(value ? 1 : 0).build();
    }
}

abstract class StringServerMessage extends ServerMessageBase {
    private final String value;

    StringServerMessage(MessageCode.Server code, String value) {
        super(code);
        this.value = value;
    }

    final String value() {
        return value;
    }

    @Override
    public final byte[] toByteArray() {
        return builder().writeString(value).build();
    }
}
