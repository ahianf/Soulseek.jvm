// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.exceptions.MessageException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class PrivateRoomCommandTest {
    static java.util.stream.Stream<Arguments> userMessages() {
        return java.util.stream.Stream.of(
                Arguments.of(
                        MessageCode.Server.PRIVATE_ROOM_ADD_OPERATOR,
                        (BiFunction<String, String, PrivateRoomUserMessage>) PrivateRoomAddOperator::new,
                        (Function<byte[], PrivateRoomUserMessage>) PrivateRoomAddOperator::fromByteArray),
                Arguments.of(
                        MessageCode.Server.PRIVATE_ROOM_ADD_USER,
                        (BiFunction<String, String, PrivateRoomUserMessage>) PrivateRoomAddUser::new,
                        (Function<byte[], PrivateRoomUserMessage>) PrivateRoomAddUser::fromByteArray),
                Arguments.of(
                        MessageCode.Server.PRIVATE_ROOM_REMOVE_OPERATOR,
                        (BiFunction<String, String, PrivateRoomUserMessage>) PrivateRoomRemoveOperator::new,
                        (Function<byte[], PrivateRoomUserMessage>) PrivateRoomRemoveOperator::fromByteArray),
                Arguments.of(
                        MessageCode.Server.PRIVATE_ROOM_REMOVE_USER,
                        (BiFunction<String, String, PrivateRoomUserMessage>) PrivateRoomRemoveUser::new,
                        (Function<byte[], PrivateRoomUserMessage>) PrivateRoomRemoveUser::fromByteArray));
    }

    @ParameterizedTest(name = "{0} retains data and round trips")
    @MethodSource("userMessages")
    void userMessageRoundTrips(
            MessageCode.Server code,
            BiFunction<String, String, PrivateRoomUserMessage> constructor,
            Function<byte[], PrivateRoomUserMessage> parser) {
        PrivateRoomUserMessage message = constructor.apply("room", "üser");
        MessageReader<MessageCode.Server> reader = new MessageReader<>(message.toByteArray(), MessageCode.Server.class);

        assertEquals("room", message.getRoomName());
        assertEquals("üser", message.getUsername());
        assertEquals(code, reader.readCode());
        assertEquals("room", reader.readString());
        assertEquals("üser", reader.readString());

        PrivateRoomUserMessage parsed = parser.apply(message.toByteArray());
        assertEquals("room", parsed.getRoomName());
        assertEquals("üser", parsed.getUsername());
    }

    @ParameterizedTest(name = "{0} rejects mismatched and missing data")
    @MethodSource("userMessages")
    void userMessageRejectsInvalidFrames(
            MessageCode.Server code,
            BiFunction<String, String, PrivateRoomUserMessage> constructor,
            Function<byte[], PrivateRoomUserMessage> parser) {
        byte[] mismatch =
                new MessageBuilder().writeCode(MessageCode.Peer.BROWSE_REQUEST).build();
        byte[] missing = new MessageBuilder().writeCode(code).build();

        assertThrows(MessageException.class, () -> parser.apply(mismatch));
        assertThrows(MessageReadException.class, () -> parser.apply(missing));
    }

    @Test
    @DisplayName("Private-room toggle retains data and writes boolean byte")
    void toggleRoundTrips() {
        for (boolean accept : new boolean[] {false, true}) {
            PrivateRoomToggle message = new PrivateRoomToggle(accept);
            MessageReader<MessageCode.Server> reader =
                    new MessageReader<>(message.toByteArray(), MessageCode.Server.class);

            assertEquals(accept, message.isAcceptInvitations());
            assertEquals(MessageCode.Server.PRIVATE_ROOM_TOGGLE, reader.readCode());
            assertEquals(accept ? 1 : 0, reader.readByte());
            assertEquals(
                    accept,
                    PrivateRoomToggle.fromByteArray(message.toByteArray()).isAcceptInvitations());
        }
    }

    @Test
    @DisplayName("Private-room toggle treats every positive byte as true")
    void toggleParsesPositiveByte() {
        byte[] message = new MessageBuilder()
                .writeCode(MessageCode.Server.PRIVATE_ROOM_TOGGLE)
                .writeByte(255)
                .build();

        assertEquals(true, PrivateRoomToggle.fromByteArray(message).isAcceptInvitations());
    }

    @Test
    @DisplayName("Private-room toggle rejects mismatch and missing data")
    void toggleRejectsInvalidFrames() {
        byte[] mismatch =
                new MessageBuilder().writeCode(MessageCode.Peer.BROWSE_REQUEST).build();
        byte[] missing = new MessageBuilder()
                .writeCode(MessageCode.Server.PRIVATE_ROOM_TOGGLE)
                .build();

        assertThrows(MessageException.class, () -> PrivateRoomToggle.fromByteArray(mismatch));
        assertThrows(MessageReadException.class, () -> PrivateRoomToggle.fromByteArray(missing));
    }
}
