// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.exceptions.MessageException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.room.RoomInfo;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SimpleServerNotificationTest {
    static java.util.stream.Stream<Arguments> stringNotifications() {
        return java.util.stream.Stream.of(
                Arguments.of(MessageCode.Server.GLOBAL_ADMIN_MESSAGE, (Function<byte[], String>)
                        GlobalMessageNotification::fromByteArray),
                Arguments.of(MessageCode.Server.ADD_PRIVILEGED_USER, (Function<byte[], String>)
                        PrivilegedUserNotification::fromByteArray));
    }

    @ParameterizedTest(name = "{0} parses one string")
    @MethodSource("stringNotifications")
    void parsesStringNotification(MessageCode.Server code, Function<byte[], String> parser) {
        byte[] message =
                new MessageBuilder().writeCode(code).writeString("üser message").build();

        assertEquals("üser message", parser.apply(message));
        assertThrows(
                MessageReadException.class,
                () -> parser.apply(new MessageBuilder().writeCode(code).build()));
        assertThrows(
                MessageException.class,
                () -> parser.apply(new MessageBuilder()
                        .writeCode(MessageCode.Peer.BROWSE_REQUEST)
                        .build()));
    }

    @Test
    @DisplayName("Excluded phrases preserve order and are immutable")
    void parsesExcludedPhrases() {
        byte[] message = stringList(MessageCode.Server.EXCLUDED_SEARCH_PHRASES, "larry", "moe", "curly", "shemp");

        List<String> result = ExcludedSearchPhrasesNotification.fromByteArray(message);
        assertEquals(List.of("larry", "moe", "curly", "shemp"), result);
        assertThrows(UnsupportedOperationException.class, () -> result.add("joe"));
    }

    @Test
    @DisplayName("Privileged users preserve order and are immutable")
    void parsesPrivilegedUsers() {
        byte[] message = stringList(MessageCode.Server.PRIVILEGED_USERS, "larry", "moe", "curly", "shemp");

        List<String> result = PrivilegedUserListNotification.fromByteArray(message);
        assertEquals(List.of("larry", "moe", "curly", "shemp"), result);
        assertThrows(UnsupportedOperationException.class, () -> result.add("joe"));
    }

    @Test
    @DisplayName("String lists reject mismatch and missing counts")
    void stringListsRejectInvalidFrames() {
        byte[] mismatch =
                new MessageBuilder().writeCode(MessageCode.Peer.BROWSE_REQUEST).build();

        assertThrows(MessageException.class, () -> ExcludedSearchPhrasesNotification.fromByteArray(mismatch));
        assertThrows(
                MessageReadException.class,
                () -> PrivilegedUserListNotification.fromByteArray(new MessageBuilder()
                        .writeCode(MessageCode.Server.PRIVILEGED_USERS)
                        .build()));
    }

    @Test
    @DisplayName("Private owned-room list parses room and users")
    void parsesOwnedRoom() {
        RoomInfo room = PrivateRoomOwnedListNotification.fromByteArray(
                roomList(MessageCode.Server.PRIVATE_ROOM_OWNED, "secret", "alice", "bob"));

        assertEquals("secret", room.name());
        assertEquals(2, room.userCount());
        assertEquals(List.of("alice", "bob"), room.users());
    }

    @Test
    @DisplayName("Private user list parses room and users")
    void parsesPrivateRoomUsers() {
        RoomInfo room = PrivateRoomUserListNotification.fromByteArray(
                roomList(MessageCode.Server.PRIVATE_ROOM_USERS, "secret", "alice", "bob"));

        assertEquals("secret", room.name());
        assertEquals(2, room.userCount());
        assertEquals(List.of("alice", "bob"), room.users());
    }

    @Test
    @DisplayName("Private room lists reject mismatch and missing data")
    void privateRoomListsRejectInvalidFrames() {
        byte[] mismatch =
                new MessageBuilder().writeCode(MessageCode.Peer.BROWSE_REQUEST).build();
        byte[] missing = new MessageBuilder()
                .writeCode(MessageCode.Server.PRIVATE_ROOM_USERS)
                .build();

        assertThrows(MessageException.class, () -> PrivateRoomOwnedListNotification.fromByteArray(mismatch));
        assertThrows(MessageReadException.class, () -> PrivateRoomUserListNotification.fromByteArray(missing));
    }

    private static byte[] stringList(MessageCode.Server code, String... values) {
        MessageBuilder builder = new MessageBuilder().writeCode(code).writeInteger(values.length);
        for (String value : values) {
            builder.writeString(value);
        }
        return builder.build();
    }

    private static byte[] roomList(MessageCode.Server code, String room, String... users) {
        MessageBuilder builder =
                new MessageBuilder().writeCode(code).writeString(room).writeInteger(users.length);
        for (String user : users) {
            builder.writeString(user);
        }
        return builder.build();
    }
}
