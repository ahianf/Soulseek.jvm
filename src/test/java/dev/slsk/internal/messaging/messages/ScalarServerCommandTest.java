// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.MessageReader;
import dev.slsk.internal.user.WireUserPresence;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ScalarServerCommandTest {
    @Test
    @DisplayName("Empty server commands preserve their exact codes")
    void emptyCommandsPreserveCodes() {
        assertEmpty(new CheckPrivilegesRequest(), MessageCode.Server.CHECK_PRIVILEGES);
        assertEmpty(new RoomListRequest(), MessageCode.Server.ROOM_LIST);
        assertEmpty(new StartPublicChatCommand(), MessageCode.Server.ASK_PUBLIC_CHAT);
        assertEmpty(new StopPublicChatCommand(), MessageCode.Server.STOP_PUBLIC_CHAT);
    }

    @Test
    @DisplayName("Boolean server commands preserve flags and codes")
    void booleanCommandsPreserveFlagsAndCodes() {
        AcceptChildrenCommand accepted = new AcceptChildrenCommand(true);
        AcceptChildrenCommand rejected = new AcceptChildrenCommand(false);
        HaveNoParentsCommand noParents = new HaveNoParentsCommand(true);
        HaveNoParentsCommand hasParent = new HaveNoParentsCommand(false);

        assertEquals(true, accepted.isAccepted());
        assertEquals(false, rejected.isAccepted());
        assertEquals(true, noParents.hasNoParents());
        assertEquals(false, hasParent.hasNoParents());
        assertByte(accepted, MessageCode.Server.ACCEPT_CHILDREN, 1);
        assertByte(rejected, MessageCode.Server.ACCEPT_CHILDREN, 0);
        assertByte(noParents, MessageCode.Server.HAVE_NO_PARENTS, 1);
        assertByte(hasParent, MessageCode.Server.HAVE_NO_PARENTS, 0);
    }

    @Test
    @DisplayName("Acknowledgement commands preserve identifiers")
    void acknowledgementCommandsPreserveIdentifiers() {
        AcknowledgePrivateMessageCommand privateMessage = new AcknowledgePrivateMessageCommand(0x12345678);
        AcknowledgePrivilegeNotificationCommand privilege = new AcknowledgePrivilegeNotificationCommand(-17);

        assertEquals(0x12345678, privateMessage.getId());
        assertEquals(-17, privilege.getId());
        assertInteger(privateMessage, MessageCode.Server.ACKNOWLEDGE_PRIVATE_MESSAGE, 0x12345678);
        assertInteger(privilege, MessageCode.Server.ACKNOWLEDGE_NOTIFY_PRIVILEGES, -17);
    }

    @Test
    @DisplayName("Distributed scalar commands preserve values and codes")
    void distributedCommandsPreserveValues() {
        BranchLevelCommand level = new BranchLevelCommand(-1);
        ChildDepthCommand depth = new ChildDepthCommand(17);

        assertEquals(-1, level.getLevel());
        assertEquals(17, depth.getDepth());
        assertInteger(level, MessageCode.Server.BRANCH_LEVEL, -1);
        assertInteger(depth, MessageCode.Server.CHILD_DEPTH, 17);
    }

    @Test
    @DisplayName("Upload speed command preserves its value and code")
    void uploadSpeedCommandPreservesData() {
        SendUploadSpeedCommand command = new SendUploadSpeedCommand(-1);

        assertEquals(-1, command.getSpeed());
        assertInteger(command, MessageCode.Server.SEND_UPLOAD_SPEED, -1);
    }

    @Test
    @DisplayName("Listen port command preserves boundaries and wire data")
    void listenPortCommandPreservesValidationAndData() {
        SetListenPortCommand minimum = new SetListenPortCommand(1024);
        SetListenPortCommand maximum = new SetListenPortCommand(65535);

        assertEquals(1024, minimum.getPort());
        assertEquals(65535, maximum.getPort());
        assertInteger(minimum, MessageCode.Server.SET_LISTEN_PORT, 1024);
        assertInteger(maximum, MessageCode.Server.SET_LISTEN_PORT, 65535);
        assertThrows(IllegalArgumentException.class, () -> new SetListenPortCommand(1023));
        assertThrows(IllegalArgumentException.class, () -> new SetListenPortCommand(65536));
    }

    @Test
    @DisplayName("Online status command preserves enum and wire value")
    void onlineStatusCommandPreservesData() {
        SetOnlineStatusCommand command = new SetOnlineStatusCommand(WireUserPresence.AWAY);

        assertEquals(WireUserPresence.AWAY, command.getStatus());
        assertInteger(command, MessageCode.Server.SET_ONLINE_STATUS, WireUserPresence.AWAY.getValue());
        assertThrows(NullPointerException.class, () -> new SetOnlineStatusCommand(null));
    }

    private static void assertEmpty(OutgoingMessage message, MessageCode.Server code) {
        byte[] bytes = message.toByteArray();
        MessageReader<MessageCode.Server> reader = new MessageReader<>(bytes, MessageCode.Server.class);

        assertEquals(8, bytes.length);
        assertEquals(code, reader.readCode());
        assertEquals(0, reader.getRemaining());
    }

    private static void assertByte(OutgoingMessage message, MessageCode.Server code, int value) {
        byte[] bytes = message.toByteArray();
        MessageReader<MessageCode.Server> reader = new MessageReader<>(bytes, MessageCode.Server.class);

        assertEquals(9, bytes.length);
        assertEquals(code, reader.readCode());
        assertEquals(value, reader.readByte());
        assertEquals(0, reader.getRemaining());
    }

    private static void assertInteger(OutgoingMessage message, MessageCode.Server code, int value) {
        byte[] bytes = message.toByteArray();
        MessageReader<MessageCode.Server> reader = new MessageReader<>(bytes, MessageCode.Server.class);

        assertEquals(12, bytes.length);
        assertEquals(code, reader.readCode());
        assertEquals(value, reader.readInteger());
        assertEquals(0, reader.getRemaining());
    }
}
