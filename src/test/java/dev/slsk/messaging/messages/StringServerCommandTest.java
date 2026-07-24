// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.MessageReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class StringServerCommandTest {
    @Test
    @DisplayName("Branch and room string commands preserve data")
    void branchAndRoomCommandsPreserveData() {
        BranchRootCommand branch = new BranchRootCommand("branch");
        LeaveRoomRequest leave = new LeaveRoomRequest("room");
        PrivateRoomDropMembershipCommand membership = new PrivateRoomDropMembershipCommand("members");
        PrivateRoomDropOwnershipCommand ownership = new PrivateRoomDropOwnershipCommand("owned");

        assertEquals("branch", branch.getUsername());
        assertEquals("room", leave.getRoomName());
        assertEquals("members", membership.getRoomName());
        assertEquals("owned", ownership.getRoomName());
        assertString(branch, MessageCode.Server.BRANCH_ROOT, "branch");
        assertString(leave, MessageCode.Server.LEAVE_ROOM, "room");
        assertString(membership, MessageCode.Server.PRIVATE_ROOM_DROP_MEMBERSHIP, "members");
        assertString(ownership, MessageCode.Server.PRIVATE_ROOM_DROP_OWNERSHIP, "owned");
    }

    @Test
    @DisplayName("User string requests preserve data")
    void userRequestsPreserveData() {
        UnwatchUserCommand unwatch = new UnwatchUserCommand("one");
        UserAddressRequest address = new UserAddressRequest("two");
        UserPrivilegesRequest privileges = new UserPrivilegesRequest("three");
        UserStatisticsRequest statistics = new UserStatisticsRequest("four");
        UserStatusRequest status = new UserStatusRequest("five");
        WatchUserRequest watch = new WatchUserRequest("six");

        assertEquals("one", unwatch.getUsername());
        assertEquals("two", address.getUsername());
        assertEquals("three", privileges.getUsername());
        assertEquals("four", statistics.getUsername());
        assertEquals("five", status.getUsername());
        assertEquals("six", watch.getUsername());
        assertString(unwatch, MessageCode.Server.UNWATCH_USER, "one");
        assertString(address, MessageCode.Server.GET_PEER_ADDRESS, "two");
        assertString(privileges, MessageCode.Server.USER_PRIVILEGES, "three");
        assertString(statistics, MessageCode.Server.GET_USER_STATS, "four");
        assertString(status, MessageCode.Server.GET_STATUS, "five");
        assertString(watch, MessageCode.Server.WATCH_USER, "six");
    }

    @Test
    @DisplayName("String commands preserve UTF-8 wire encoding")
    void stringCommandsPreserveUtf8() {
        BranchRootCommand command = new BranchRootCommand("á");
        byte[] bytes = command.toByteArray();
        MessageReader<MessageCode.Server> reader = new MessageReader<>(bytes, MessageCode.Server.class);

        assertEquals(14, bytes.length);
        assertEquals(MessageCode.Server.BRANCH_ROOT, reader.readCode());
        assertEquals("á", reader.readString());
        assertEquals(0, reader.getRemaining());
    }

    private static void assertString(IOutgoingMessage message, MessageCode.Server code, String value) {
        MessageReader<MessageCode.Server> reader = new MessageReader<>(message.toByteArray(), MessageCode.Server.class);

        assertEquals(code, reader.readCode());
        assertEquals(value, reader.readString());
        assertEquals(0, reader.getRemaining());
    }
}
