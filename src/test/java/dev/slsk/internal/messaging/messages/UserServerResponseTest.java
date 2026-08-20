// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.messaging.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.exceptions.MessageException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.internal.messaging.MessageBuilder;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.user.UserData;
import dev.slsk.internal.user.UserPresence;
import dev.slsk.internal.user.UserStatistics;
import dev.slsk.internal.user.UserStatus;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class UserServerResponseTest {
    @Test
    @DisplayName("User address constructors retain endpoint data")
    void addressConstructorsRetainData() throws Exception {
        InetAddress address = InetAddress.getByName("127.1.2.3");
        InetSocketAddress endpoint = new InetSocketAddress(address, 2234);
        UserAddressResponse fromEndpoint = new UserAddressResponse("alice", endpoint);
        UserAddressResponse fromParts = new UserAddressResponse("alice", address, 2234);

        assertSame(endpoint, fromEndpoint.getIpEndpoint());
        assertEquals(address, fromEndpoint.getIpAddress());
        assertEquals(2234, fromEndpoint.getPort());
        assertEquals("alice", fromParts.getUsername());
        assertEquals(endpoint, fromParts.getIpEndpoint());
    }

    @Test
    @DisplayName("User address parses reversed IPv4 bytes")
    void addressParses() throws Exception {
        byte[] frame = new MessageBuilder()
                .writeCode(MessageCode.Server.GET_PEER_ADDRESS)
                .writeString("alice")
                .writeBytes(new byte[] {3, 2, 1, 127})
                .writeInteger(2234)
                .build();

        UserAddressResponse response = UserAddressResponse.fromByteArray(frame);
        assertEquals("alice", response.getUsername());
        assertEquals(InetAddress.getByName("127.1.2.3"), response.getIpAddress());
        assertEquals(2234, response.getPort());
    }

    @Test
    @DisplayName("Privilege response parses every positive byte as true")
    void privilegeParses() {
        for (int value : new int[] {0, 1, 255}) {
            UserPrivilegeResponse response = UserPrivilegeResponse.fromByteArray(new MessageBuilder()
                    .writeCode(MessageCode.Server.USER_PRIVILEGES)
                    .writeString("alice")
                    .writeByte(value)
                    .build());
            assertEquals("alice", response.getUsername());
            assertEquals(value > 0, response.isPrivileged());
        }
    }

    @Test
    @DisplayName("Statistics response parses all scalar widths")
    void statisticsParse() {
        UserStatistics statistics = UserStatisticsResponseFactory.fromByteArray(new MessageBuilder()
                .writeCode(MessageCode.Server.GET_USER_STATS)
                .writeString("alice")
                .writeInteger(-12)
                .writeLong(9_876_543_210L)
                .writeInteger(34)
                .writeInteger(56)
                .build());

        assertEquals("alice", statistics.username());
        assertEquals(-12, statistics.averageSpeed());
        assertEquals(9_876_543_210L, statistics.uploadCount());
        assertEquals(34, statistics.fileCount());
        assertEquals(56, statistics.directoryCount());
    }

    @Test
    @DisplayName("Status response parses presence and privilege")
    void statusParses() {
        UserStatus status = UserStatusResponseFactory.fromByteArray(new MessageBuilder()
                .writeCode(MessageCode.Server.GET_STATUS)
                .writeString("alice")
                .writeInteger(UserPresence.AWAY.getValue())
                .writeByte(255)
                .build());

        assertEquals("alice", status.username());
        assertEquals(UserPresence.AWAY, status.presence());
        assertTrue(status.privileged());
    }

    @Test
    @DisplayName("Watch response retains direct constructor data")
    void watchConstructorRetainsData() {
        UserData data = new UserData("alice", UserPresence.ONLINE, 12, 34, 56, 78, "CL");
        WatchUserResponse response = new WatchUserResponse("alice", true, data);

        assertEquals("alice", response.getUsername());
        assertTrue(response.isExists());
        assertSame(data, response.getUserData());
    }

    @Test
    @DisplayName("Watch response parses existing user with country")
    void watchExistingUserParses() {
        WatchUserResponse response = WatchUserResponse.fromByteArray(watchFrame(true, "CL"));

        assertTrue(response.isExists());
        UserData data = response.getUserData();
        assertEquals("alice", data.username());
        assertEquals(UserPresence.ONLINE, data.status());
        assertEquals(12, data.averageSpeed());
        assertEquals(9_876_543_210L, data.uploadCount());
        assertEquals(34, data.fileCount());
        assertEquals(56, data.directoryCount());
        assertEquals("CL", data.countryCode());
    }

    @Test
    @DisplayName("Watch response accepts blank and missing country")
    void watchCountryIsOptional() {
        assertEquals(
                "",
                WatchUserResponse.fromByteArray(watchFrame(true, ""))
                        .getUserData()
                        .countryCode());

        byte[] missingCountry = new MessageBuilder()
                .writeCode(MessageCode.Server.WATCH_USER)
                .writeString("alice")
                .writeByte(1)
                .writeInteger(UserPresence.ONLINE.getValue())
                .writeInteger(12)
                .writeLong(9_876_543_210L)
                .writeInteger(34)
                .writeInteger(56)
                .build();
        assertNull(WatchUserResponse.fromByteArray(missingCountry).getUserData().countryCode());
    }

    @Test
    @DisplayName("Watch response skips absent-user detail fields")
    void watchAbsentUserParses() {
        WatchUserResponse response = WatchUserResponse.fromByteArray(watchFrame(false, null));

        assertEquals("alice", response.getUsername());
        assertEquals(false, response.isExists());
        assertNull(response.getUserData());
    }

    @Test
    @DisplayName("User responses reject mismatches and missing data")
    void responsesRejectInvalidFrames() {
        byte[] mismatch =
                new MessageBuilder().writeCode(MessageCode.Peer.BROWSE_REQUEST).build();

        assertThrows(MessageException.class, () -> UserAddressResponse.fromByteArray(mismatch));
        assertThrows(MessageException.class, () -> UserPrivilegeResponse.fromByteArray(mismatch));
        assertThrows(MessageException.class, () -> UserStatisticsResponseFactory.fromByteArray(mismatch));
        assertThrows(MessageException.class, () -> UserStatusResponseFactory.fromByteArray(mismatch));
        assertThrows(MessageException.class, () -> WatchUserResponse.fromByteArray(mismatch));

        assertThrows(
                MessageReadException.class,
                () -> UserAddressResponse.fromByteArray(new MessageBuilder()
                        .writeCode(MessageCode.Server.GET_PEER_ADDRESS)
                        .build()));
        assertThrows(
                MessageReadException.class,
                () -> WatchUserResponse.fromByteArray(new MessageBuilder()
                        .writeCode(MessageCode.Server.WATCH_USER)
                        .build()));
    }

    private static byte[] watchFrame(boolean exists, String countryCode) {
        MessageBuilder builder = new MessageBuilder()
                .writeCode(MessageCode.Server.WATCH_USER)
                .writeString("alice")
                .writeByte(exists ? 1 : 0);
        if (!exists) {
            return builder.build();
        }
        builder.writeInteger(UserPresence.ONLINE.getValue())
                .writeInteger(12)
                .writeLong(9_876_543_210L)
                .writeInteger(34)
                .writeInteger(56);
        if (countryCode != null) {
            builder.writeString(countryCode);
        }
        return builder.build();
    }
}
