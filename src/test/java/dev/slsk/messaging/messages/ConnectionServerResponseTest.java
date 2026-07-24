// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.messaging.messages;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.events.UserCannotConnectEvent;
import dev.slsk.exceptions.MessageException;
import dev.slsk.exceptions.MessageReadException;
import dev.slsk.messaging.MessageBuilder;
import dev.slsk.messaging.MessageCode;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ConnectionServerResponseTest {
    @Test
    @DisplayName("Login constructor applies optional defaults")
    void loginConstructorDefaults() throws Exception {
        InetAddress address = InetAddress.getByName("127.1.2.3");
        LoginResponse response = new LoginResponse(true, "ok", address);

        assertTrue(response.isSucceeded());
        assertEquals("ok", response.getMessage());
        assertSame(address, response.getIpAddress());
        assertNull(response.getHash());
        assertEquals(false, response.isSupporter());
    }

    @Test
    @DisplayName("Login parses failure without success-only fields")
    void loginFailureParses() {
        LoginResponse response = LoginResponse.fromByteArray(new MessageBuilder()
                .writeCode(MessageCode.Server.LOGIN)
                .writeByte(0)
                .writeString("bad password")
                .build());

        assertEquals(false, response.isSucceeded());
        assertEquals("bad password", response.getMessage());
        assertNull(response.getIpAddress());
        assertNull(response.getHash());
        assertEquals(false, response.isSupporter());
    }

    @Test
    @DisplayName("Login success parses address, hash, and exact flag byte")
    void loginSuccessParses() throws Exception {
        LoginResponse response = LoginResponse.fromByteArray(loginFrame(1));

        assertTrue(response.isSucceeded());
        assertEquals("", response.getMessage());
        assertEquals(InetAddress.getByName("127.1.2.3"), response.getIpAddress());
        assertEquals("hash", response.getHash());
        assertTrue(response.isSupporter());
        assertEquals(false, LoginResponse.fromByteArray(loginFrame(255)).isSupporter());
    }

    @Test
    @DisplayName("Connect-to-peer constructors and parser retain fields")
    void connectToPeerParses() throws Exception {
        InetAddress address = InetAddress.getByName("127.1.2.3");
        InetSocketAddress endpoint = new InetSocketAddress(address, 2234);
        ConnectToPeerResponse direct = new ConnectToPeerResponse("alice", "D", endpoint, -42, true);
        assertSame(endpoint, direct.getIpEndpoint());

        ConnectToPeerResponse parsed = ConnectToPeerResponse.fromByteArray(new MessageBuilder()
                .writeCode(MessageCode.Server.CONNECT_TO_PEER)
                .writeString("alice")
                .writeString("D")
                .writeBytes(new byte[] {3, 2, 1, 127})
                .writeInteger(2234)
                .writeInteger(-42)
                .writeByte(255)
                .build());
        assertEquals("alice", parsed.getUsername());
        assertEquals("D", parsed.getType());
        assertEquals(address, parsed.getIpAddress());
        assertEquals(2234, parsed.getPort());
        assertEquals(-42, parsed.getToken());
        assertTrue(parsed.isPrivileged());
    }

    @Test
    @DisplayName("Net info snapshots parent tuples and parses addresses")
    void netInfoParses() throws Exception {
        NetInfoParent first = new NetInfoParent("alice", InetAddress.getByName("127.1.2.3"), 2234);
        List<NetInfoParent> source = new ArrayList<>(List.of(first));
        NetInfoNotification direct = new NetInfoNotification(7, source);
        source.clear();
        assertEquals(7, direct.getParentCount());
        assertEquals(List.of(first), direct.getParents());
        assertThrows(
                UnsupportedOperationException.class, () -> direct.getParents().clear());

        NetInfoNotification parsed = NetInfoNotification.fromByteArray(new MessageBuilder()
                .writeCode(MessageCode.Server.NET_INFO)
                .writeInteger(1)
                .writeString("alice")
                .writeBytes(new byte[] {3, 2, 1, 127})
                .writeInteger(2234)
                .build());
        assertEquals(1, parsed.getParentCount());
        assertEquals(first, parsed.getParents().get(0));
    }

    @Test
    @DisplayName("Cannot-connect supports optional username and round trips")
    void cannotConnectRoundTrips() {
        CannotConnect withName = new CannotConnect(-42, "alice");
        CannotConnect tokenOnly = new CannotConnect(-42);

        assertEquals(-42, withName.getToken());
        assertEquals("alice", withName.getUsername());
        assertEquals(
                "alice", CannotConnect.fromByteArray(withName.toByteArray()).getUsername());
        assertNull(CannotConnect.fromByteArray(tokenOnly.toByteArray()).getUsername());
        assertArrayEquals(tokenOnly.toByteArray(), new CannotConnect(-42, "").toByteArray());

        UserCannotConnectEvent eventData = new UserCannotConnectEvent(withName);
        assertEquals(-42, eventData.getToken());
        assertEquals("alice", eventData.getUsername());
    }

    @Test
    @DisplayName("Privilege notification retains and parses fields")
    void privilegeNotificationParses() {
        PrivilegeNotification direct = new PrivilegeNotification(-42, "alice");
        assertEquals(-42, direct.getId());
        assertEquals("alice", direct.getUsername());

        PrivilegeNotification parsed = PrivilegeNotification.fromByteArray(new MessageBuilder()
                .writeCode(MessageCode.Server.NOTIFY_PRIVILEGES)
                .writeInteger(-42)
                .writeString("alice")
                .build());
        assertEquals(-42, parsed.getId());
        assertEquals("alice", parsed.getUsername());
    }

    @Test
    @DisplayName("SocketConnection responses reject mismatches and missing data")
    void responsesRejectInvalidFrames() {
        byte[] mismatch =
                new MessageBuilder().writeCode(MessageCode.Peer.BROWSE_REQUEST).build();

        assertThrows(MessageException.class, () -> LoginResponse.fromByteArray(mismatch));
        assertThrows(MessageException.class, () -> ConnectToPeerResponse.fromByteArray(mismatch));
        assertThrows(MessageException.class, () -> NetInfoNotification.fromByteArray(mismatch));
        assertThrows(MessageException.class, () -> CannotConnect.fromByteArray(mismatch));
        assertThrows(MessageException.class, () -> PrivilegeNotification.fromByteArray(mismatch));

        assertThrows(
                MessageReadException.class,
                () -> LoginResponse.fromByteArray(
                        new MessageBuilder().writeCode(MessageCode.Server.LOGIN).build()));
        assertThrows(
                MessageReadException.class,
                () -> ConnectToPeerResponse.fromByteArray(new MessageBuilder()
                        .writeCode(MessageCode.Server.CONNECT_TO_PEER)
                        .build()));
        assertThrows(
                MessageReadException.class,
                () -> NetInfoNotification.fromByteArray(new MessageBuilder()
                        .writeCode(MessageCode.Server.NET_INFO)
                        .build()));
        assertThrows(
                MessageReadException.class,
                () -> PrivilegeNotification.fromByteArray(new MessageBuilder()
                        .writeCode(MessageCode.Server.NOTIFY_PRIVILEGES)
                        .build()));
    }

    private static byte[] loginFrame(int supporterByte) {
        return new MessageBuilder()
                .writeCode(MessageCode.Server.LOGIN)
                .writeByte(1)
                .writeString("")
                .writeBytes(new byte[] {3, 2, 1, 127})
                .writeString("hash")
                .writeByte(supporterByte)
                .build();
    }
}
