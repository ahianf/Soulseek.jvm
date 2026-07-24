// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network.tcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ConnectionValueTypesTest {
    @Test
    @DisplayName("SocketConnection state preserves every numeric value")
    void stateValuesMatchSource() {
        assertEquals(0, ConnectionState.PENDING.getValue());
        assertEquals(1, ConnectionState.CONNECTING.getValue());
        assertEquals(2, ConnectionState.CONNECTED.getValue());
        assertEquals(3, ConnectionState.DISCONNECTING.getValue());
        assertEquals(4, ConnectionState.DISCONNECTED.getValue());
        assertEquals(ConnectionState.CONNECTED, ConnectionState.fromValue(2));
    }

    @Test
    @DisplayName("SocketConnection type flags combine and test bits")
    void connectionTypesPreserveFlags() {
        ConnectionTypes combined = ConnectionTypes.OUTBOUND.or(ConnectionTypes.DIRECT);

        assertEquals(5, combined.getValue());
        assertTrue(combined.hasFlag(ConnectionTypes.OUTBOUND));
        assertTrue(combined.hasFlag(ConnectionTypes.DIRECT));
        assertFalse(combined.hasFlag(ConnectionTypes.INBOUND));
        assertEquals(combined, ConnectionTypes.fromValue(5));
        assertEquals("OUTBOUND | DIRECT", combined.toString());
    }

    @Test
    @DisplayName("SocketConnection key retains endpoint-only data")
    void endpointKeyRetainsData() throws Exception {
        InetSocketAddress endpoint = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 2234);
        ConnectionKey key = new ConnectionKey(endpoint);

        assertNull(key.getUsername());
        assertSame(endpoint, key.getIpEndPoint());
    }

    @Test
    @DisplayName("SocketConnection key retains username and endpoint")
    void fullKeyRetainsData() throws Exception {
        InetSocketAddress endpoint = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 2234);
        ConnectionKey key = new ConnectionKey("alice", endpoint);

        assertEquals("alice", key.getUsername());
        assertSame(endpoint, key.getIpEndPoint());
    }

    @Test
    @DisplayName("SocketConnection key equality follows source hash equality")
    void keyEqualityCoversNullableComponents() throws Exception {
        InetSocketAddress endpoint = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 2234);
        ConnectionKey first = new ConnectionKey("alice", endpoint);
        ConnectionKey equal =
                new ConnectionKey("alice", new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 2234));

        assertEquals(first.hashCode(), equal.hashCode());
        assertEquals(first, equal);
        assertFalse(first.equals(new ConnectionKey("bob", endpoint)));
        assertFalse(first.equals(
                new ConnectionKey("alice", new InetSocketAddress(InetAddress.getByName("127.0.0.2"), 2234))));
        assertFalse(first.equals(
                new ConnectionKey("alice", new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 2235))));
        assertEquals(new ConnectionKey("alice", null), new ConnectionKey("alice", null));
        assertEquals(new ConnectionKey(null, endpoint), new ConnectionKey(null, endpoint));
        assertFalse(first.equals(null));
        assertFalse(first.equals("alice"));
    }

    @Test
    @DisplayName("SocketConnection data event computes percentage")
    void dataEventComputesPercentage() {
        ConnectionDataEventArgs args = new ConnectionDataEventArgs(3, 20);

        assertEquals(3, args.getCurrentLength());
        assertEquals(20, args.getTotalLength());
        assertEquals(15.0, args.getPercentComplete());
        assertTrue(Double.isNaN(new ConnectionDataEventArgs(0, 0).getPercentComplete()));
        assertEquals(Double.POSITIVE_INFINITY, new ConnectionDataEventArgs(1, 0).getPercentComplete());
    }

    @Test
    @DisplayName("Disconnected event preserves message and exception")
    void disconnectedEventRetainsData() {
        IllegalStateException exception = new IllegalStateException("broken");
        ConnectionDisconnectedEventArgs args = new ConnectionDisconnectedEventArgs("closed", exception);

        assertEquals("closed", args.getMessage());
        assertSame(exception, args.getException());
        assertNull(new ConnectionDisconnectedEventArgs("closed").getException());
    }

    @Test
    @DisplayName("State-changed event applies optional defaults")
    void stateChangedEventRetainsData() {
        IllegalStateException exception = new IllegalStateException("broken");
        ConnectionStateChangedEventArgs complete = new ConnectionStateChangedEventArgs(
                ConnectionState.CONNECTED, ConnectionState.DISCONNECTED, "closed", exception);
        ConnectionStateChangedEventArgs minimal =
                new ConnectionStateChangedEventArgs(ConnectionState.CONNECTED, ConnectionState.DISCONNECTED);

        assertEquals(ConnectionState.CONNECTED, complete.getPreviousState());
        assertEquals(ConnectionState.DISCONNECTED, complete.getCurrentState());
        assertEquals("closed", complete.getMessage());
        assertSame(exception, complete.getException());
        assertNull(minimal.getMessage());
        assertNull(minimal.getException());
    }
}
