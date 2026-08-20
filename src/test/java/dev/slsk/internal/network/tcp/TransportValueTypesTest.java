// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TransportValueTypesTest {
    @Test
    @DisplayName("SocketConnection type is one classified category")
    void connectionTypeIsOneCategory() {
        assertSame(ConnectionType.OUTBOUND_DIRECT, ConnectionType.valueOf("OUTBOUND_DIRECT"));
        assertFalse(ConnectionType.OUTBOUND_DIRECT == ConnectionType.INBOUND_DIRECT);
    }

    @Test
    @DisplayName("SocketConnection key retains endpoint-only data")
    void endpointKeyRetainsData() throws Exception {
        InetSocketAddress endpoint = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 2234);
        ConnectionKey key = new ConnectionKey(endpoint);

        assertNull(key.getUsername());
        assertSame(endpoint, key.getIpEndpoint());
    }

    @Test
    @DisplayName("SocketConnection key retains username and endpoint")
    void fullKeyRetainsData() throws Exception {
        InetSocketAddress endpoint = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 2234);
        ConnectionKey key = new ConnectionKey("alice", endpoint);

        assertEquals("alice", key.getUsername());
        assertSame(endpoint, key.getIpEndpoint());
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
        ConnectionDataEvent args = new ConnectionDataEvent(3, 20);

        assertEquals(3, args.currentLength());
        assertEquals(20, args.totalLength());
        assertEquals(15.0, args.percentComplete());
        assertTrue(Double.isNaN(new ConnectionDataEvent(0, 0).percentComplete()));
        assertEquals(Double.POSITIVE_INFINITY, new ConnectionDataEvent(1, 0).percentComplete());
    }

    @Test
    @DisplayName("Disconnected event preserves message and exception")
    void disconnectedEventRetainsData() {
        IllegalStateException exception = new IllegalStateException("broken");
        ConnectionDisconnectedEvent args = new ConnectionDisconnectedEvent("closed", exception);

        assertEquals("closed", args.message());
        assertSame(exception, args.exception());
        assertNull(new ConnectionDisconnectedEvent("closed").exception());
    }

    @Test
    @DisplayName("State-changed event applies optional defaults")
    void stateChangedEventRetainsData() {
        IllegalStateException exception = new IllegalStateException("broken");
        ConnectionStateChangedEvent complete = new ConnectionStateChangedEvent(
                TransportState.CONNECTED, TransportState.DISCONNECTED, "closed", exception);
        ConnectionStateChangedEvent minimal =
                new ConnectionStateChangedEvent(TransportState.CONNECTED, TransportState.DISCONNECTED);

        assertEquals(TransportState.CONNECTED, complete.previousState());
        assertEquals(TransportState.DISCONNECTED, complete.currentState());
        assertEquals("closed", complete.message());
        assertSame(exception, complete.exception());
        assertNull(minimal.message());
        assertNull(minimal.exception());
    }
}
