// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.options;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ConnectionOptionsTest {
    @Test
    @DisplayName("Instantiates properly")
    void instantiatesProperly() {
        ConnectionOptions options = new ConnectionOptions(-1, -2, -3, -4, -5);

        assertEquals(-1, options.getReadBufferSize());
        assertEquals(-2, options.getWriteBufferSize());
        assertEquals(-3, options.getWriteQueueSize());
        assertEquals(-4, options.getConnectTimeout());
        assertEquals(-5, options.getInactivityTimeout());
        assertNull(options.getProxyOptions());
        assertNotNull(options.getConfigureSocket());
    }

    @Test
    @DisplayName("Uses source defaults")
    void usesSourceDefaults() throws Exception {
        ConnectionOptions options = new ConnectionOptions();

        assertEquals(16_384, options.getReadBufferSize());
        assertEquals(16_384, options.getWriteBufferSize());
        assertEquals(250, options.getWriteQueueSize());
        assertEquals(10_000, options.getConnectTimeout());
        assertEquals(15_000, options.getInactivityTimeout());
        try (Socket socket = new Socket()) {
            options.getConfigureSocket().configure(socket);
        }
    }

    @Test
    @DisplayName("Optional overloads preserve trailing defaults")
    void optionalOverloadsPreserveTrailingDefaults() {
        ConnectionOptions one = new ConnectionOptions(1);
        ConnectionOptions two = new ConnectionOptions(1, 2);
        ConnectionOptions three = new ConnectionOptions(1, 2, 3);
        ConnectionOptions four = new ConnectionOptions(1, 2, 3, 4);

        assertEquals(16_384, one.getWriteBufferSize());
        assertEquals(250, two.getWriteQueueSize());
        assertEquals(10_000, three.getConnectTimeout());
        assertEquals(15_000, four.getInactivityTimeout());
    }

    @Test
    @DisplayName("Canonical constructor retains proxy and callback")
    void canonicalConstructorRetainsProxyAndCallback() throws Exception {
        ProxyOptions proxy = new ProxyOptions("127.0.0.1", 1);
        AtomicBoolean invoked = new AtomicBoolean();
        SocketConfigurator configurator = socket -> invoked.set(true);
        ConnectionOptions options = new ConnectionOptions(1, 2, 3, 4, 5, proxy, configurator);

        try (Socket socket = new Socket()) {
            options.getConfigureSocket().configure(socket);
        }

        assertSame(proxy, options.getProxyOptions());
        assertSame(configurator, options.getConfigureSocket());
        assertEquals(true, invoked.get());
    }

    @Test
    @DisplayName("WithoutInactivityTimeout forces InactivityTimeout to -1")
    void withoutInactivityTimeoutForcesNegativeOne() {
        ProxyOptions proxy = new ProxyOptions("127.0.0.1", 1);
        SocketConfigurator configurator = socket -> {};
        ConnectionOptions options = new ConnectionOptions(1, 2, 3, 4, 5000, proxy, configurator);

        ConnectionOptions copy = options.withoutInactivityTimeout();

        assertEquals(1, copy.getReadBufferSize());
        assertEquals(2, copy.getWriteBufferSize());
        assertEquals(3, copy.getWriteQueueSize());
        assertEquals(4, copy.getConnectTimeout());
        assertEquals(-1, copy.getInactivityTimeout());
        assertSame(proxy, copy.getProxyOptions());
        assertSame(configurator, copy.getConfigureSocket());
    }
}
