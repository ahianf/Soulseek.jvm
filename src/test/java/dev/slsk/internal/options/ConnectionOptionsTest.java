// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ConnectionOptionsTest {
    @Test
    @DisplayName("Instantiates properly")
    void instantiatesProperly() {
        ConnectionOptions options = ConnectionOptions.builder()
                .readBufferSize(-1)
                .writeBufferSize(-2)
                .writeQueueSize(-3)
                .connectTimeout(Duration.ofMillis(-4))
                .inactivityTimeout(Duration.ofMillis(-5))
                .build();

        assertEquals(-1, options.readBufferSize());
        assertEquals(-2, options.writeBufferSize());
        assertEquals(-3, options.writeQueueSize());
        assertEquals(Duration.ofMillis(-4), options.connectTimeout());
        assertEquals(Duration.ofMillis(-5), options.inactivityTimeout());
        assertNull(options.proxyOptions());
        assertNotNull(options.configureSocket());
    }

    @Test
    @DisplayName("Uses source defaults")
    void usesSourceDefaults() throws Exception {
        ConnectionOptions options = new ConnectionOptions();

        assertEquals(16_384, options.readBufferSize());
        assertEquals(16_384, options.writeBufferSize());
        assertEquals(250, options.writeQueueSize());
        assertEquals(Duration.ofSeconds(10), options.connectTimeout());
        assertEquals(Duration.ofSeconds(15), options.inactivityTimeout());
        try (Socket socket = new Socket()) {
            options.configureSocket().configure(socket);
        }
    }

    /**
     * The relayed path gets its own budget, and never a shorter one than the
     * direct connect it used to borrow.
     */
    @Test
    @DisplayName("Indirect solicitation waits at least twenty seconds")
    void indirectSolicitationTimeoutFloorsAtTwentySeconds() {
        assertEquals(Duration.ofSeconds(20), new ConnectionOptions().indirectSolicitationTimeout());
        assertEquals(
                Duration.ofSeconds(20),
                ConnectionOptions.builder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .build()
                        .indirectSolicitationTimeout());
        assertEquals(
                Duration.ofSeconds(45),
                ConnectionOptions.builder()
                        .connectTimeout(Duration.ofSeconds(45))
                        .build()
                        .indirectSolicitationTimeout());
    }

    @Test
    @DisplayName("Builder preserves defaults for fields not named")
    void builderPreservesUnnamedDefaults() {
        ConnectionOptions options =
                ConnectionOptions.builder().readBufferSize(1).build();

        assertEquals(16_384, options.writeBufferSize());
        assertEquals(250, options.writeQueueSize());
        assertEquals(Duration.ofSeconds(10), options.connectTimeout());
        assertEquals(Duration.ofSeconds(15), options.inactivityTimeout());
    }

    @Test
    @DisplayName("Canonical constructor retains proxy and callback")
    void canonicalConstructorRetainsProxyAndCallback() throws Exception {
        ProxyOptions proxy = new ProxyOptions("127.0.0.1", 1);
        AtomicBoolean invoked = new AtomicBoolean();
        SocketConfigurator configurator = socket -> invoked.set(true);
        ConnectionOptions options = ConnectionOptions.builder()
                .readBufferSize(1)
                .writeBufferSize(2)
                .writeQueueSize(3)
                .connectTimeout(Duration.ofMillis(4))
                .inactivityTimeout(Duration.ofMillis(5))
                .proxyOptions(proxy)
                .configureSocket(configurator)
                .build();

        try (Socket socket = new Socket()) {
            options.configureSocket().configure(socket);
        }

        assertSame(proxy, options.proxyOptions());
        assertSame(configurator, options.configureSocket());
        assertEquals(true, invoked.get());
    }

    @Test
    @DisplayName("WithoutInactivityTimeout removes the deadline")
    void withoutInactivityTimeoutRemovesDeadline() {
        ProxyOptions proxy = new ProxyOptions("127.0.0.1", 1);
        SocketConfigurator configurator = socket -> {};
        ConnectionOptions options = ConnectionOptions.builder()
                .readBufferSize(1)
                .writeBufferSize(2)
                .writeQueueSize(3)
                .connectTimeout(Duration.ofMillis(4))
                .inactivityTimeout(Duration.ofSeconds(5))
                .proxyOptions(proxy)
                .configureSocket(configurator)
                .build();

        ConnectionOptions copy = options.withoutInactivityTimeout();

        assertEquals(1, copy.readBufferSize());
        assertEquals(2, copy.writeBufferSize());
        assertEquals(3, copy.writeQueueSize());
        assertEquals(Duration.ofMillis(4), copy.connectTimeout());
        assertNull(copy.inactivityTimeout());
        assertSame(proxy, copy.proxyOptions());
        assertSame(configurator, copy.configureSocket());
    }
}
