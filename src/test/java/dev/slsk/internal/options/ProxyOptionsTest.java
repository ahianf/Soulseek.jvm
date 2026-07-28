// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.exceptions.AddressException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class ProxyOptionsTest {
    @Test
    @DisplayName("Instantiates properly")
    void instantiatesProperly() throws Exception {
        ProxyOptions options = new ProxyOptions("127.0.0.1", 1234, "user", "password");

        assertEquals("127.0.0.1", options.getAddress());
        assertEquals(1234, options.getPort());
        assertEquals("user", options.getUsername());
        assertEquals("password", options.getPassword());
        assertEquals(InetAddress.getByName("127.0.0.1"), options.getIpAddress());
        assertEquals(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 1234), options.getIpEndpoint());
    }

    @Test
    @DisplayName("Throws AddressException on bad address")
    void throwsAddressExceptionOnBadAddress() {
        AddressException exception =
                assertThrows(AddressException.class, () -> new ProxyOptions("not a valid host name", 1, "u", "p"));

        assertTrue(exception.getMessage().startsWith("Failed to resolve address '': "));
        assertTrue(exception.getCause() instanceof java.net.UnknownHostException);
    }

    @Test
    @DisplayName("Does not throw on resolvable address")
    void doesNotThrowOnResolvableAddress() {
        ProxyOptions options = new ProxyOptions("localhost", 1, "u", "p");

        assertTrue(options.getIpAddress().isLoopbackAddress());
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 65_536})
    @DisplayName("Throws IllegalArgumentException on bad port")
    void throwsOnBadPort(int port) {
        assertThrows(IllegalArgumentException.class, () -> new ProxyOptions("127.0.0.1", port, "u", "p"));
    }

    @ParameterizedTest
    @MethodSource("badInputs")
    @DisplayName("Throws IllegalArgumentException on bad input")
    void throwsOnBadInput(String address, String username, String password) {
        assertThrows(IllegalArgumentException.class, () -> new ProxyOptions(address, 1, username, password));
    }

    @Test
    @DisplayName("Does not throw if username and password are null")
    void doesNotThrowIfUsernameAndPasswordAreNull() {
        ProxyOptions options = new ProxyOptions("127.0.0.1", 1, null, null);

        assertNull(options.getUsername());
        assertNull(options.getPassword());
    }

    @ParameterizedTest
    @MethodSource("badCredentials")
    @DisplayName("Throws IllegalArgumentException on bad credential length")
    void throwsOnBadCredentialLength(String username, String password) {
        assertThrows(IllegalArgumentException.class, () -> new ProxyOptions("127.0.0.1", 1, username, password));
    }

    @Test
    @DisplayName("Accepts port and credential boundaries")
    void acceptsPortAndCredentialBoundaries() {
        String maxCredential = "a".repeat(255);

        assertEquals(0, new ProxyOptions("127.0.0.1", 0).getPort());
        assertEquals(65_535, new ProxyOptions("127.0.0.1", 65_535).getPort());
        ProxyOptions options = new ProxyOptions("127.0.0.1", 1, maxCredential, maxCredential);
        assertEquals(255, options.getUsername().length());
        assertEquals(255, options.getPassword().length());
    }

    @Test
    @DisplayName("Username-only optional overload follows canonical validation")
    void usernameOnlyOverloadFollowsCanonicalValidation() {
        assertThrows(IllegalArgumentException.class, () -> new ProxyOptions("127.0.0.1", 1, "user"));
    }

    private static Stream<Arguments> badInputs() {
        return Stream.of(
                Arguments.of("127.0.0.1", null, "a"),
                Arguments.of("127.0.0.1", "a", null),
                Arguments.of(null, "user", "pass"),
                Arguments.of("", "user", "pass"),
                Arguments.of(" ", "user", "pass"),
                Arguments.of("\u00a0", "user", "pass"));
    }

    private static Stream<Arguments> badCredentials() {
        String tooLong = "a".repeat(256);
        return Stream.of(
                Arguments.of("", ""),
                Arguments.of("", "a"),
                Arguments.of("a", ""),
                Arguments.of(tooLong, tooLong),
                Arguments.of(tooLong, "a"),
                Arguments.of("a", tooLong));
    }
}
