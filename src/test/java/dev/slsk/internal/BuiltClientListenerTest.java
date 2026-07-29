// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.Soulseek;
import dev.slsk.internal.options.SoulseekClientOptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Where a client built by the builder listens for peers.
 *
 * <p>The listen port is advertised to the server and handed to every peer that
 * wants to reach this client. A listener bound to the loopback address
 * advertises a port nothing outside the machine can connect to, and the failure
 * is invisible: the server offers an indirect connection for every peer that
 * cannot reach us, so the client still talks to peers and still returns search
 * results. What it loses is every inbound direct connection, which is most of
 * what a peer does with the port it was given.
 */
class BuiltClientListenerTest {

    @Test
    @DisplayName("a built client listens for peers on every address, not just loopback")
    void theListenerIsReachableFromOffTheMachine() {
        try (Soulseek slsk = Soulseek.builder()
                .credentials("alice", "password")
                .applicationMinorVersion(157)
                .listenPort(2235)
                .build()) {
            SoulseekClientOptions options = ((DefaultSoulseek) slsk).client().getOptions();

            assertTrue(
                    options.getListenIpAddress().isAnyLocalAddress(),
                    "expected the wildcard address, got " + options.getListenIpAddress());
            assertEquals(2235, options.getListenPort());
            assertTrue(options.isEnableListener());
        }
    }
}
