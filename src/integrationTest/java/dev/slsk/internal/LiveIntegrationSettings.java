// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.options.SoulseekClientOptions;
import java.util.Set;
import org.junit.jupiter.api.Assumptions;

final class LiveIntegrationSettings {
    /**
     * The port peers reach this client on during a live run.
     *
     * <p>40000, because that is the port forwarded to the machine these tests
     * are run from. It is not the library's default and must not become it —
     * the shipped default is part of the frozen public surface — but a live run
     * that advertises a port nothing forwards can only ever exercise the half
     * of the protocol we initiate. Peers that answer a solicitation by
     * connecting back, inbound browse and inbound transfer requests all arrive
     * here or not at all.
     */
    static final int LISTEN_PORT = 40_000;

    private static final String USERNAME = "SLSK_INTEGRATION_USERNAME";
    private static final String PASSWORD = "SLSK_INTEGRATION_PASSWORD";
    private static final String MINOR_VERSION = "SLSK_INTEGRATION_MINOR_VERSION";
    private static final String UNIQUE_ATTESTATION = "SLSK_INTEGRATION_MINOR_VERSION_UNIQUE";
    private static final String LISTEN_PORT_OVERRIDE = "SLSK_INTEGRATION_LISTEN_PORT";
    private static final Set<Integer> DOCUMENTED_MINOR_VERSIONS = Set.of(100, 760, 9_999);

    private LiveIntegrationSettings() {}

    /**
     * Returns the options a live client runs with.
     *
     * <p>Stock apart from the listener, which has to be on a port the network
     * in front of this machine forwards. {@code SLSK_INTEGRATION_LISTEN_PORT}
     * overrides it for anyone whose network forwards a different one.
     *
     * @return the options
     */
    static SoulseekClientOptions options() {
        return SoulseekClientOptions.builder().listenPort(listenPort()).build();
    }

    private static int listenPort() {
        String override = System.getenv(LISTEN_PORT_OVERRIDE);
        if (!isPresent(override)) {
            return LISTEN_PORT;
        }
        try {
            return Integer.parseInt(override.trim());
        } catch (NumberFormatException exception) {
            throw new AssertionError(LISTEN_PORT_OVERRIDE + " must be an integer", exception);
        }
    }

    static Credentials requireCredentials() {
        String username = System.getenv(USERNAME);
        String password = System.getenv(PASSWORD);
        String minorVersion = System.getenv(MINOR_VERSION);
        String attestation = System.getenv(UNIQUE_ATTESTATION);
        Assumptions.assumeTrue(
                isPresent(username)
                        && isPresent(password)
                        && isPresent(minorVersion)
                        && Boolean.parseBoolean(attestation),
                "Live Soulseek credentials, a minor version, and uniqueness " + "attestation were not supplied");

        int parsedMinorVersion;
        try {
            parsedMinorVersion = Integer.parseInt(minorVersion);
        } catch (NumberFormatException exception) {
            throw new AssertionError(MINOR_VERSION + " must be an integer", exception);
        }
        assertTrue(parsedMinorVersion > 100, MINOR_VERSION + " must be greater than 100");
        assertFalse(
                DOCUMENTED_MINOR_VERSIONS.contains(parsedMinorVersion),
                MINOR_VERSION + " is already listed in the maintained " + "Nicotine+ protocol registry");
        return new Credentials(username, password, parsedMinorVersion);
    }

    private static boolean isPresent(String value) {
        return value != null && !value.isBlank();
    }

    record Credentials(String username, String password, int minorVersion) {}
}
