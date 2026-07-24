// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.junit.jupiter.api.Assumptions;

final class LiveIntegrationSettings {
    private static final String USERNAME = "SLSK_INTEGRATION_USERNAME";
    private static final String PASSWORD = "SLSK_INTEGRATION_PASSWORD";
    private static final String MINOR_VERSION = "SLSK_INTEGRATION_MINOR_VERSION";
    private static final String UNIQUE_ATTESTATION = "SLSK_INTEGRATION_MINOR_VERSION_UNIQUE";
    private static final Set<Integer> DOCUMENTED_MINOR_VERSIONS = Set.of(100, 760, 9_999);

    private LiveIntegrationSettings() {}

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
