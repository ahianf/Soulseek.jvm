// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Assumptions;

/**
 * Locates the pinned Soulseek.NET behavioral oracle so the parity inventories can be verified against the source they
 * claim to map.
 *
 * <p>The oracle is a sibling checkout rather than a module of this build. Tests that compare against it abort with a
 * reason when it is absent, so a checkout without the oracle still produces a passing suite while a checkout with the
 * oracle enforces the parity claims.
 */
final class CsharpOracle {
    /** System property naming the Soulseek.NET checkout root. */
    static final String PROPERTY = "soulseek.csharp.oracle";

    /** Environment variable naming the Soulseek.NET checkout root. */
    static final String ENVIRONMENT_VARIABLE = "SOULSEEK_CSHARP_ORACLE";

    /** Location used when neither the property nor the environment variable is set. */
    static final Path DEFAULT_ROOT = Path.of("..", "Soulseek.NET");

    private CsharpOracle() {}

    /**
     * Returns the configured oracle root, aborting the calling test when it does not contain the expected library
     * sources.
     *
     * @return the oracle checkout root
     */
    static Path requireRoot() {
        Path root = configuredRoot();
        Assumptions.assumeTrue(
                isOracleRoot(root),
                () -> "The pinned Soulseek.NET oracle was not found at "
                        + root.toAbsolutePath() + ". Set -D" + PROPERTY + " or " + ENVIRONMENT_VARIABLE
                        + " to a Soulseek.NET checkout to enforce the parity inventories.");
        return root;
    }

    /**
     * Returns the oracle library source root.
     *
     * @return the {@code src} directory of the oracle checkout
     */
    static Path requireSourceRoot() {
        return requireRoot().resolve("src");
    }

    /**
     * Returns the oracle unit test root.
     *
     * @return the unit test project directory of the oracle checkout
     */
    static Path requireUnitTestRoot() {
        return requireRoot().resolve(Path.of("tests", "Soulseek.Tests.Unit"));
    }

    /**
     * Returns the oracle integration test root.
     *
     * @return the integration test project directory of the oracle checkout
     */
    static Path requireIntegrationTestRoot() {
        return requireRoot().resolve(Path.of("tests", "Soulseek.Tests.Integration"));
    }

    private static Path configuredRoot() {
        String property = System.getProperty(PROPERTY);
        if (property != null && !property.isBlank()) {
            return Path.of(property);
        }
        String environment = System.getenv(ENVIRONMENT_VARIABLE);
        if (environment != null && !environment.isBlank()) {
            return Path.of(environment);
        }
        return DEFAULT_ROOT;
    }

    private static boolean isOracleRoot(Path root) {
        return Files.isDirectory(root.resolve("src"))
                && Files.isRegularFile(root.resolve(Path.of("src", "SoulseekClient.cs")));
    }
}
