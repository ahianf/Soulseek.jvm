// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.options.SoulseekClientOptions;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The ratchet on the fold.
 *
 * <p>This used to assert that the concrete client implemented a public {@code
 * SoulseekClient} interface member for member. That interface is gone, and with
 * it the claim it encoded — that the library's public surface was one god
 * object. What is left underneath is an engine, and the useful thing to assert
 * about an engine mid-fold is not what it has but what it still has that it
 * should not.
 *
 * <p>So the inventory is split three ways. {@link #SEAM} is what the engine is
 * for: the context the extracted collaborators delegate through, and the
 * accessors the message handlers reach it by. {@link #ENGINE} is what it
 * genuinely owns: the connection lifecycle and the state that goes with it.
 * {@link #AWAITING_A_FACET} is the residue — blocking operations with a facet
 * waiting to take them — and it is now empty. What is left to remove is the
 * listener registry, which goes when the facets stop subscribing through it.
 */
class SoulseekClientApiTest {

    /** {@code ClientContext} plus the accessors the handlers reach the engine by. */
    private static final Set<String> SEAM = Set.of(
            "acknowledgePrivateMessageOperation",
            "acknowledgePrivilegeNotificationOperation",
            "defaultToken",
            "executeCorrelatedCommand",
            "executeCorrelatedRequest",
            "getClientOptions",
            "getDiagnostic",
            "getDistributedConnectionManager",
            "getDistributedMessageHandler",
            "getDownloadDictionary",
            "getDownloadRegistry",
            "getDownloadTokenBucket",
            "getIoAdapter",
            "getListener",
            "getLoggedInUsername",
            "getPeerConnectionManager",
            "getPeerMessageHandler",
            "getScheduler",
            "getSearchRegistry",
            "getSearchResponder",
            "getSearches",
            "getServerConnection",
            "getTokenFactory",
            "getUploadRegistry",
            "getUploadTokenBucket",
            "getUserEndpointOperation",
            "getWaiter",
            "raiseSearchEvent",
            "reportBrowseProgress",
            "requireLoggedIn",
            "resolveUserEndpoint",
            "writeBytesToServer",
            "writeToPeer",
            "writeToServer");

    /** The connection lifecycle and the state that belongs to it. */
    private static final Set<String> ENGINE = Set.of(
            "close",
            "connect",
            "disconnect",
            "getAddress",
            "getIpAddress",
            "getIpEndpoint",
            "getMajorVersion",
            "getMinorVersion",
            "getNextToken",
            "getOptions",
            "getPort",
            "getServerInfo",
            "getState",
            "getUsername",
            "reconfigureOptions");

    /**
     * Blocking operations with a facet waiting to take them.
     *
     * <p><strong>Empty, and that is the assertion.</strong> Every wrapper that
     * stood between a caller and a collaborator now lives in the facet that owns
     * it, and {@code ClientOperations} is deleted. What remains below is the
     * engine and the seam; the listener pairs are the last thing on the list.
     */
    private static final Set<String> AWAITING_A_FACET = Set.of();

    private static Set<String> publicInstanceMethodNames() {
        return Arrays.stream(DefaultSoulseekClient.class.getMethods())
                .filter(method -> method.getDeclaringClass() != Object.class)
                .filter(method -> !Modifier.isStatic(method.getModifiers()))
                .map(Method::getName)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    @Test
    @DisplayName("the engine carries the seam, the lifecycle, and nothing unaccounted for")
    void everyPublicMethodIsSeamOrLifecycle() {
        Set<String> observed = publicInstanceMethodNames();
        // Not "endsWith(Listener)": getListener() is the engine's own accessor
        // for the incoming-connection listener, not an event registration.
        observed.removeIf(name -> (name.startsWith("add") || name.startsWith("remove")) && name.endsWith("Listener"));

        Set<String> expected = Stream.of(SEAM, ENGINE, AWAITING_A_FACET)
                .flatMap(Set::stream)
                .collect(Collectors.toCollection(TreeSet::new));

        assertEquals(expected, observed);
    }

    @Test
    @DisplayName("the engine offers no blocking operation of its own")
    void noBlockingOperationSurvivesOnTheEngine() {
        assertTrue(
                AWAITING_A_FACET.isEmpty(), "the fold is not finished; these still have no facet: " + AWAITING_A_FACET);

        // The five collaborators are how a facet reaches the network now. They
        // are package-private, so a facet can hold one and nothing else can.
        for (String accessor : Set.of("rooms", "users", "server", "searchCoordinator", "transfers")) {
            assertThrows(
                    NoSuchMethodException.class,
                    () -> DefaultSoulseekClient.class.getMethod(accessor),
                    accessor + "() must stay package-private: it hands out a collaborator");
        }
    }

    @Test
    @DisplayName("the listener registry is intact until a facet replaces each of its events")
    void everyClientEventStillHasItsPair() {
        Set<String> names = publicInstanceMethodNames();
        long added = names.stream()
                .filter(name -> name.startsWith("add") && name.endsWith("Listener"))
                .count();
        long removed = names.stream()
                .filter(name -> name.startsWith("remove") && name.endsWith("Listener"))
                .count();

        assertEquals(47, added, "46 client events plus DiagnosticGenerated");
        assertEquals(added, removed, "every add has a remove");
    }

    @Test
    void constructionPreservesValidationDefaultsOptionsAndLifecycle() {
        assertThrows(IllegalArgumentException.class, () -> new DefaultSoulseekClient(100));

        try (DefaultSoulseekClient client = new DefaultSoulseekClient(9999)) {
            assertEquals(170, client.getMajorVersion());
            assertEquals(9999, client.getMinorVersion());
            assertNotNull(client.getOptions());
            assertEquals(SoulseekClientState.DISCONNECTED, client.getState());
            client.close();
        }

        SoulseekClientOptions options = new SoulseekClientOptions();
        try (DefaultSoulseekClient client = new DefaultSoulseekClient(9999, options)) {
            assertSame(options, client.getOptions());
        }
    }

    @Test
    @DisplayName("nothing about the engine is reachable from outside the package")
    void theEngineAndItsConstructorsAreNotPublic() {
        assertFalse(Modifier.isPublic(DefaultSoulseekClient.class.getModifiers()));
        Arrays.stream(DefaultSoulseekClient.class.getDeclaredConstructors())
                .forEach(constructor -> assertFalse(Modifier.isPublic(constructor.getModifiers())));

        for (Class<?> implemented : DefaultSoulseekClient.class.getInterfaces()) {
            assertTrue(
                    implemented == AutoCloseable.class || implemented.getName().startsWith("dev.slsk.internal."),
                    "the engine implements " + implemented.getName() + ", which is not internal");
        }
    }

    @Test
    void staticEventDispatchControlsRemainAvailable() {
        boolean original = DefaultSoulseekClient.isRaiseEventsAsynchronously();
        try {
            DefaultSoulseekClient.setRaiseEventsAsynchronously(!original);
            assertEquals(!original, DefaultSoulseekClient.isRaiseEventsAsynchronously());
        } finally {
            DefaultSoulseekClient.setRaiseEventsAsynchronously(original);
        }
    }
}
