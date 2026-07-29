// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.EngineEvents.Kind;
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
 * <p>This used to assert that the concrete client implemented a public
 * {@code SoulseekClient} interface member for member. That interface is gone,
 * and with it the claim it encoded — that the library's public surface was one
 * god object. What is left underneath is an engine, and the useful thing to
 * assert about an engine mid-fold is not what it has but what it still has that
 * it should not.
 *
 * <p>So the inventory is split three ways. {@link #SEAM} is what the engine is
 * for: the context the extracted collaborators delegate through, and the
 * accessors the message handlers reach it by. {@link #ENGINE} is what it
 * genuinely owns: the connection lifecycle and the state that goes with it.
 * {@link #AWAITING_A_FACET} is the residue — blocking operations with a facet
 * waiting to take them — and it is now empty, as is the listener registry that
 * once sat beside it. What the engine has left is what an engine should have.
 */
class EngineApiTest {

    /**
     * The one collaborator interface the engine still answers.
     *
     * <p>It was nine. Eight went the same way: the members were one-line
     * accessors, so each component took what it used as a constructor argument
     * and the interface was deleted — {@code EngineContext} with them, and with
     * it every public method that existed only because an interface declared
     * it. The engine's public surface went from forty-eight names to
     * twenty-one in one commit without one line of behaviour moving.
     *
     * <p>What is left is {@link dev.slsk.internal.messaging.handlers.PeerServices}:
     * what this client offers a peer. Its six members are the upload, share and
     * profile state, and they leave with {@code TransferEngine} in Phase 4.
     */
    private static final Set<String> SEAM =
            Set.of("admission", "catalog", "offered", "profile", "serve", "uploadPolicy");

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
        return Arrays.stream(SoulseekEngine.class.getMethods())
                .filter(method -> method.getDeclaringClass() != Object.class)
                .filter(method -> !Modifier.isStatic(method.getModifiers()))
                .map(Method::getName)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    @Test
    @DisplayName("the engine carries the seam, the lifecycle, and nothing unaccounted for")
    void everyPublicMethodIsSeamOrLifecycle() {
        Set<String> observed = publicInstanceMethodNames();
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
        for (String accessor : Set.of("rooms", "users", "server", "searches", "transfers")) {
            assertThrows(
                    NoSuchMethodException.class,
                    () -> SoulseekEngine.class.getMethod(accessor),
                    accessor + "() must stay package-private: it hands out a collaborator");
        }
    }

    @Test
    @DisplayName("the named listener registry is gone, and every event it named is a kind")
    void theListenerRegistryCollapsedIntoOneChannel() {
        Set<String> names = publicInstanceMethodNames();
        assertTrue(
                names.stream()
                        .noneMatch(name ->
                                (name.startsWith("add") || name.startsWith("remove")) && name.endsWith("Listener")),
                "the engine still carries named listener registrations: " + names);

        assertEquals(47, Kind.values().length, "46 client events plus DiagnosticGenerated");
    }

    @Test
    void constructionPreservesValidationDefaultsOptionsAndLifecycle() {
        assertThrows(IllegalArgumentException.class, () -> new SoulseekEngine(100));

        try (SoulseekEngine client = new SoulseekEngine(9999)) {
            assertEquals(170, client.getMajorVersion());
            assertEquals(9999, client.getMinorVersion());
            assertNotNull(client.getOptions());
            assertEquals(SoulseekClientState.DISCONNECTED, client.getState());
            client.close();
        }

        SoulseekClientOptions options = new SoulseekClientOptions();
        try (SoulseekEngine client = new SoulseekEngine(9999, options)) {
            assertSame(options, client.getOptions());
        }
    }

    @Test
    @DisplayName("nothing about the engine is reachable from outside the package")
    void theEngineAndItsConstructorsAreNotPublic() {
        assertFalse(Modifier.isPublic(SoulseekEngine.class.getModifiers()));
        Arrays.stream(SoulseekEngine.class.getDeclaredConstructors())
                .forEach(constructor -> assertFalse(Modifier.isPublic(constructor.getModifiers())));

        for (Class<?> implemented : SoulseekEngine.class.getInterfaces()) {
            assertTrue(
                    implemented == AutoCloseable.class || implemented.getName().startsWith("dev.slsk.internal."),
                    "the engine implements " + implemented.getName() + ", which is not internal");
        }
    }

    @Test
    @DisplayName("event dispatch and diagnostics are decided per client, never per process")
    void twoClientsDoNotShareDispatchOrDiagnostics() {
        // This replaces two tests that covered a static dispatch flag and a
        // static diagnostic factory. Both were write-only in production: real
        // dispatch reads the per-connection ConnectionOptions and every
        // diagnostic goes to the engine's own sink, so the only readers those
        // globals ever had were the tests defending them. What is worth
        // asserting is the property that made them safe to delete.
        try (SoulseekEngine first = new SoulseekEngine(9999, dispatching(true));
                SoulseekEngine second = new SoulseekEngine(9999, dispatching(false))) {
            assertTrue(first.getOptions().isRaiseEventsAsynchronously());
            assertFalse(second.getOptions().isRaiseEventsAsynchronously());

            // And the decision reaches the transport that acts on it, which is
            // the only place it is ever read.
            assertTrue(first.getOptions().getServerConnectionOptions().isRaiseEventsAsynchronously());
            assertFalse(second.getOptions().getServerConnectionOptions().isRaiseEventsAsynchronously());

            assertNotSame(first.getDiagnostic(), second.getDiagnostic());
        }
    }

    /** Default options but for the dispatch policy, which is the only thing under test. */
    private static SoulseekClientOptions dispatching(boolean asynchronously) {
        SoulseekClientOptions defaults = new SoulseekClientOptions();
        return new SoulseekClientOptions(
                defaults.isEnableListener(),
                defaults.getListenIpAddress(),
                defaults.getListenPort(),
                defaults.isEnableDistributedNetwork(),
                defaults.isAcceptDistributedChildren(),
                defaults.getDistributedChildLimit(),
                defaults.getMaximumConcurrentSearches(),
                defaults.getMaximumConcurrentUploads(),
                defaults.getMaximumUploadSpeed(),
                defaults.getMaximumConcurrentDownloads(),
                defaults.getMaximumDownloadSpeed(),
                defaults.isDeduplicateSearchRequests(),
                defaults.getMessageTimeout(),
                defaults.isAutoAcknowledgePrivateMessages(),
                defaults.isAutoAcknowledgePrivilegeNotifications(),
                defaults.isAcceptPrivateRoomInvitations(),
                defaults.getMinimumDiagnosticLevel(),
                defaults.getStartingToken(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                asynchronously);
    }

    @Test
    @DisplayName("no process-global mutable state survives in the engine")
    void theEngineHoldsNoStaticMutableState() {
        for (java.lang.reflect.Field field : SoulseekEngine.class.getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            assertTrue(
                    Modifier.isFinal(field.getModifiers()),
                    "SoulseekEngine." + field.getName() + " is static and mutable");
        }
    }
}
