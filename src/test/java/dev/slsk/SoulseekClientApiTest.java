// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.diagnostics.DiagnosticSource;
import dev.slsk.options.SoulseekClientOptions;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SoulseekClientApiTest {
    @Test
    void concreteClientImplementsTheCompleteInterfaceSurface() throws NoSuchMethodException {
        assertTrue(SoulseekClient.class.isAssignableFrom(DefaultSoulseekClient.class));
        assertTrue(AutoCloseable.class.isAssignableFrom(SoulseekClient.class));
        assertTrue(DiagnosticSource.class.isAssignableFrom(SoulseekClient.class));

        for (Method method : SoulseekClient.class.getMethods()) {
            if (Modifier.isStatic(method.getModifiers())) {
                continue;
            }
            Method implementation = DefaultSoulseekClient.class.getMethod(method.getName(), method.getParameterTypes());
            assertEquals(method.getReturnType(), implementation.getReturnType(), method::toGenericString);
        }
    }

    @Test
    void implementationHasNoUnaccountedPublicInstanceOperations() {
        Set<String> collaborationHooks = Set.of(
                "getDistributedConnectionManager",
                "getDistributedMessageHandler",
                "getDownloadDictionary",
                "getListener",
                "getPeerConnectionManager",
                "getPeerMessageHandler",
                "getSearches",
                "getSearchResponder",
                "getServerConnection",
                "getWaiter",
                // Future-shaped operations the internal collaborator interfaces
                // still call directly. The public API in front of them is
                // blocking; these disappear when the client is decomposed.
                // ClientContext, the seam the extracted components delegate
                // through (Phase 6). Not part of the public surface.
                "requireLoggedIn",
                "defaultToken",
                "getClientOptions",
                "getDiagnostic",
                "writeToServer",
                "executeCorrelatedCommand",
                "executeCorrelatedRequest",
                "writeToPeer",
                "resolveUserEndpoint",
                "reportBrowseProgress",
                "getLoggedInUsername",
                "acknowledgePrivateMessageOperation",
                "acknowledgePrivilegeNotificationOperation",
                "getUserEndpointOperation");
        Set<String> observedHooks = new HashSet<>();

        for (Method method : DefaultSoulseekClient.class.getDeclaredMethods()) {
            if (!Modifier.isPublic(method.getModifiers()) || Modifier.isStatic(method.getModifiers())) {
                continue;
            }
            try {
                Method contract = SoulseekClient.class.getMethod(method.getName(), method.getParameterTypes());
                assertEquals(method.getReturnType(), contract.getReturnType(), method::toGenericString);
            } catch (NoSuchMethodException exception) {
                assertTrue(collaborationHooks.contains(method.getName()), method::toGenericString);
                observedHooks.add(method.getName());
            }
        }

        assertEquals(collaborationHooks, observedHooks);
    }

    @Test
    void interfaceAccountsForEveryClientEventContract() {
        long addMethods = Arrays.stream(SoulseekClient.class.getMethods())
                .map(Method::getName)
                .filter(name -> name.startsWith("add") && name.endsWith("Listener"))
                .count();
        long removeMethods = Arrays.stream(SoulseekClient.class.getMethods())
                .map(Method::getName)
                .filter(name -> name.startsWith("remove") && name.endsWith("Listener"))
                .count();

        assertEquals(47, addMethods, "46 client events plus DiagnosticGenerated");
        assertEquals(47, removeMethods);
    }

    @Test
    void factoriesPreserveValidationDefaultsOptionsAndLifecycle() {
        assertThrows(IllegalArgumentException.class, () -> SoulseekClient.create(100));

        try (SoulseekClient client = SoulseekClient.create(9999)) {
            assertEquals(170, client.getMajorVersion());
            assertEquals(9999, client.getMinorVersion());
            assertNotNull(client.getOptions());
            assertEquals(SoulseekClientState.DISCONNECTED, client.getState());
            client.close();
        }

        SoulseekClientOptions options = new SoulseekClientOptions();
        try (SoulseekClient client = SoulseekClient.create(9999, options)) {
            assertSame(options, client.getOptions());
        }
    }

    @Test
    void implementationAndConstructorsAreNotPublic() {
        assertTrue(SoulseekClient.class.isInterface());
        assertFalse(Modifier.isPublic(DefaultSoulseekClient.class.getModifiers()));
        Arrays.stream(DefaultSoulseekClient.class.getDeclaredConstructors())
                .forEach(constructor -> assertFalse(Modifier.isPublic(constructor.getModifiers())));
    }

    @Test
    void staticEventDispatchControlsRemainAvailable() {
        boolean original = SoulseekClient.isRaiseEventsAsynchronously();
        try {
            SoulseekClient.setRaiseEventsAsynchronously(!original);
            assertEquals(!original, SoulseekClient.isRaiseEventsAsynchronously());
        } finally {
            SoulseekClient.setRaiseEventsAsynchronously(original);
        }
    }
}
