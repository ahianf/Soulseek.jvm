// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.diagnostics.IDiagnosticGenerator;
import java.lang.reflect.Method;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class ISoulseekClientTest {
    @Test
    void concreteClientImplementsTheCompleteInterfaceSurface() throws NoSuchMethodException {
        assertTrue(ISoulseekClient.class.isAssignableFrom(SoulseekClient.class));
        assertTrue(AutoCloseable.class.isAssignableFrom(ISoulseekClient.class));
        assertTrue(IDiagnosticGenerator.class.isAssignableFrom(ISoulseekClient.class));

        for (Method method : ISoulseekClient.class.getMethods()) {
            Method implementation = SoulseekClient.class.getMethod(method.getName(), method.getParameterTypes());
            assertEquals(method.getReturnType(), implementation.getReturnType(), method::toGenericString);
        }
    }

    @Test
    void interfaceAccountsForEverySourceEventContract() {
        long addMethods = Arrays.stream(ISoulseekClient.class.getMethods())
                .map(Method::getName)
                .filter(name -> name.startsWith("add") && name.endsWith("Listener"))
                .count();
        long removeMethods = Arrays.stream(ISoulseekClient.class.getMethods())
                .map(Method::getName)
                .filter(name -> name.startsWith("remove") && name.endsWith("Listener"))
                .count();

        assertEquals(47, addMethods, "46 client events plus DiagnosticGenerated");
        assertEquals(47, removeMethods);
    }

    @Test
    void clientCanBeUsedAndClosedThroughInterface() {
        ISoulseekClient client = new SoulseekClient(9999);
        assertEquals(170, client.getMajorVersion());
        assertEquals(9999, client.getMinorVersion());
        client.close();
        client.close();
    }
}
