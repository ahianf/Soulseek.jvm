// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.diagnostics.DiagnosticSink;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import org.junit.jupiter.api.Test;

class SoulseekClientCleanupTest {
    @Test
    void exitsWhenUploadSynchronizationRootIsHeld() {
        try (Fixture fixture = new Fixture()) {
            Semaphore syncRoot = fixture.client.getUploadSemaphoreSyncRootForTest();
            syncRoot.acquireUninterruptibly();
            Semaphore user = new Semaphore(1);
            fixture.client.getUploadSemaphoresForTest().put("alice", user);
            try {
                fixture.client.cleanupUploadSemaphoresAsync().join();
                assertSame(user, fixture.client.getUploadSemaphoresForTest().get("alice"));
            } finally {
                syncRoot.release();
            }
        }
    }

    @Test
    void removesAvailableUploadSemaphoreAndEmitsDiagnostic() {
        try (Fixture fixture = new Fixture()) {
            fixture.client.getUploadSemaphoresForTest().put("alice", new Semaphore(1));

            fixture.client.cleanupUploadSemaphoresAsync().join();

            assertTrue(fixture.client.getUploadSemaphoresForTest().isEmpty());
            assertEquals(List.of("Cleaned up upload semaphore for alice"), fixture.diagnostic.debugMessages);
        }
    }

    @Test
    void retainsUploadSemaphoreWithoutAvailablePermit() {
        try (Fixture fixture = new Fixture()) {
            Semaphore held = new Semaphore(0);
            fixture.client.getUploadSemaphoresForTest().put("alice", held);

            fixture.client.cleanupUploadSemaphoresAsync().join();

            assertSame(held, fixture.client.getUploadSemaphoresForTest().get("alice"));
            assertTrue(fixture.diagnostic.debugMessages.isEmpty());
        }
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == double.class) {
            return 0D;
        }
        return null;
    }

    private static final class Fixture implements AutoCloseable {
        private final DiagnosticProbe diagnostic = new DiagnosticProbe();
        private final SoulseekClient client = new SoulseekClient(
                9999,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                diagnostic.proxy,
                null,
                null,
                null);

        @Override
        public void close() {
            client.close();
        }
    }

    private static final class DiagnosticProbe {
        private final List<String> debugMessages = new ArrayList<>();
        private final DiagnosticSink proxy = (DiagnosticSink) Proxy.newProxyInstance(
                DiagnosticSink.class.getClassLoader(), new Class<?>[] {DiagnosticSink.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("debug")) {
                debugMessages.add((String) arguments[0]);
                return null;
            }
            return defaultValue(method.getReturnType());
        }
    }
}
