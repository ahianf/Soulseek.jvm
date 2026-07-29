// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.internal.diagnostics.DiagnosticSink;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import org.junit.jupiter.api.Test;

class EngineCleanupTest {
    @Test
    void exitsWhenUploadSynchronizationRootIsHeld() {
        try (Fixture fixture = new Fixture()) {
            Semaphore syncRoot = fixture.client.transfers().uploadSemaphoreSyncRootForTest();
            syncRoot.acquireUninterruptibly();
            Semaphore user = new Semaphore(1);
            fixture.client.transfers().uploadSemaphoresForTest().put("alice", user);
            try {
                fixture.client.transfers().cleanupUploadSemaphores();
                assertSame(
                        user,
                        fixture.client.transfers().uploadSemaphoresForTest().get("alice"));
            } finally {
                syncRoot.release();
            }
        }
    }

    @Test
    void removesAvailableUploadSemaphoreAndEmitsDiagnostic() {
        try (Fixture fixture = new Fixture()) {
            fixture.client.transfers().uploadSemaphoresForTest().put("alice", new Semaphore(1));

            fixture.client.transfers().cleanupUploadSemaphores();

            assertTrue(fixture.client.transfers().uploadSemaphoresForTest().isEmpty());
            assertEquals(List.of("Cleaned up upload semaphore for alice"), fixture.diagnostic.debugMessages);
        }
    }

    /**
     * The sweep and the upload path have to be looking at the same map.
     *
     * <p>They were not. There were two per-user semaphore maps — one on the
     * engine, which the sweep ran against, and one on the transfer engine, which
     * every upload actually took its permit from — so the map that filled up was
     * never swept and the map that was swept was always empty. One owner is what
     * makes this assertable at all.
     */
    @Test
    void theSweepReachesTheSemaphoresAnUploadActuallyTakes() {
        try (Fixture fixture = new Fixture()) {
            TransferDomain transfers = fixture.client.transfers();
            Semaphore taken = transfers.uploadSemaphoreFor("alice", dev.slsk.CancellationSignal.none());
            assertSame(taken, transfers.uploadSemaphoresForTest().get("alice"));

            taken.acquireUninterruptibly();
            transfers.cleanupUploadSemaphores();
            assertSame(taken, transfers.uploadSemaphoresForTest().get("alice"), "a semaphore in use is kept");

            // Between an upload's fetch and its acquire the semaphore sits at
            // full permits; sweeping it there hands the next upload to the
            // same user a fresh one, and both run concurrently against one
            // peer. The lease taken at the fetch is what covers that window.
            taken.release();
            transfers.cleanupUploadSemaphores();
            assertSame(
                    taken,
                    transfers.uploadSemaphoresForTest().get("alice"),
                    "a semaphore a run still references is kept, permits or not");

            transfers.releaseUploadSemaphoreLease("alice");
            transfers.cleanupUploadSemaphores();
            assertTrue(
                    transfers.uploadSemaphoresForTest().isEmpty(), "a semaphore nothing holds or references is swept");
        }
    }

    @Test
    void retainsUploadSemaphoreWithoutAvailablePermit() {
        try (Fixture fixture = new Fixture()) {
            Semaphore held = new Semaphore(0);
            fixture.client.transfers().uploadSemaphoresForTest().put("alice", held);

            fixture.client.transfers().cleanupUploadSemaphores();

            assertSame(
                    held, fixture.client.transfers().uploadSemaphoresForTest().get("alice"));
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
        private final SoulseekEngine client = new SoulseekEngine(
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
