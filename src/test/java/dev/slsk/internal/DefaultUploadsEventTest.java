// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.events.UploadEvent;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.options.TransferStateChange;
import dev.slsk.internal.transfer.TransferInternal;
import dev.slsk.transfer.TransferOutcome;
import dev.slsk.user.Username;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The uploads bus used to be silent: no code anywhere published any
 * UploadEvent, so a consumer subscribing to {@code uploads().events()}
 * received nothing, ever, and a finished upload vanished from {@code all()}
 * with no {@code Finished} ever firing. These tests pin the wiring: the facet
 * registers itself with the transfer domain and the admission, and every
 * lifecycle step reaches the stream.
 */
@Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DefaultUploadsEventTest {

    @Test
    @DisplayName("an upload's lifecycle reaches the event stream: requested, state changes, finished")
    void uploadLifecycleIsPublished() throws Exception {
        try (Fixture fixture = new Fixture()) {
            List<UploadEvent> seen = new CopyOnWriteArrayList<>();
            CountDownLatch finished = new CountDownLatch(1);
            fixture.uploads.events().subscribe(event -> {
                seen.add(event);
                if (event instanceof UploadEvent.Finished) {
                    finished.countDown();
                }
            });

            TransferDomain.UploadObserver observer = fixture.client.transfers().uploadObserverForTest();
            assertNotNull(observer, "the facet registers itself with the domain");

            // The lifecycle a served upload reports, driven the way UploadRun
            // drives it: a first transition from NONE, a move to REQUESTED,
            // and a terminal SUCCEEDED.
            TransferInternal upload = new TransferInternal(TransferDirection.UPLOAD, "alice", "file", 42);
            upload.setState(TransferState.QUEUED.or(TransferState.LOCALLY));
            observer.stateChanged(new TransferStateChange(TransferState.NONE, upload.toTransfer()));

            TransferState previous = upload.getState();
            upload.setState(TransferState.REQUESTED);
            observer.stateChanged(new TransferStateChange(previous, upload.toTransfer()));

            previous = upload.getState();
            upload.setState(TransferState.COMPLETED.or(TransferState.SUCCEEDED));
            observer.stateChanged(new TransferStateChange(previous, upload.toTransfer()));

            assertTrue(finished.await(5, TimeUnit.SECONDS), "the terminal event never arrived");
            assertInstanceOf(UploadEvent.Requested.class, seen.get(0), "the first transition is the request");
            assertTrue(
                    seen.stream().anyMatch(UploadEvent.StateChanged.class::isInstance),
                    "intermediate transitions are state changes");
            UploadEvent.Finished terminal = (UploadEvent.Finished) seen.stream()
                    .filter(UploadEvent.Finished.class::isInstance)
                    .findFirst()
                    .orElseThrow();
            assertInstanceOf(TransferOutcome.Succeeded.class, terminal.outcome());
        }
    }

    @Test
    @DisplayName("a refusal — here a ban — is published as Denied")
    void aRefusalIsPublishedAsDenied() throws Exception {
        try (Fixture fixture = new Fixture()) {
            CountDownLatch received = new CountDownLatch(1);
            List<UploadEvent.Denied> denials = new CopyOnWriteArrayList<>();
            fixture.uploads.events().subscribe(UploadEvent.Denied.class, event -> {
                denials.add(event);
                received.countDown();
            });

            fixture.uploads.ban(Username.of("mallory"), "spamming");
            fixture.client.transfers().admission().decide(Username.of("mallory"), "song.mp3");

            assertTrue(received.await(5, TimeUnit.SECONDS), "the denial never arrived");
            assertEquals(Username.of("mallory"), denials.get(0).user());
            assertEquals("song.mp3", denials.get(0).path());
            assertEquals("spamming", denials.get(0).reason());
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

    /** An uploads facet over an engine whose server connection is a stub. */
    private static final class Fixture implements AutoCloseable {

        private final DiagnosticSink diagnostics = (DiagnosticSink) Proxy.newProxyInstance(
                DiagnosticSink.class.getClassLoader(),
                new Class<?>[] {DiagnosticSink.class},
                (proxy, method, arguments) -> defaultValue(method.getReturnType()));
        private final SoulseekEngine client;
        private final DefaultUploads uploads;

        private Fixture() {
            MessageConnection connection = (MessageConnection) Proxy.newProxyInstance(
                    MessageConnection.class.getClassLoader(),
                    new Class<?>[] {MessageConnection.class},
                    (proxy, method, arguments) -> defaultValue(method.getReturnType()));
            client = new SoulseekEngine(
                    9999,
                    null,
                    connection,
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
                    diagnostics,
                    null,
                    null,
                    null);
            uploads = new DefaultUploads(client, new EventBus<>("uploads", diagnostics));
        }

        @Override
        public void close() {
            client.close();
        }
    }
}
