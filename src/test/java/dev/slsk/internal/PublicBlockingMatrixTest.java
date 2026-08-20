// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.Soulseek;
import dev.slsk.connection.ServerAddress;
import dev.slsk.download.DownloadRequest;
import dev.slsk.events.ConnectionEvent;
import dev.slsk.internal.EngineEvents.Kind;
import dev.slsk.internal.common.Monitors;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.connection.SoulseekClientState;
import dev.slsk.internal.diagnostics.DiagnosticSeverity;
import dev.slsk.internal.diagnostics.FilteringDiagnosticSink;
import dev.slsk.internal.events.EventBus;
import dev.slsk.internal.events.RoomJoinedEvent;
import dev.slsk.internal.network.ConnectionFactory;
import dev.slsk.internal.network.DefaultMessageConnection;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.MessageEvent;
import dev.slsk.internal.network.tcp.SocketConnector;
import dev.slsk.internal.network.tcp.SocketTransport;
import dev.slsk.internal.network.tcp.TransportConnection;
import dev.slsk.internal.network.tcp.TransportState;
import dev.slsk.internal.options.ConnectionOptions;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.search.SearchId;
import dev.slsk.search.SearchLimits;
import dev.slsk.search.SearchQuery;
import dev.slsk.search.SearchStatus;
import dev.slsk.share.SharedFolder;
import dev.slsk.spi.TransferSink;
import dev.slsk.transfer.TransferId;
import dev.slsk.transfer.TransferState;
import dev.slsk.user.BrowseRequest;
import dev.slsk.user.UserPresence;
import dev.slsk.user.Username;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/** The section 7.1 matrix over real public calls and real internal parks. */
class PublicBlockingMatrixTest {

    private static final Username BOB = Username.of("bob");
    private static final Duration LONG_DEADLINE = Duration.ofSeconds(30);
    private static final Duration SHORT_DEADLINE = Duration.ofMillis(100);
    private static final long PROMPT_MILLIS = 1_000;

    @TestFactory
    Stream<DynamicTest> everyBlockingFormConsumesItsObservedInterrupt() {
        return cases().stream()
                .flatMap(entry -> Stream.of(
                        DynamicTest.dynamicTest(entry.name() + " without deadline", () -> interrupt(entry, false)),
                        DynamicTest.dynamicTest(entry.name() + " with deadline", () -> interrupt(entry, true))));
    }

    @TestFactory
    Stream<DynamicTest> everyDeadlineExpiresWithTheSameOwnershipCleanup() {
        return cases().stream().map(entry -> DynamicTest.dynamicTest(entry.name(), () -> expire(entry)));
    }

    private static List<MatrixCase> cases() {
        return List.of(
                ask(
                        "connection.ping",
                        f -> f.ask(
                                () -> f.slsk.connection().ping(),
                                timeout -> f.slsk.connection().ping(timeout))),
                custom("search.await", FacetFixture::searchAwait),
                custom("search.run", FacetFixture::searchRun),
                custom("downloads.await", FacetFixture::downloadAwait),
                ask(
                        "users.info",
                        f -> f.ask(
                                () -> f.slsk.users().info(BOB),
                                timeout -> f.slsk.users().info(BOB, timeout))),
                ask(
                        "users.statistics",
                        f -> f.ask(
                                () -> f.slsk.users().statistics(BOB),
                                timeout -> f.slsk.users().statistics(BOB, timeout))),
                ask(
                        "users.status",
                        f -> f.ask(
                                () -> f.slsk.users().status(BOB),
                                timeout -> f.slsk.users().status(BOB, timeout))),
                ask(
                        "users.endpoint",
                        f -> f.ask(
                                () -> f.slsk.users().endpoint(BOB),
                                timeout -> f.slsk.users().endpoint(BOB, timeout))),
                ask(
                        "users.browse",
                        f -> f.ask(
                                () -> f.slsk.users().browse(BrowseRequest.of(BOB)),
                                timeout -> f.slsk.users().browse(BrowseRequest.of(BOB), timeout))),
                ask(
                        "users.directory",
                        f -> f.ask(
                                () -> f.slsk.users().directory(BOB, "music"),
                                timeout -> f.slsk.users().directory(BOB, "music", timeout))),
                ask(
                        "rooms.list",
                        f -> f.ask(
                                () -> f.slsk.rooms().list(),
                                timeout -> f.slsk.rooms().list(timeout))),
                ask(
                        "rooms.join",
                        f -> f.ask(
                                () -> f.slsk.rooms().join("matrix"),
                                timeout -> f.slsk.rooms().join("matrix", timeout))),
                custom("rooms.leave", f -> {
                    f.joinRoomLocally();
                    return f.ask(
                            () -> f.slsk.rooms().leave("matrix"),
                            timeout -> f.slsk.rooms().leave("matrix", timeout));
                }),
                delivery(
                        "rooms.say",
                        f -> f.delivery(
                                () -> f.slsk.rooms().say("matrix", "hello"),
                                timeout -> f.slsk.rooms().say("matrix", "hello", timeout))),
                delivery(
                        "rooms.setTicker",
                        f -> f.delivery(
                                () -> f.slsk.rooms().setTicker("matrix", "ticker"),
                                timeout -> f.slsk.rooms().setTicker("matrix", "ticker", timeout))),
                delivery(
                        "rooms.startPublicChat",
                        f -> f.delivery(
                                () -> f.slsk.rooms().startPublicChat(),
                                timeout -> f.slsk.rooms().startPublicChat(timeout))),
                delivery(
                        "rooms.stopPublicChat",
                        f -> f.delivery(
                                () -> f.slsk.rooms().stopPublicChat(),
                                timeout -> f.slsk.rooms().stopPublicChat(timeout))),
                ask(
                        "privateRooms.addMember",
                        f -> f.ask(
                                () -> f.slsk.rooms().privateRooms().addMember("matrix", BOB),
                                timeout -> f.slsk.rooms().privateRooms().addMember("matrix", BOB, timeout))),
                ask(
                        "privateRooms.removeMember",
                        f -> f.ask(
                                () -> f.slsk.rooms().privateRooms().removeMember("matrix", BOB),
                                timeout -> f.slsk.rooms().privateRooms().removeMember("matrix", BOB, timeout))),
                ask(
                        "privateRooms.addOperator",
                        f -> f.ask(
                                () -> f.slsk.rooms().privateRooms().addOperator("matrix", BOB),
                                timeout -> f.slsk.rooms().privateRooms().addOperator("matrix", BOB, timeout))),
                ask(
                        "privateRooms.removeOperator",
                        f -> f.ask(
                                () -> f.slsk.rooms().privateRooms().removeOperator("matrix", BOB),
                                timeout -> f.slsk.rooms().privateRooms().removeOperator("matrix", BOB, timeout))),
                ask(
                        "privateRooms.dropMembership",
                        f -> f.ask(
                                () -> f.slsk.rooms().privateRooms().dropMembership("matrix"),
                                timeout -> f.slsk.rooms().privateRooms().dropMembership("matrix", timeout))),
                ask(
                        "privateRooms.dropOwnership",
                        f -> f.ask(
                                () -> f.slsk.rooms().privateRooms().dropOwnership("matrix"),
                                timeout -> f.slsk.rooms().privateRooms().dropOwnership("matrix", timeout))),
                delivery(
                        "chat.send",
                        f -> f.delivery(
                                () -> f.slsk.chat().send(BOB, "hello"),
                                timeout -> f.slsk.chat().send(BOB, "hello", timeout))),
                custom("shares.rescan", FacetFixture::shareScan),
                delivery(
                        "me.presence",
                        f -> f.delivery(
                                () -> f.slsk.me().presence(UserPresence.AWAY),
                                timeout -> f.slsk.me().presence(UserPresence.AWAY, timeout))),
                ask(
                        "me.privileges",
                        f -> f.ask(
                                () -> f.slsk.me().privileges(),
                                timeout -> f.slsk.me().privileges(timeout))),
                delivery(
                        "me.giftPrivileges",
                        f -> f.delivery(
                                () -> f.slsk.me().giftPrivileges(BOB, 1),
                                timeout -> f.slsk.me().giftPrivileges(BOB, 1, timeout))),
                ask(
                        "me.changePassword",
                        f -> f.ask(
                                () -> f.slsk.me().changePassword("new-password"),
                                timeout -> f.slsk.me().changePassword("new-password", timeout))));
    }

    private static MatrixCase ask(String name, Function<FacetFixture, Scenario> scenario) {
        return new MatrixCase(name, scenario);
    }

    private static MatrixCase delivery(String name, Function<FacetFixture, Scenario> scenario) {
        return new MatrixCase(name, scenario);
    }

    private static MatrixCase custom(String name, ScenarioFactory scenario) {
        return new MatrixCase(name, fixture -> {
            try {
                return scenario.create(fixture);
            } catch (Exception failure) {
                throw new ScenarioCreationException(failure);
            }
        });
    }

    private static void interrupt(MatrixCase entry, boolean durationForm) throws Exception {
        try (FacetFixture fixture = new FacetFixture()) {
            Scenario scenario = entry.scenario().apply(fixture);
            Run run = Run.start(() -> {
                if (durationForm) {
                    scenario.timed().run(LONG_DEADLINE);
                } else {
                    scenario.plain().run();
                }
            });
            try {
                scenario.parked().await(run.thread());
                long started = System.nanoTime();
                run.thread().interrupt();
                boolean prompt = run.join(PROMPT_MILLIS);
                long elapsed = elapsedMillis(started);
                if (!prompt) {
                    scenario.unblock().run();
                    run.join(PROMPT_MILLIS);
                }
                assertTrue(prompt, entry.name() + " ignored interruption for " + elapsed + " ms");
                assertInstanceOf(InterruptedException.class, run.failure());
                assertFalse(
                        run.interruptObservedAfterCatch(),
                        "the single interrupt delivered to the parked invocation was not consumed");
                scenario.unblock().run();
                scenario.verify().run();
            } finally {
                scenario.unblock().run();
                scenario.cleanup().run();
            }
        }
    }

    private static void expire(MatrixCase entry) throws Exception {
        try (FacetFixture fixture = new FacetFixture()) {
            Scenario scenario = entry.scenario().apply(fixture);
            Run run = Run.start(() -> scenario.timed().run(SHORT_DEADLINE));
            try {
                scenario.parked().await(run.thread());
                boolean prompt = run.join(PROMPT_MILLIS);
                if (!prompt) {
                    scenario.unblock().run();
                    run.join(PROMPT_MILLIS);
                }
                assertTrue(
                        prompt,
                        entry.name() + " did not obey its caller deadline; after releasing the slow stage it "
                                + (run.failure() == null ? "returned normally" : "failed with " + run.failure()));
                assertInstanceOf(TimeoutException.class, run.failure());
                scenario.unblock().run();
                scenario.verify().run();
            } finally {
                scenario.unblock().run();
                scenario.cleanup().run();
            }
        }
    }

    @TestFactory
    Stream<DynamicTest> connectFormsAbandonTheirSocketForInterruptAndExpiry() {
        return Stream.of(false, true)
                .flatMap(addressForm -> Stream.of(
                        DynamicTest.dynamicTest(
                                connectName(addressForm, "interrupt"), () -> connect(addressForm, false)),
                        DynamicTest.dynamicTest(
                                connectName(addressForm, "duration interrupt"), () -> connect(addressForm, true)),
                        DynamicTest.dynamicTest(connectName(addressForm, "expiry"), () -> connectExpiry(addressForm))));
    }

    private static String connectName(boolean addressForm, String mode) {
        return "connection.connect" + (addressForm ? "(address) " : "() ") + mode;
    }

    private static void connect(boolean addressForm, boolean durationForm) throws Exception {
        try (ConnectFixture fixture = new ConnectFixture()) {
            Run run = Run.start(() -> fixture.connect(addressForm, durationForm ? LONG_DEADLINE : null));
            fixture.tcp.connectStarted.await(5, TimeUnit.SECONDS);
            long started = System.nanoTime();
            run.thread().interrupt();
            assertTrue(run.join(PROMPT_MILLIS), "connect did not abandon promptly");
            assertTrue(elapsedMillis(started) < PROMPT_MILLIS);
            assertInstanceOf(InterruptedException.class, run.failure());
            assertFalse(run.interruptObservedAfterCatch());
            fixture.assertAbandoned();
        }
    }

    private static void connectExpiry(boolean addressForm) throws Exception {
        try (ConnectFixture fixture = new ConnectFixture()) {
            Run run = Run.start(() -> fixture.connect(addressForm, SHORT_DEADLINE));
            fixture.tcp.connectStarted.await(5, TimeUnit.SECONDS);
            assertTrue(run.join(PROMPT_MILLIS), "connect did not expire promptly");
            assertInstanceOf(TimeoutException.class, run.failure());
            fixture.assertAbandoned();
        }
    }

    @Test
    void anInterruptOutsideTheLibraryLeavesTheConnectedClientUntouched() throws Exception {
        try (FacetFixture fixture = new FacetFixture()) {
            CountDownLatch parked = new CountDownLatch(1);
            Thread unrelated = Thread.ofVirtual().start(() -> {
                parked.countDown();
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException expected) {
                    // This thread owns no library invocation.
                }
            });
            assertTrue(parked.await(5, TimeUnit.SECONDS));
            unrelated.interrupt();
            unrelated.join();
            fixture.assertServerUsable();
        }
    }

    private record MatrixCase(String name, Function<FacetFixture, Scenario> scenario) {}

    private record Scenario(
            CheckedCall plain,
            TimedCall timed,
            Parked parked,
            CheckedCall verify,
            CheckedCall unblock,
            CheckedCall cleanup) {}

    @FunctionalInterface
    private interface ScenarioFactory {
        Scenario create(FacetFixture fixture) throws Exception;
    }

    @FunctionalInterface
    private interface CheckedCall {
        void run() throws Exception;
    }

    @FunctionalInterface
    private interface TimedCall {
        void run(Duration timeout) throws Exception;
    }

    @FunctionalInterface
    private interface Parked {
        void await(Thread caller) throws Exception;
    }

    private static final class ScenarioCreationException extends RuntimeException {
        private ScenarioCreationException(Throwable cause) {
            super(cause);
        }
    }

    private static final class Run {
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final AtomicBoolean interruptObservedAfterCatch = new AtomicBoolean();
        private final Thread thread;

        private Run(CheckedCall call) {
            thread = Thread.ofVirtual().start(() -> {
                try {
                    call.run();
                } catch (Throwable thrown) {
                    failure.set(thrown);
                    interruptObservedAfterCatch.set(Thread.interrupted());
                }
            });
        }

        static Run start(CheckedCall call) {
            return new Run(call);
        }

        boolean join(long millis) throws InterruptedException {
            thread.join(millis);
            return !thread.isAlive();
        }

        Thread thread() {
            return thread;
        }

        Throwable failure() {
            return failure.get();
        }

        boolean interruptObservedAfterCatch() {
            return interruptObservedAfterCatch.get();
        }
    }

    /** A logged-in public client over an actual connection writer and waiter. */
    private static final class FacetFixture implements AutoCloseable {
        private final ControlledStream stream = new ControlledStream();
        private final ControlledSocketConnector tcp = new ControlledSocketConnector(stream, false);
        private final DefaultMessageConnection server = new DefaultMessageConnection(
                new InetSocketAddress("127.0.0.1", 1), new ConnectionOptions(), Integer.BYTES, tcp, Monitors.shared());
        private final SoulseekEngine engine;
        private final Soulseek slsk;
        private final List<Path> temporaryPaths = new ArrayList<>();

        private FacetFixture() {
            try {
                server.connect(CancellationSignal.none());
            } catch (Exception impossible) {
                throw new AssertionError("the controlled transport connects synchronously", impossible);
            }
            engine = new SoulseekEngine(
                    9999,
                    SoulseekClientOptions.builder().enableListener(false).build(),
                    server,
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
                    null,
                    null);
            engine.setStateForTest(SoulseekClientState.LOGGED_IN);
            slsk = DefaultSoulseek.over(engine, "alice", "password");
        }

        private Scenario ask(CheckedCall plain, TimedCall timed) {
            int baseline = stream.writeCount();
            return new Scenario(
                    plain,
                    timed,
                    caller -> {
                        stream.awaitWriteCount(baseline + 1);
                        awaitParked(caller);
                    },
                    this::assertServerUsable,
                    () -> {},
                    () -> {});
        }

        private Scenario delivery(CheckedCall plain, TimedCall timed) {
            ControlledStream.WriteGate gate = stream.stallNextWrite();
            return new Scenario(
                    plain,
                    timed,
                    caller -> {
                        assertTrue(gate.started.await(5, TimeUnit.SECONDS), "the frame writer never started");
                        awaitParked(caller);
                    },
                    () -> {
                        assertTrue(gate.finished.await(5, TimeUnit.SECONDS), "the detached frame never finished");
                        assertServerUsable();
                    },
                    gate::release,
                    gate::release);
        }

        private Scenario searchRun() {
            int baseline = stream.writeCount();
            return new Scenario(
                    () -> slsk.search().run(query()),
                    timeout -> slsk.search().run(query(), timeout),
                    caller -> {
                        stream.awaitWriteCount(baseline + 1);
                        awaitParked(caller);
                    },
                    () -> {
                        assertTrue(slsk.search().active().isEmpty(), "interrupted run left its owned search active");
                        assertServerUsable();
                    },
                    () -> {},
                    () -> slsk.search().active().forEach(search -> slsk.search().stop(search.id())));
        }

        private Scenario searchAwait() throws Exception {
            int baseline = stream.writeCount();
            SearchId id = slsk.search().start(query());
            stream.awaitWriteCount(baseline + 1);
            return new Scenario(
                    () -> slsk.search().await(id),
                    timeout -> slsk.search().await(id, timeout),
                    PublicBlockingMatrixTest::awaitParked,
                    () -> {
                        assertEquals(
                                SearchStatus.IN_PROGRESS,
                                slsk.search().get(id).status(),
                                "await cancelled a search it did not own");
                        assertServerUsable();
                    },
                    () -> {},
                    () -> slsk.search().stop(id));
        }

        private Scenario downloadAwait() throws Exception {
            TransferSink sink = new TransferSink() {
                @Override
                public WritableByteChannel open(long resumeOffset) {
                    return new WritableByteChannel() {
                        private boolean open = true;

                        @Override
                        public int write(ByteBuffer source) {
                            int count = source.remaining();
                            source.position(source.limit());
                            return count;
                        }

                        @Override
                        public boolean isOpen() {
                            return open;
                        }

                        @Override
                        public void close() {
                            open = false;
                        }
                    };
                }

                @Override
                public void commit() {}

                @Override
                public void discard() {}
            };
            TransferId id = slsk.downloads().enqueue(DownloadRequest.of(BOB, "music\\song.mp3", sink));
            slsk.downloads().pause(id);
            awaitCondition(() -> slsk.downloads().get(id).state() instanceof TransferState.Paused);
            return new Scenario(
                    () -> slsk.downloads().await(id),
                    timeout -> slsk.downloads().await(id, timeout),
                    PublicBlockingMatrixTest::awaitParked,
                    () -> assertTrue(
                            slsk.downloads().get(id).state() instanceof TransferState.Paused,
                            "await cancelled the independently owned download"),
                    () -> {},
                    () -> slsk.downloads().cancel(id));
        }

        private Scenario shareScan() throws Exception {
            Path directory = Files.createTempDirectory("slsk-cancellation-matrix-");
            Path file = Files.writeString(directory.resolve("song.mp3"), "audio");
            temporaryPaths.add(file);
            temporaryPaths.add(directory);
            slsk.shares().configure(List.of(SharedFolder.of(directory)));
            return delivery(
                    () -> slsk.shares().rescan(), timeout -> slsk.shares().rescan(timeout));
        }

        private void joinRoomLocally() {
            engine.events().publish(Kind.ROOM_JOINED, new RoomJoinedEvent("matrix", "alice", null));
            assertEquals("matrix", slsk.rooms().get("matrix").name());
        }

        private void assertServerUsable() throws Exception {
            assertEquals(TransportState.CONNECTED, server.getState(), "the shared server connection was lost");
            int before = stream.writeCount();
            slsk.chat().send(BOB, "probe");
            stream.awaitWriteCount(before + 1);
            assertEquals(TransportState.CONNECTED, server.getState(), "the shared server connection was not reusable");
        }

        @Override
        public void close() throws Exception {
            stream.releaseAll();
            slsk.close();
            for (Path path : temporaryPaths) {
                Files.deleteIfExists(path);
            }
        }
    }

    /** One public connect attempt whose transport cannot finish until closed. */
    private static final class ConnectFixture implements AutoCloseable {
        private final ControlledStream stream = new ControlledStream();
        private final ControlledSocketConnector tcp = new ControlledSocketConnector(stream, true);
        private final DefaultMessageConnection server = new DefaultMessageConnection(
                new InetSocketAddress("127.0.0.1", 1), new ConnectionOptions(), Integer.BYTES, tcp, Monitors.shared());
        private final SoulseekEngine engine;
        private final EventBus<ConnectionEvent> events =
                new EventBus<>("connect-matrix", new FilteringDiagnosticSink(DiagnosticSeverity.NONE, ignored -> {}));
        private final DefaultConnection connection;

        private ConnectFixture() {
            ConnectionFactory factory = new FixedConnectionFactory(server);
            engine = new SoulseekEngine(
                    9999,
                    SoulseekClientOptions.builder().enableListener(false).build(),
                    null,
                    factory,
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
                    null);
            connection = new DefaultConnection(
                    engine,
                    new DefaultConnection.Credentials("alice", "password"),
                    events,
                    ServerAddress.of("127.0.0.1", 1));
        }

        private void connect(boolean addressForm, Duration timeout) throws Exception {
            ServerAddress address = ServerAddress.of("127.0.0.1", 1);
            if (addressForm && timeout == null) {
                connection.connect(address);
            } else if (addressForm) {
                connection.connect(address, timeout);
            } else if (timeout == null) {
                connection.connect();
            } else {
                connection.connect(timeout);
            }
        }

        private void assertAbandoned() throws Exception {
            assertTrue(tcp.closed.get(), "the abandoned connect socket stayed open");
            assertTrue(tcp.connectExited.await(5, TimeUnit.SECONDS), "the transport connect worker was orphaned");
            assertEquals(TransportState.DISCONNECTED, server.getState());
            assertFalse(engine.getState() == SoulseekClientState.CONNECTING);
        }

        @Override
        public void close() {
            connection.close();
            engine.close();
            events.close();
        }
    }

    private static final class FixedConnectionFactory implements ConnectionFactory {
        private final MessageConnection connection;

        private FixedConnectionFactory(MessageConnection connection) {
            this.connection = connection;
        }

        @Override
        public MessageConnection getDistributedConnection(
                String username, InetSocketAddress endpoint, ConnectionOptions options, SocketConnector client) {
            return connection;
        }

        @Override
        public MessageConnection getMessageConnection(
                String username, InetSocketAddress endpoint, ConnectionOptions options, SocketConnector client) {
            return connection;
        }

        @Override
        public MessageConnection getServerConnection(
                InetSocketAddress endpoint,
                java.util.function.Consumer<TransportConnection> connected,
                java.util.function.Consumer<dev.slsk.internal.network.tcp.ConnectionDisconnectedEvent> disconnected,
                java.util.function.Consumer<MessageEvent> read,
                java.util.function.Consumer<MessageEvent> written,
                ConnectionOptions options,
                SocketConnector client) {
            return connection;
        }

        @Override
        public dev.slsk.internal.network.tcp.TransportConnection getTransferConnection(
                InetSocketAddress endpoint, ConnectionOptions options, SocketConnector client) {
            return connection;
        }
    }

    /** A transport with deterministic connect and frame-write gates. */
    private static final class ControlledSocketConnector implements SocketConnector {
        private final ControlledStream stream;
        private final boolean blockConnect;
        private final Socket socket = new Socket();
        private final CountDownLatch connectStarted = new CountDownLatch(1);
        private final CountDownLatch closeGate = new CountDownLatch(1);
        private final CountDownLatch connectExited = new CountDownLatch(1);
        private final AtomicBoolean closed = new AtomicBoolean();
        private volatile boolean connected;

        private ControlledSocketConnector(ControlledStream stream, boolean blockConnect) {
            this.stream = stream;
            this.blockConnect = blockConnect;
        }

        @Override
        public Socket socket() {
            return socket;
        }

        @Override
        public boolean isConnected() {
            return connected && !closed.get();
        }

        @Override
        public InetSocketAddress getRemoteEndpoint() {
            return new InetSocketAddress("127.0.0.1", 1);
        }

        @Override
        public void connect(InetAddress address, int port) {
            connectStarted.countDown();
            try {
                if (blockConnect) {
                    closeGate.await();
                }
                if (closed.get()) {
                    throw new java.util.concurrent.CancellationException("connect socket closed");
                }
                connected = true;
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("the library interrupted its socket worker", interrupted);
            } finally {
                connectExited.countDown();
            }
        }

        @Override
        public ProxyEndpoint connectThroughProxy(
                InetAddress proxyAddress,
                int proxyPort,
                InetAddress destinationAddress,
                int destinationPort,
                String username,
                String password,
                CancellationSignal signal) {
            connect(destinationAddress, destinationPort);
            return new ProxyEndpoint("127.0.0.1", proxyPort);
        }

        @Override
        public SocketTransport transport() {
            return stream;
        }

        @Override
        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                connected = false;
                closeGate.countDown();
                stream.close();
                socket.close();
            }
        }
    }

    private static final class ControlledStream implements SocketTransport {
        private final AtomicInteger writes = new AtomicInteger();
        private final AtomicReference<WriteGate> nextGate = new AtomicReference<>();
        private final List<WriteGate> gates = new ArrayList<>();
        private final CountDownLatch closed = new CountDownLatch(1);

        @Override
        public int read(byte[] buffer, int offset, int size) {
            try {
                closed.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("the library interrupted its socket reader", interrupted);
            }
            return 0;
        }

        @Override
        public void write(byte[] buffer, int offset, int size) throws IOException {
            writes.incrementAndGet();
            WriteGate gate = nextGate.getAndSet(null);
            if (gate == null) {
                return;
            }
            gate.started.countDown();
            try {
                gate.released.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("the library interrupted its frame writer", interrupted);
            } finally {
                gate.finished.countDown();
            }
        }

        private synchronized WriteGate stallNextWrite() {
            WriteGate gate = new WriteGate();
            if (!nextGate.compareAndSet(null, gate)) {
                throw new IllegalStateException("a write gate is already armed");
            }
            gates.add(gate);
            return gate;
        }

        private int writeCount() {
            return writes.get();
        }

        private void awaitWriteCount(int expected) throws Exception {
            awaitCondition(() -> writes.get() >= expected);
        }

        private synchronized void releaseAll() {
            for (WriteGate gate : gates) {
                gate.release();
            }
        }

        @Override
        public void close() {
            releaseAll();
            closed.countDown();
        }

        private static final class WriteGate {
            private final CountDownLatch started = new CountDownLatch(1);
            private final CountDownLatch released = new CountDownLatch(1);
            private final CountDownLatch finished = new CountDownLatch(1);

            private void release() {
                released.countDown();
            }
        }
    }

    private static SearchQuery query() {
        Duration duration = Duration.ofMinutes(5);
        return SearchQuery.of("matrix").withLimits(new SearchLimits(duration, duration, 250, 250));
    }

    private static void awaitParked(Thread thread) throws Exception {
        awaitCondition(
                () -> thread.getState() == Thread.State.WAITING || thread.getState() == Thread.State.TIMED_WAITING);
    }

    private static void awaitCondition(java.util.function.BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("the invocation never reached its real blocking point");
            }
            Thread.onSpinWait();
        }
    }

    private static long elapsedMillis(long started) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
    }
}
