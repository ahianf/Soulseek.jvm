// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.exceptions.ConnectionException;
import dev.slsk.internal.ServerLink;
import dev.slsk.internal.ServerLinks;
import dev.slsk.internal.common.Constants;
import dev.slsk.internal.common.Outcomes;
import dev.slsk.internal.common.TokenFactory;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.diagnostics.DiagnosticEventListener;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.messaging.handlers.PeerMessageHandler;
import dev.slsk.internal.messaging.messages.ConnectToPeerRequest;
import dev.slsk.internal.messaging.messages.ConnectToPeerResponse;
import dev.slsk.internal.messaging.messages.OutgoingMessage;
import dev.slsk.internal.messaging.messages.PeerInit;
import dev.slsk.internal.messaging.messages.PierceFirewall;
import dev.slsk.internal.network.tcp.Connection;
import dev.slsk.internal.network.tcp.ConnectionDisconnectedEvent;
import dev.slsk.internal.network.tcp.ConnectionEventListener;
import dev.slsk.internal.network.tcp.ConnectionKey;
import dev.slsk.internal.network.tcp.ConnectionState;
import dev.slsk.internal.network.tcp.ConnectionTypes;
import dev.slsk.internal.network.tcp.TcpClient;
import dev.slsk.internal.options.ConnectionOptions;
import dev.slsk.internal.options.SoulseekClientOptions;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PeerNetworkTest {
    private static final InetSocketAddress DIRECT_ENDPOINT = endpoint(41001);
    private static final InetSocketAddress INDIRECT_ENDPOINT = endpoint(41002);
    private static final String LOCAL_USER = "local";
    private static final String USERNAME = "peer";
    private static final int TOKEN = 0x12345678;

    @Test
    void constructionDiagnosticsAndClosePreserveSourceLifecycle() {
        Fixture nulls = new Fixture();
        assertThrows(
                NullPointerException.class,
                () -> new PeerNetwork(
                        null, nulls.server, nulls.waiter, nulls.tokens, nulls.peerMessages, nulls.factory));

        Fixture fixture = new Fixture();
        PeerNetwork manager = fixture.manager();
        AtomicInteger events = new AtomicInteger();
        DiagnosticEventListener listener = (sender, args) -> events.incrementAndGet();
        manager.addDiagnosticGeneratedListener(listener);

        // The supplied diagnostic remains usable and close is idempotent.
        fixture.diagnostic.info("test");
        assertEquals(0, events.get());
        assertTrue(manager.getMessageConnections().isEmpty());
        assertTrue(manager.getPendingSolicitations().isEmpty());
        assertDoesNotThrow(manager::close);
        assertDoesNotThrow(manager::close);

        PeerNetwork defaultDiagnostic = new PeerNetwork(
                fixture.options, fixture.server, fixture.waiter, fixture.tokens, fixture.peerMessages, fixture.factory);
        defaultDiagnostic.addDiagnosticGeneratedListener(listener);
        // The default factory is covered through a debug-producing failure.
        defaultDiagnostic.getCachedMessageConnection("missing");
        defaultDiagnostic.close();
    }

    @Test
    void incomingMessageConnectionIsHandedOffStartedAndCached() {
        Fixture fixture = new Fixture();
        ConnectionProbe incoming = ConnectionProbe.connection(DIRECT_ENDPOINT);
        ConnectionProbe message = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        fixture.factory.messageHandoff = message;

        fixture.manager().addOrUpdateMessageConnection(USERNAME, incoming.connection());

        assertEquals(1, incoming.handoffCount);
        assertEquals(1, incoming.closeCount);
        assertEquals(1, message.startReadingCount);
        assertEquals(ConnectionTypes.INBOUND.or(ConnectionTypes.DIRECT), message.type);
        assertSame(message.messageConnection(), fixture.manager().getCachedMessageConnection(USERNAME));
        assertEquals(
                List.of(new PeerEndpoint(USERNAME, DIRECT_ENDPOINT)),
                fixture.manager().getMessageConnections());
        assertEquals(1, message.messageReadListeners.size());
        assertEquals(1, message.messageReceivedListeners.size());
        assertEquals(1, message.messageWrittenListeners.size());
    }

    @Test
    void incomingMessageSupersedesWithoutDisposingOldConnection() {
        Fixture fixture = new Fixture();
        ConnectionProbe first = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        ConnectionProbe second = ConnectionProbe.message(USERNAME, INDIRECT_ENDPOINT);
        fixture.factory.messageHandoff = first;
        fixture.manager()
                .addOrUpdateMessageConnection(
                        USERNAME, ConnectionProbe.connection(DIRECT_ENDPOINT).connection());

        fixture.factory.messageHandoff = second;
        fixture.manager()
                .addOrUpdateMessageConnection(
                        USERNAME, ConnectionProbe.connection(INDIRECT_ENDPOINT).connection());

        assertEquals(0, first.closeCount);
        assertSame(second.messageConnection(), fixture.manager().getCachedMessageConnection(USERNAME));
        assertTrue(fixture.diagnostic.contains("Superseding cached"));
    }

    @Test
    void incomingMessageStartFailureClosesAndPurgesCache() {
        Fixture fixture = new Fixture();
        ConnectionProbe message = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        RuntimeException expected = new RuntimeException("start failed");
        message.startFailure = expected;
        fixture.factory.messageHandoff = message;

        CompletionException thrown = assertThrows(
                CompletionException.class,
                () -> fixture.manager()
                        .addOrUpdateMessageConnection(
                                USERNAME,
                                ConnectionProbe.connection(DIRECT_ENDPOINT).connection()));

        ConnectionException mapped = assertInstanceOf(ConnectionException.class, thrown.getCause());
        assertSame(expected, mapped.getCause());
        assertEquals(1, message.closeCount);
        assertNull(fixture.manager().getCachedMessageConnection(USERNAME));
        assertTrue(fixture.diagnostic.contains("Purging message connection cache"));
    }

    @Test
    void acceptedTransferReadsLittleEndianRemoteToken() {
        Fixture fixture = new Fixture();
        ConnectionProbe incoming = ConnectionProbe.connection(DIRECT_ENDPOINT);
        ConnectionProbe transfer = ConnectionProbe.connection(DIRECT_ENDPOINT);
        transfer.readFuture = CompletableFuture.completedFuture(littleEndian(TOKEN));
        fixture.factory.transferHandoff = transfer;

        TransferConnectionResult result = fixture.manager().getTransferConnection(USERNAME, 91, incoming.connection());

        assertSame(transfer.connection(), result.connection());
        assertEquals(TOKEN, result.remoteToken());
        assertEquals(1, incoming.handoffCount);
        assertEquals(ConnectionTypes.INBOUND.or(ConnectionTypes.DIRECT), transfer.type);
    }

    @Test
    void acceptedTransferReadFailureClosesAndMapsException() {
        Fixture fixture = new Fixture();
        ConnectionProbe transfer = ConnectionProbe.connection(DIRECT_ENDPOINT);
        IllegalStateException expected = new IllegalStateException("read");
        transfer.readFuture = CompletableFuture.failedFuture(expected);
        fixture.factory.transferHandoff = transfer;

        CompletionException thrown = assertThrows(
                CompletionException.class,
                () -> fixture.manager()
                        .getTransferConnection(
                                USERNAME,
                                TOKEN,
                                ConnectionProbe.connection(DIRECT_ENDPOINT).connection()));

        ConnectionException mapped = assertInstanceOf(ConnectionException.class, thrown.getCause());
        assertSame(expected, mapped.getCause());
        assertEquals(1, transfer.closeCount);
    }

    @Test
    void connectToPeerTransferConnectsPiercesAndReadsToken() {
        Fixture fixture = new Fixture();
        ConnectionProbe transfer = ConnectionProbe.connection(INDIRECT_ENDPOINT);
        transfer.readFuture = CompletableFuture.completedFuture(littleEndian(TOKEN));
        fixture.factory.transferDirect = transfer;
        ConnectToPeerResponse response =
                new ConnectToPeerResponse(USERNAME, Constants.ConnectionType.TRANSFER, INDIRECT_ENDPOINT, 77, true);

        TransferConnectionResult result = fixture.manager().getTransferConnection(response);

        assertSame(transfer.connection(), result.connection());
        assertEquals(TOKEN, result.remoteToken());
        assertEquals(1, transfer.connectCount);
        assertArrayEquals(new PierceFirewall(77).toByteArray(), transfer.byteWrites.getFirst());
        assertEquals(ConnectionTypes.INBOUND.or(ConnectionTypes.INDIRECT), transfer.type);
    }

    @Test
    void connectToPeerTransferFailureClosesAndMapsException() {
        Fixture fixture = new Fixture();
        ConnectionProbe transfer = ConnectionProbe.connection(INDIRECT_ENDPOINT);
        IllegalStateException expected = new IllegalStateException("connect");
        transfer.connectFuture = CompletableFuture.failedFuture(expected);
        fixture.factory.transferDirect = transfer;

        CompletionException thrown = assertThrows(
                CompletionException.class,
                () -> fixture.manager()
                        .getTransferConnection(new ConnectToPeerResponse(
                                USERNAME, Constants.ConnectionType.TRANSFER, INDIRECT_ENDPOINT, TOKEN, false)));

        ConnectionException mapped = assertInstanceOf(ConnectionException.class, thrown.getCause());
        assertSame(expected, mapped.getCause());
        assertEquals(1, transfer.closeCount);
    }

    @Test
    void outboundTransferUsesDirectWinnerAndWritesInitializationAndToken() {
        Fixture fixture = new Fixture();
        ConnectionProbe direct = ConnectionProbe.connection(DIRECT_ENDPOINT);
        fixture.factory.transferDirect = direct;
        fixture.waiter.defaultFuture = CompletableFuture.failedFuture(new RuntimeException("indirect"));

        Connection result =
                fixture.manager().getTransferConnection(USERNAME, DIRECT_ENDPOINT, TOKEN, CancellationSignal.none());

        assertSame(direct.connection(), result);
        assertEquals(ConnectionTypes.OUTBOUND.or(ConnectionTypes.DIRECT), direct.type);
        assertEquals(2, direct.byteWrites.size());
        assertArrayEquals(
                new PeerInit(LOCAL_USER, Constants.ConnectionType.TRANSFER, TOKEN).toByteArray(),
                direct.byteWrites.get(0));
        assertArrayEquals(littleEndian(TOKEN), direct.byteWrites.get(1));
        assertTrue(fixture.diagnostic.contains("established first"));
    }

    @Test
    void outboundTransferUsesIndirectWinnerAndSolicitationHandoff() {
        Fixture fixture = new Fixture();
        ConnectionProbe direct = ConnectionProbe.connection(DIRECT_ENDPOINT);
        direct.connectFuture = CompletableFuture.failedFuture(new RuntimeException("direct"));
        ConnectionProbe accepted = ConnectionProbe.connection(INDIRECT_ENDPOINT);
        ConnectionProbe indirect = ConnectionProbe.connection(INDIRECT_ENDPOINT);
        fixture.factory.transferDirect = direct;
        fixture.factory.transferHandoff = indirect;
        fixture.waiter.defaultFuture = CompletableFuture.completedFuture(accepted.connection());

        Connection result =
                fixture.manager().getTransferConnection(USERNAME, DIRECT_ENDPOINT, TOKEN, CancellationSignal.none());

        assertSame(indirect.connection(), result);
        assertEquals(ConnectionTypes.OUTBOUND.or(ConnectionTypes.INDIRECT), indirect.type);
        assertEquals(1, accepted.handoffCount);
        assertEquals(1, accepted.closeCount);
        assertEquals(1, indirect.byteWrites.size());
        assertArrayEquals(littleEndian(TOKEN), indirect.byteWrites.getFirst());
        ConnectToPeerRequest request =
                assertInstanceOf(ConnectToPeerRequest.class, fixture.serverConnection.outgoingWrites.getFirst());
        assertEquals(Constants.ConnectionType.TRANSFER, request.getType());
        assertTrue(fixture.manager().getPendingSolicitations().isEmpty());
    }

    @Test
    void outboundTransferBothFailuresProduceSourceConnectionException() {
        Fixture fixture = new Fixture();
        ConnectionProbe direct = ConnectionProbe.connection(DIRECT_ENDPOINT);
        direct.connectFuture = CompletableFuture.failedFuture(new RuntimeException("direct"));
        fixture.factory.transferDirect = direct;
        fixture.waiter.defaultFuture = CompletableFuture.failedFuture(new RuntimeException("indirect"));

        CompletionException thrown = assertThrows(
                CompletionException.class,
                () -> fixture.manager()
                        .getTransferConnection(USERNAME, DIRECT_ENDPOINT, TOKEN, CancellationSignal.none()));

        assertTrue(assertInstanceOf(ConnectionException.class, thrown.getCause())
                .getMessage()
                .contains("direct or indirect transfer connection"));
    }

    @Test
    void connectToPeerMessageConnectsPiercesAndCaches() {
        Fixture fixture = new Fixture();
        ConnectionProbe message = ConnectionProbe.message(USERNAME, INDIRECT_ENDPOINT);
        fixture.factory.messageDirect = message;
        ConnectToPeerResponse response =
                new ConnectToPeerResponse(USERNAME, Constants.ConnectionType.PEER, INDIRECT_ENDPOINT, TOKEN, false);

        MessageConnection result = fixture.manager().getOrAddMessageConnection(response);
        MessageConnection cached = fixture.manager().getOrAddMessageConnection(response);

        assertSame(message.messageConnection(), result);
        assertSame(result, cached);
        assertEquals(1, message.connectCount);
        assertArrayEquals(new PierceFirewall(TOKEN).toByteArray(), message.byteWrites.getFirst());
        assertEquals(ConnectionTypes.INBOUND.or(ConnectionTypes.INDIRECT), message.type);
    }

    @Test
    void connectToPeerMessageFailureClosesPurgesAndMaps() {
        Fixture fixture = new Fixture();
        ConnectionProbe message = ConnectionProbe.message(USERNAME, INDIRECT_ENDPOINT);
        IllegalStateException expected = new IllegalStateException("connect");
        message.connectFuture = CompletableFuture.failedFuture(expected);
        fixture.factory.messageDirect = message;

        CompletionException thrown = assertThrows(
                CompletionException.class,
                () -> fixture.manager()
                        .getOrAddMessageConnection(new ConnectToPeerResponse(
                                USERNAME, Constants.ConnectionType.PEER, INDIRECT_ENDPOINT, TOKEN, false)));

        ConnectionException mapped = assertInstanceOf(ConnectionException.class, thrown.getCause());
        assertSame(expected, mapped.getCause());
        assertEquals(1, message.closeCount);
        assertNull(fixture.manager().getCachedMessageConnection(USERNAME));
    }

    @Test
    void outboundMessageUsesDirectWinnerAndWritesPeerInit() {
        Fixture fixture = new Fixture();
        ConnectionProbe direct = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        fixture.factory.messageDirect = direct;
        fixture.waiter.defaultFuture = CompletableFuture.failedFuture(new RuntimeException("indirect"));

        MessageConnection result = fixture.manager()
                .getOrAddMessageConnection(USERNAME, DIRECT_ENDPOINT, TOKEN, CancellationSignal.none());

        assertSame(direct.messageConnection(), result);
        assertEquals(ConnectionTypes.OUTBOUND.or(ConnectionTypes.DIRECT), direct.type);
        assertArrayEquals(
                new PeerInit(LOCAL_USER, Constants.ConnectionType.PEER, TOKEN).toByteArray(),
                direct.byteWrites.getFirst());
        assertEquals(0, direct.startReadingCount);
    }

    @Test
    void outboundMessageUsesIndirectWinnerAndStartsReading() {
        Fixture fixture = new Fixture();
        ConnectionProbe direct = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        direct.connectFuture = CompletableFuture.failedFuture(new RuntimeException("direct"));
        ConnectionProbe accepted = ConnectionProbe.connection(INDIRECT_ENDPOINT);
        ConnectionProbe indirect = ConnectionProbe.message(USERNAME, INDIRECT_ENDPOINT);
        fixture.factory.messageDirect = direct;
        fixture.factory.messageHandoff = indirect;
        fixture.waiter.defaultFuture = CompletableFuture.completedFuture(accepted.connection());

        MessageConnection result = fixture.manager()
                .getOrAddMessageConnection(USERNAME, DIRECT_ENDPOINT, TOKEN, CancellationSignal.none());

        assertSame(indirect.messageConnection(), result);
        assertEquals(ConnectionTypes.OUTBOUND.or(ConnectionTypes.INDIRECT), indirect.type);
        assertEquals(1, indirect.startReadingCount);
        assertEquals(0, indirect.byteWrites.size());
        assertTrue(fixture.manager().getPendingSolicitations().isEmpty());
        assertEquals(
                Constants.ConnectionType.PEER,
                assertInstanceOf(ConnectToPeerRequest.class, fixture.serverConnection.outgoingWrites.getFirst())
                        .getType());
    }

    @Test
    void outboundMessageFailuresPurgeCacheAndMapException() {
        Fixture fixture = new Fixture();
        ConnectionProbe direct = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        direct.connectFuture = CompletableFuture.failedFuture(new RuntimeException("direct"));
        fixture.factory.messageDirect = direct;
        fixture.waiter.defaultFuture = CompletableFuture.failedFuture(new RuntimeException("indirect"));

        CompletionException thrown = assertThrows(
                CompletionException.class,
                () -> fixture.manager()
                        .getOrAddMessageConnection(USERNAME, DIRECT_ENDPOINT, TOKEN, CancellationSignal.none()));

        assertInstanceOf(ConnectionException.class, thrown.getCause());
        assertNull(fixture.manager().getCachedMessageConnection(USERNAME));
        assertTrue(fixture.diagnostic.contains("Purging message connection cache"));
    }

    @Test
    void messageNegotiationFailureClosesAndPreservesCause() {
        Fixture fixture = new Fixture();
        ConnectionProbe direct = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        IllegalStateException expected = new IllegalStateException("write");
        direct.writeFuture = CompletableFuture.failedFuture(expected);
        fixture.factory.messageDirect = direct;
        fixture.waiter.defaultFuture = CompletableFuture.failedFuture(new RuntimeException("indirect"));

        CompletionException thrown = assertThrows(
                CompletionException.class,
                () -> fixture.manager()
                        .getOrAddMessageConnection(USERNAME, DIRECT_ENDPOINT, TOKEN, CancellationSignal.none()));

        ConnectionException mapped = assertInstanceOf(ConnectionException.class, thrown.getCause());
        assertSame(expected, mapped.getCause());
        assertEquals(1, direct.closeCount);
    }

    /**
     * The property the cache exists for, and the one the cell has to keep: the
     * first caller to ask for a peer establishes the connection, and everyone
     * who asks while that is still in flight gets the same connection rather
     * than a second socket to the same peer.
     */
    @Test
    @org.junit.jupiter.api.Timeout(
            value = 10,
            unit = java.util.concurrent.TimeUnit.SECONDS,
            threadMode = org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD)
    void concurrentMessageRequestsShareOneInFlightConnection() {
        Fixture fixture = new Fixture();
        ConnectionProbe direct = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        CompletableFuture<Void> connect = new CompletableFuture<>();
        direct.connectFuture = connect;
        fixture.factory.messageDirect = direct;
        fixture.waiter.defaultFuture = CompletableFuture.failedFuture(new RuntimeException("indirect"));

        // Every caller blocks, so each needs a thread of its own; the cache
        // still has to give them one connection between them.
        List<CompletableFuture<MessageConnection>> callers = requestConcurrently(fixture, 8);
        connect.complete(null);

        for (CompletableFuture<MessageConnection> caller : callers) {
            assertSame(direct.messageConnection(), caller.join());
        }
        assertEquals(1, fixture.factory.messageDirectCount);
    }

    /** The other half of the same property: one failure, seen by everyone. */
    @Test
    @org.junit.jupiter.api.Timeout(
            value = 10,
            unit = java.util.concurrent.TimeUnit.SECONDS,
            threadMode = org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD)
    void aFailedEstablishmentIsRaisedToEveryConcurrentRequest() {
        Fixture fixture = new Fixture();
        ConnectionProbe direct = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        CompletableFuture<Void> connect = new CompletableFuture<>();
        direct.connectFuture = connect;
        fixture.factory.messageDirect = direct;
        fixture.waiter.defaultFuture = CompletableFuture.failedFuture(new RuntimeException("indirect"));

        List<CompletableFuture<MessageConnection>> callers = requestConcurrently(fixture, 8);
        connect.completeExceptionally(new ConnectionException("no route to host"));

        for (CompletableFuture<MessageConnection> caller : callers) {
            CompletionException thrown = assertThrows(CompletionException.class, caller::join);
            assertInstanceOf(ConnectionException.class, unwrap(thrown));
        }
        assertEquals(1, fixture.factory.messageDirectCount);
        assertNull(fixture.manager().getCachedMessageConnection(USERNAME));
    }

    private static List<CompletableFuture<MessageConnection>> requestConcurrently(Fixture fixture, int callers) {
        java.util.concurrent.Executor threads = task -> Thread.ofVirtual().start(task);
        List<CompletableFuture<MessageConnection>> requests = new ArrayList<>();
        for (int caller = 0; caller < callers; caller++) {
            int solicitationToken = TOKEN + caller;
            requests.add(CompletableFuture.supplyAsync(
                    () -> fixture.manager()
                            .getOrAddMessageConnection(
                                    USERNAME, DIRECT_ENDPOINT, solicitationToken, CancellationSignal.none()),
                    threads));
        }
        return requests;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    @Test
    void awaitingTransferReturnsEitherWinnerAndMapsDualFailure() {
        Fixture directFixture = new Fixture();
        ConnectionProbe direct = ConnectionProbe.connection(DIRECT_ENDPOINT);
        directFixture.waiter.put(
                new WaitKey(Constants.WaitKey.DIRECT_TRANSFER, USERNAME, TOKEN),
                CompletableFuture.completedFuture(direct.connection()));
        directFixture.waiter.put(
                new WaitKey(Constants.WaitKey.INDIRECT_TRANSFER, USERNAME, "file", TOKEN),
                CompletableFuture.failedFuture(new RuntimeException()));
        assertSame(
                direct.connection(),
                directFixture.manager().awaitTransferConnection(USERNAME, "file", TOKEN, CancellationSignal.none()));

        Fixture indirectFixture = new Fixture();
        ConnectionProbe indirect = ConnectionProbe.connection(INDIRECT_ENDPOINT);
        indirectFixture.waiter.put(
                new WaitKey(Constants.WaitKey.DIRECT_TRANSFER, USERNAME, TOKEN),
                CompletableFuture.failedFuture(new RuntimeException()));
        indirectFixture.waiter.put(
                new WaitKey(Constants.WaitKey.INDIRECT_TRANSFER, USERNAME, "file", TOKEN),
                CompletableFuture.completedFuture(indirect.connection()));
        assertSame(
                indirect.connection(),
                indirectFixture.manager().awaitTransferConnection(USERNAME, "file", TOKEN, CancellationSignal.none()));

        Fixture failedFixture = new Fixture();
        failedFixture.waiter.defaultFuture = CompletableFuture.failedFuture(new RuntimeException());
        CompletionException thrown = assertThrows(
                CompletionException.class,
                () -> failedFixture
                        .manager()
                        .awaitTransferConnection(USERNAME, "file", TOKEN, CancellationSignal.none()));
        assertInstanceOf(ConnectionException.class, thrown.getCause());
    }

    @Test
    void invalidationDisconnectAndRemoveAllCleanUpCache() {
        Fixture fixture = new Fixture();
        ConnectionProbe first = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        fixture.factory.messageHandoff = first;
        fixture.manager()
                .addOrUpdateMessageConnection(
                        USERNAME, ConnectionProbe.connection(DIRECT_ENDPOINT).connection());
        assertTrue(fixture.manager().tryInvalidateMessageConnectionCache(USERNAME));
        assertFalse(fixture.manager().tryInvalidateMessageConnectionCache(USERNAME));

        ConnectionProbe second = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        fixture.factory.messageHandoff = second;
        fixture.manager()
                .addOrUpdateMessageConnection(
                        USERNAME, ConnectionProbe.connection(DIRECT_ENDPOINT).connection());
        second.fireDisconnected("closed", null);
        assertNull(fixture.manager().getCachedMessageConnection(USERNAME));
        assertEquals(1, second.closeCount);
        assertTrue(fixture.diagnostic.contains("Removed message connection record"));

        ConnectionProbe third = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        fixture.factory.messageHandoff = third;
        fixture.manager()
                .addOrUpdateMessageConnection(
                        USERNAME, ConnectionProbe.connection(DIRECT_ENDPOINT).connection());
        fixture.manager().removeAndDisposeAll();
        assertTrue(fixture.manager().getMessageConnections().isEmpty());
        assertEquals(1, third.closeCount);
    }

    /**
     * removeAndDisposeAll iterates the map weakly, so an insertion racing the
     * sweep — or arriving after close() — used to create a cell nothing would
     * ever dispose. A shutdown-time place-in-queue poll or upload-failure
     * notification could repopulate the cache of a closed network, which is
     * how a live run's last cache census read 1 rather than 0.
     */
    @Test
    @DisplayName("a closed network refuses new cache entries instead of leaking them")
    void aClosedNetworkRefusesNewCacheEntries() {
        Fixture fixture = new Fixture();
        fixture.manager().close();

        ConnectionProbe late = ConnectionProbe.message(USERNAME, DIRECT_ENDPOINT);
        fixture.factory.messageHandoff = late;
        assertThrows(
                Exception.class,
                () -> fixture.manager()
                        .addOrUpdateMessageConnection(
                                USERNAME,
                                ConnectionProbe.connection(DIRECT_ENDPOINT).connection()));
        assertTrue(fixture.manager().getMessageConnections().isEmpty(), "the closed network's cache must stay empty");
    }

    /**
     * The map holds attempts as well as connections. This used to join every
     * one, so asking how many peers were connected waited for whichever was
     * slowest to answer — up to the connection timeout — and threw when one
     * failed. Its callers are a metrics gauge and a diagnostic, which is to say
     * the one thing a monitoring system calls every few seconds.
     */
    @Test
    // On a separate thread, so a regression fails in ten seconds rather than
    // hanging the build — which is what the blocking version actually did.
    @org.junit.jupiter.api.Timeout(
            value = 10,
            unit = java.util.concurrent.TimeUnit.SECONDS,
            threadMode = org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD)
    @org.junit.jupiter.api.DisplayName("counting connections neither waits on nor breaks over an attempt in flight")
    void theConnectionListDoesNotBlockOnPendingAttempts() {
        Fixture fixture = new Fixture();
        ConnectionProbe pending = ConnectionProbe.message(USERNAME, INDIRECT_ENDPOINT);
        CompletableFuture<Void> neverConnects = new CompletableFuture<>();
        pending.connectFuture = neverConnects;
        fixture.factory.messageDirect = pending;

        java.util.concurrent.Executor callers = task -> Thread.ofVirtual().start(task);
        CompletableFuture<MessageConnection> attempt = CompletableFuture.supplyAsync(
                () -> fixture.manager()
                        .getOrAddMessageConnection(new ConnectToPeerResponse(
                                USERNAME, Constants.ConnectionType.PEER, INDIRECT_ENDPOINT, TOKEN, false)),
                callers);

        assertFalse(attempt.isDone(), "the attempt should still be in flight");
        // Returns now, not when the peer answers.
        assertEquals(List.of(), fixture.manager().getMessageConnections());

        neverConnects.completeExceptionally(new ConnectionException("no route to host"));
        // And a failed attempt is an absent connection, not a thrown gauge.
        assertEquals(List.of(), fixture.manager().getMessageConnections());
    }

    @Test
    void returnedSnapshotsAreImmutable() {
        Fixture fixture = new Fixture();
        assertThrows(
                UnsupportedOperationException.class,
                () -> fixture.manager().getPendingSolicitations().put(1, "x"));
        assertThrows(
                UnsupportedOperationException.class,
                () -> fixture.manager().getMessageConnections().add(new PeerEndpoint(USERNAME, DIRECT_ENDPOINT)));
    }

    private static InetSocketAddress endpoint(int port) {
        try {
            return new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port);
        } catch (Exception exception) {
            throw new AssertionError(exception);
        }
    }

    private static byte[] littleEndian(int value) {
        return ByteBuffer.allocate(4)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(value)
                .array();
    }

    private static final class Fixture {
        private final RecordingDiagnostic diagnostic = new RecordingDiagnostic();
        private final FakeWaiter waiter = new FakeWaiter();
        private final ConnectionProbe serverConnection = ConnectionProbe.message("", endpoint(2242));
        private final FakeFactory factory = new FakeFactory();
        private final Supplier<SoulseekClientOptions> options = SoulseekClientOptions::new;
        private final ServerLink server =
                ServerLinks.loggedIn(waiter, diagnostic, serverConnection.messageConnection(), LOCAL_USER);
        private final TokenFactory tokens = new TokenFactory(TOKEN);
        private final PeerMessageHandler peerMessages = (PeerMessageHandler) Proxy.newProxyInstance(
                PeerMessageHandler.class.getClassLoader(),
                new Class<?>[] {PeerMessageHandler.class},
                (proxy, method, arguments) -> defaultValue(method.getReturnType()));
        private final PeerNetwork manager =
                new PeerNetwork(options, server, waiter, tokens, peerMessages, factory, diagnostic);

        private PeerNetwork manager() {
            return manager;
        }
    }

    private static final class FakeFactory implements ConnectionFactory {
        private ConnectionProbe messageDirect;
        private ConnectionProbe messageHandoff;
        private ConnectionProbe transferDirect;
        private ConnectionProbe transferHandoff;
        private int messageDirectCount;

        @Override
        public MessageConnection getDistributedConnection(
                String username, InetSocketAddress ipEndpoint, ConnectionOptions options, TcpClient tcpClient) {
            throw new AssertionError("unexpected distributed connection");
        }

        @Override
        public MessageConnection getMessageConnection(
                String username, InetSocketAddress ipEndpoint, ConnectionOptions options, TcpClient tcpClient) {
            if (tcpClient != null) {
                assertNotNull(messageHandoff);
                return messageHandoff.messageConnection();
            }
            messageDirectCount++;
            assertNotNull(messageDirect);
            return messageDirect.messageConnection();
        }

        @Override
        public MessageConnection getServerConnection(
                InetSocketAddress ipEndpoint,
                ConnectionEventListener<Void> connectedEventHandler,
                ConnectionEventListener<ConnectionDisconnectedEvent> disconnectedEventHandler,
                MessageConnectionEventListener<MessageEvent> messageReadEventHandler,
                MessageConnectionEventListener<MessageEvent> messageWrittenEventHandler,
                ConnectionOptions options,
                TcpClient tcpClient) {
            throw new AssertionError("unexpected server connection");
        }

        @Override
        public Connection getTransferConnection(
                InetSocketAddress ipEndpoint, ConnectionOptions options, TcpClient tcpClient) {
            if (tcpClient != null) {
                assertNotNull(transferHandoff);
                return transferHandoff.connection();
            }
            assertNotNull(transferDirect);
            return transferDirect.connection();
        }
    }

    private static final class FakeWaiter implements Waiter {
        private final Map<WaitKey, CompletableFuture<?>> futures = new HashMap<>();
        private CompletableFuture<?> defaultFuture =
                CompletableFuture.failedFuture(new IllegalStateException("No configured wait"));

        private void put(WaitKey key, CompletableFuture<?> future) {
            futures.put(key, future);
        }

        @Override
        public int getDefaultTimeout() {
            return 5_000;
        }

        @Override
        public void cancel(WaitKey key) {
            CompletableFuture<?> future = futures.get(key);
            if (future != null) {
                future.cancel(false);
            }
        }

        @Override
        public void cancelAll() {
            futures.values().forEach(future -> future.cancel(false));
        }

        @Override
        public void complete(WaitKey key) {
            complete(key, null);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> void complete(WaitKey key, T result) {
            CompletableFuture<T> future = (CompletableFuture<T>) futures.get(key);
            if (future != null) {
                future.complete(result);
            }
        }

        @Override
        public boolean hasWait(WaitKey key) {
            return futures.containsKey(key);
        }

        @Override
        public void fail(WaitKey key, Throwable exception) {
            CompletableFuture<?> future = futures.get(key);
            if (future != null) {
                future.completeExceptionally(exception);
            }
        }

        @Override
        public void timeout(WaitKey key) {
            fail(key, new java.util.concurrent.TimeoutException());
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> Wait<T> register(
                WaitKey key, Class<T> resultType, Integer timeout, CancellationSignal cancellationSignal) {
            CompletableFuture<T> configured = (CompletableFuture<T>) futures.getOrDefault(key, defaultFuture);
            // The future is how a test says what the answer will be; Outcomes
            // turns it into what a real wait raises.
            return () -> Outcomes.raise(configured);
        }

        @Override
        public void close() {
            cancelAll();
        }
    }

    private static final class RecordingDiagnostic implements DiagnosticSink {
        private final List<String> messages = new ArrayList<>();

        private boolean contains(String value) {
            return messages.stream().anyMatch(message -> message.toLowerCase().contains(value.toLowerCase()));
        }

        @Override
        public void trace(String message) {
            messages.add(message);
        }

        @Override
        public void trace(String message, Throwable exception) {
            messages.add(message);
        }

        @Override
        public void debug(String message) {
            messages.add(message);
        }

        @Override
        public void debug(String message, Throwable exception) {
            messages.add(message);
        }

        @Override
        public void info(String message) {
            messages.add(message);
        }

        @Override
        public void warning(String message) {
            messages.add(message);
        }

        @Override
        public void warning(String message, Throwable exception) {
            messages.add(message);
        }
    }

    private static final class ConnectionProbe implements InvocationHandler {
        private final boolean message;
        private final String username;
        private final InetSocketAddress endpoint;
        private final UUID id = UUID.randomUUID();
        private final TcpClient tcpClient = (TcpClient) Proxy.newProxyInstance(
                TcpClient.class.getClassLoader(),
                new Class<?>[] {TcpClient.class},
                (proxy, method, arguments) -> defaultValue(method.getReturnType()));
        private final Connection proxy;
        private final List<ConnectionEventListener<ConnectionDisconnectedEvent>> disconnectedListeners =
                new ArrayList<>();
        private final List<MessageConnectionEventListener<MessageEvent>> messageReadListeners = new ArrayList<>();
        private final List<MessageConnectionEventListener<MessageReceivedEvent>> messageReceivedListeners =
                new ArrayList<>();
        private final List<MessageConnectionEventListener<MessageEvent>> messageWrittenListeners = new ArrayList<>();
        private final List<byte[]> byteWrites = new ArrayList<>();
        private final List<OutgoingMessage> outgoingWrites = new ArrayList<>();
        private ConnectionTypes type = ConnectionTypes.NONE;
        private CompletableFuture<Void> connectFuture = CompletableFuture.completedFuture(null);
        private CompletableFuture<Void> writeFuture = CompletableFuture.completedFuture(null);
        private CompletableFuture<byte[]> readFuture = CompletableFuture.completedFuture(new byte[4]);
        private RuntimeException startFailure;
        private int closeCount;
        private int connectCount;
        private int handoffCount;
        private int startReadingCount;

        private ConnectionProbe(boolean message, String username, InetSocketAddress endpoint) {
            this.message = message;
            this.username = username;
            this.endpoint = endpoint;
            Class<?>[] interfaces =
                    message ? new Class<?>[] {MessageConnection.class} : new Class<?>[] {Connection.class};
            proxy = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), interfaces, this);
        }

        private static ConnectionProbe connection(InetSocketAddress endpoint) {
            return new ConnectionProbe(false, null, endpoint);
        }

        private static ConnectionProbe message(String username, InetSocketAddress endpoint) {
            return new ConnectionProbe(true, username, endpoint);
        }

        private Connection connection() {
            return proxy;
        }

        private MessageConnection messageConnection() {
            if (!message) {
                throw new IllegalStateException("Not a message connection");
            }
            return (MessageConnection) proxy;
        }

        private void fireDisconnected(String message, Exception exception) {
            ConnectionDisconnectedEvent eventData = new ConnectionDisconnectedEvent(message, exception);
            List.copyOf(disconnectedListeners).forEach(listener -> listener.handle(proxy, eventData));
        }

        @Override
        public Object invoke(Object ignored, Method method, Object[] arguments) {
            String name = method.getName();
            return switch (name) {
                case "getId" -> id;
                case "getInactiveTime" -> Duration.ZERO;
                case "getIpEndpoint" -> endpoint;
                case "getKey" -> new ConnectionKey(username, endpoint);
                case "getOptions" -> new ConnectionOptions();
                case "getState" -> ConnectionState.CONNECTED;
                case "getType" -> type;
                case "setType" -> {
                    type = (ConnectionTypes) arguments[0];
                    yield null;
                }
                case "getWriteQueueDepth" -> 0;
                case "getCodeLength" -> 4;
                case "isServerConnection" -> username == null || username.isEmpty();
                case "isReadingContinuously" -> startReadingCount > 0;
                case "getUsername" -> username == null ? "" : username;
                // The transport blocks now, so a configured outcome arrives as
                // a return or a throw rather than as a settled future. join()
                // is how the future's own failure shape is preserved: a
                // cancellation raw, everything else in a CompletionException.
                case "connect" -> {
                    connectCount++;
                    Outcomes.raise(connectFuture);
                    yield null;
                }
                case "read" -> Outcomes.raise(readFuture);
                case "write" -> {
                    if (arguments[0] instanceof byte[] bytes) {
                        byteWrites.add(Arrays.copyOf(bytes, bytes.length));
                    } else if (arguments[0] instanceof OutgoingMessage value) {
                        outgoingWrites.add(value);
                    }
                    Outcomes.raise(writeFuture);
                    yield null;
                }
                case "handoffTcpClient" -> {
                    handoffCount++;
                    yield tcpClient;
                }
                case "startReadingContinuously" -> {
                    startReadingCount++;
                    if (startFailure != null) {
                        throw startFailure;
                    }
                    yield null;
                }
                case "close" -> {
                    closeCount++;
                    yield null;
                }
                case "disconnect" -> null;
                case "addDisconnectedListener" -> {
                    disconnectedListeners.add(cast(arguments[0]));
                    yield null;
                }
                case "removeDisconnectedListener" -> {
                    disconnectedListeners.remove(arguments[0]);
                    yield null;
                }
                case "addMessageReadListener" -> {
                    messageReadListeners.add(cast(arguments[0]));
                    yield null;
                }
                case "removeMessageReadListener" -> {
                    messageReadListeners.remove(arguments[0]);
                    yield null;
                }
                case "addMessageReceivedListener" -> {
                    messageReceivedListeners.add(cast(arguments[0]));
                    yield null;
                }
                case "removeMessageReceivedListener" -> {
                    messageReceivedListeners.remove(arguments[0]);
                    yield null;
                }
                case "addMessageWrittenListener" -> {
                    messageWrittenListeners.add(cast(arguments[0]));
                    yield null;
                }
                case "removeMessageWrittenListener" -> {
                    messageWrittenListeners.remove(arguments[0]);
                    yield null;
                }
                case "toString" -> "ConnectionProbe(" + endpoint + ")";
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == arguments[0];
                default -> defaultValue(method.getReturnType());
            };
        }

        @SuppressWarnings("unchecked")
        private static <T> T cast(Object value) {
            return (T) value;
        }
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0f;
        }
        if (type == double.class) {
            return 0d;
        }
        if (type == char.class) {
            return '\0';
        }
        return null;
    }
}
