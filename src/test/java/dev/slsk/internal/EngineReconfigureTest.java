// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.exceptions.ListenException;
import dev.slsk.exceptions.NoResponseException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.internal.common.Outcomes;
import dev.slsk.internal.common.TokenBucket;
import dev.slsk.internal.concurrent.CancellationController;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.connection.SoulseekClientState;
import dev.slsk.internal.messaging.messages.OutgoingMessage;
import dev.slsk.internal.messaging.messages.PrivateRoomToggle;
import dev.slsk.internal.messaging.messages.SetListenPortCommand;
import dev.slsk.internal.network.DistributedConnectionManager;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.network.tcp.Listener;
import dev.slsk.internal.options.ConnectionOptions;
import dev.slsk.internal.options.SoulseekClientOptions;
import dev.slsk.internal.options.SoulseekClientOptionsPatch;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class EngineReconfigureTest {
    private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();

    @Test
    void rejectsNullPatchAndListenerPreflightFailure() {
        Fixture fixture = new Fixture();
        assertThrows(NullPointerException.class, () -> fixture.client.reconfigureOptions(null));

        ListenerProbe failing = new ListenerProbe();
        failing.startFailure = new IllegalStateException("bind");
        fixture.listenerFactory.next = failing;
        SoulseekClientOptionsPatch patch =
                patch(null, LOOPBACK, 50_001, null, null, null, null, null, null, null, null, null);
        assertThrows(ListenException.class, () -> fixture.client.reconfigureOptions(patch));
        assertSame(fixture.options, fixture.client.getOptions());
        fixture.close();
    }

    @Test
    void emptyPatchSucceedsAndDisconnectedClientSendsNothing() {
        Fixture fixture = new Fixture();

        boolean reconnect = fixture.client.reconfigureOptions(new SoulseekClientOptionsPatch());

        assertFalse(reconnect);
        assertTrue(fixture.server.messages.isEmpty());
        assertEquals(0, fixture.distributed.updateCount);
        assertNotSame(fixture.options, fixture.client.getOptions());
        fixture.close();
    }

    @Test
    void reportsReconnectOnlyForConnectedDisablingAndConnectionOptions() {
        assertReconnect(
                new SoulseekClientOptions(),
                patch(null, null, null, false, null, null, null, null, null, null, null, null),
                true);
        SoulseekClientOptions distributedDisabled = new SoulseekClientOptions()
                .with(patch(null, null, null, false, null, null, null, null, null, null, null, null));
        assertReconnect(
                distributedDisabled,
                patch(null, null, null, true, null, null, null, null, null, null, null, null),
                false);
        assertReconnect(
                new SoulseekClientOptions(),
                patch(null, null, null, null, false, null, null, null, null, null, null, null),
                true);
        assertReconnect(
                new SoulseekClientOptions(),
                patch(null, null, null, null, null, null, null, null, null, new ConnectionOptions(), null, null),
                true);
        assertReconnect(
                new SoulseekClientOptions(),
                patch(null, null, null, null, null, null, null, null, null, null, null, new ConnectionOptions()),
                true);
    }

    @Test
    void connectedClientSendsConfigurationAndForwardsToken() {
        Fixture fixture = new Fixture();
        fixture.client.setStateForTest(loggedIn());
        CancellationController source = new CancellationController();
        CancellationSignal token = source.getSignal();

        boolean reconnect = fixture.client.reconfigureOptions(new SoulseekClientOptionsPatch(), token);

        assertFalse(reconnect);
        assertEquals(3, fixture.server.messages.size());
        assertInstanceOf(SetListenPortCommand.class, fixture.server.messages.get(0));
        assertInstanceOf(PrivateRoomToggle.class, fixture.server.messages.get(1));
        assertInstanceOf(
                dev.slsk.internal.messaging.messages.UserStatisticsRequest.class, fixture.server.messages.get(2));
        fixture.server.tokens.forEach(observed -> assertSame(token, observed));
        assertEquals(1, fixture.distributed.updateCount);
        assertSame(token, fixture.distributed.token);
        fixture.close();
    }

    @Test
    void disablesListenerAndHandlesNullOrStoppedListener() {
        Fixture fixture =
                new Fixture(SoulseekClientOptions.builder().enableListener(true).build());
        fixture.initialListener.listening = true;
        SoulseekClientOptionsPatch disable =
                patch(false, null, null, null, null, null, null, null, null, null, null, null);

        fixture.client.reconfigureOptions(disable);

        assertEquals(1, fixture.initialListener.stopCount);
        assertNull(fixture.client.listener);
        assertFalse(fixture.client.getOptions().enableListener());

        Fixture nullFixture =
                new Fixture(SoulseekClientOptions.builder().enableListener(true).build());
        nullFixture.client.setListenerForTest(null);
        nullFixture.client.reconfigureOptions(disable);
        assertNull(nullFixture.client.listener);
        fixture.close();
        nullFixture.close();
    }

    @Test
    void replacesListeningListenerWithPatchedSettings() {
        Fixture fixture =
                new Fixture(SoulseekClientOptions.builder().enableListener(true).build());
        fixture.initialListener.listening = true;
        ListenerProbe preflight = new ListenerProbe();
        ListenerProbe replacement = new ListenerProbe();
        fixture.listenerFactory.sequence = new ArrayList<>(List.of(preflight, replacement));
        ConnectionOptions incoming =
                ConnectionOptions.builder().readBufferSize(8192).build();
        SoulseekClientOptionsPatch patch =
                patch(null, LOOPBACK, 50_002, null, null, null, null, null, null, null, incoming, null);

        fixture.client.reconfigureOptions(patch);

        assertEquals(1, preflight.startCount);
        assertEquals(1, preflight.stopCount);
        assertEquals(1, fixture.initialListener.stopCount);
        assertSame(replacement.proxy, fixture.client.listener);
        assertTrue(replacement.listening);
        assertEquals(50_002, replacement.port);
        assertEquals(LOOPBACK, replacement.ipAddress);
        assertSame(incoming, replacement.options);
        assertEquals(50_002, fixture.client.getOptions().listenPort());
        assertSame(incoming, fixture.client.getOptions().incomingConnectionOptions());
        fixture.close();
    }

    @Test
    void stoppedListenerIsRemovedWithoutReplacement() {
        Fixture fixture = new Fixture();
        fixture.initialListener.listening = false;
        ConnectionOptions incoming =
                ConnectionOptions.builder().readBufferSize(4096).build();
        SoulseekClientOptionsPatch patch =
                patch(null, null, null, null, null, null, null, null, null, null, incoming, null);

        fixture.client.reconfigureOptions(patch);

        assertNull(fixture.client.listener);
        assertSame(incoming, fixture.client.getOptions().incomingConnectionOptions());
        assertEquals(0, fixture.listenerFactory.created.size());
        fixture.close();
    }

    @Test
    void updatesOptionsAndChangedTokenBucketCapacities() {
        Fixture fixture = new Fixture();
        ConnectionOptions peer =
                ConnectionOptions.builder().readBufferSize(7000).build();
        SoulseekClientOptionsPatch patch = patch(false, null, null, false, false, 7, 50, 70, true, null, null, null);
        SoulseekClientOptionsPatch.Builder builder = SoulseekClientOptionsPatch.builder()
                .deduplicateSearchRequests(false)
                .autoAcknowledgePrivateMessages(false)
                .autoAcknowledgePrivilegeNotifications(false)
                .peerConnectionOptions(peer);
        patch.enableListener().ifPresent(builder::enableListener);
        patch.listenIpAddress().ifPresent(builder::listenIpAddress);
        patch.listenPort().ifPresent(builder::listenPort);
        patch.enableDistributedNetwork().ifPresent(builder::enableDistributedNetwork);
        patch.acceptDistributedChildren().ifPresent(builder::acceptDistributedChildren);
        patch.distributedChildLimit().ifPresent(builder::distributedChildLimit);
        patch.maximumUploadSpeed().ifPresent(builder::maximumUploadSpeed);
        patch.maximumDownloadSpeed().ifPresent(builder::maximumDownloadSpeed);
        patch.acceptPrivateRoomInvitations().ifPresent(builder::acceptPrivateRoomInvitations);
        patch = builder.build();

        fixture.client.reconfigureOptions(patch);

        SoulseekClientOptions updated = fixture.client.getOptions();
        assertFalse(updated.enableListener());
        assertFalse(updated.enableDistributedNetwork());
        assertFalse(updated.acceptDistributedChildren());
        assertEquals(7, updated.distributedChildLimit());
        assertEquals(50, updated.maximumUploadSpeed());
        assertEquals(70, updated.maximumDownloadSpeed());
        assertFalse(updated.deduplicateSearchRequests());
        assertFalse(updated.autoAcknowledgePrivateMessages());
        assertFalse(updated.autoAcknowledgePrivilegeNotifications());
        assertTrue(updated.acceptPrivateRoomInvitations());
        assertSame(peer, updated.peerConnectionOptions());
        assertEquals((50 * 1024L) / 10, fixture.uploadBucket.getCapacity());
        assertEquals((70 * 1024L) / 10, fixture.downloadBucket.getCapacity());
        fixture.close();
    }

    @Test
    void unchangedSpeedsDoNotResetConsumedBucketCounts() {
        Fixture fixture = new Fixture();
        fixture.uploadBucket.get(25);
        fixture.downloadBucket.get(25);
        long uploadCapacity = fixture.uploadBucket.getCapacity();
        long downloadCapacity = fixture.downloadBucket.getCapacity();

        fixture.client.reconfigureOptions(patch(
                null,
                null,
                null,
                null,
                null,
                null,
                fixture.options.maximumUploadSpeed(),
                fixture.options.maximumDownloadSpeed(),
                null,
                null,
                null,
                null));

        assertEquals(uploadCapacity, fixture.uploadBucket.getCapacity());
        assertEquals(downloadCapacity, fixture.downloadBucket.getCapacity());
        fixture.close();
    }

    @Test
    void preservesCancellationAndTimeoutButWrapsOtherErrorsWithoutRollback() {
        Fixture cancelledFixture = new Fixture();
        CancellationController source = new CancellationController();
        source.cancel();
        assertInstanceOf(
                CancellationException.class,
                completionCause(() -> cancelledFixture.client.reconfigureOptions(
                        new SoulseekClientOptionsPatch(), source.getSignal())));
        assertSame(cancelledFixture.options, cancelledFixture.client.getOptions());

        Fixture timeoutFixture = new Fixture();
        timeoutFixture.client.setStateForTest(loggedIn());
        TimeoutException timeout = new TimeoutException("timeout");
        timeoutFixture.server.failure = timeout;
        assertSame(
                timeout,
                assertInstanceOf(
                                NoResponseException.class,
                                completionCause(() ->
                                        timeoutFixture.client.reconfigureOptions(new SoulseekClientOptionsPatch())))
                        .getCause());

        Fixture failedFixture = new Fixture();
        failedFixture.client.setStateForTest(loggedIn());
        IllegalStateException failure = new IllegalStateException("write");
        failedFixture.server.failure = failure;
        SoulseekClientOptionsPatch changed =
                patch(null, null, null, false, null, null, null, null, null, null, null, null);
        SoulseekClientException wrapped = assertInstanceOf(
                SoulseekClientException.class, completionCause(() -> failedFixture.client.reconfigureOptions(changed)));
        assertSame(failure, wrapped.getCause());
        assertFalse(failedFixture.client.getOptions().enableDistributedNetwork());
        cancelledFixture.close();
        timeoutFixture.close();
        failedFixture.close();
    }

    private static void assertReconnect(
            SoulseekClientOptions options, SoulseekClientOptionsPatch patch, boolean expected) {
        Fixture fixture = new Fixture(options);
        fixture.client.setStateForTest(loggedIn());
        assertEquals(expected, fixture.client.reconfigureOptions(patch));
        fixture.close();
    }

    private static SoulseekClientOptionsPatch patch(
            Boolean enableListener,
            InetAddress listenAddress,
            Integer listenPort,
            Boolean enableDistributed,
            Boolean acceptChildren,
            Integer childLimit,
            Integer maximumUploadSpeed,
            Integer maximumDownloadSpeed,
            Boolean acceptPrivateInvitations,
            ConnectionOptions serverOptions,
            ConnectionOptions incomingOptions,
            ConnectionOptions distributedOptions) {
        SoulseekClientOptionsPatch.Builder builder = SoulseekClientOptionsPatch.builder();
        if (enableListener != null) {
            builder.enableListener(enableListener);
        }
        if (listenAddress != null) {
            builder.listenIpAddress(listenAddress);
        }
        if (listenPort != null) {
            builder.listenPort(listenPort);
        }
        if (enableDistributed != null) {
            builder.enableDistributedNetwork(enableDistributed);
        }
        if (acceptChildren != null) {
            builder.acceptDistributedChildren(acceptChildren);
        }
        if (childLimit != null) {
            builder.distributedChildLimit(childLimit);
        }
        if (maximumUploadSpeed != null) {
            builder.maximumUploadSpeed(maximumUploadSpeed);
        }
        if (maximumDownloadSpeed != null) {
            builder.maximumDownloadSpeed(maximumDownloadSpeed);
        }
        if (acceptPrivateInvitations != null) {
            builder.acceptPrivateRoomInvitations(acceptPrivateInvitations);
        }
        if (serverOptions != null) {
            builder.serverConnectionOptions(serverOptions);
        }
        if (incomingOptions != null) {
            builder.incomingConnectionOptions(incomingOptions);
        }
        if (distributedOptions != null) {
            builder.distributedConnectionOptions(distributedOptions);
        }
        return builder.build();
    }

    private static SoulseekClientState loggedIn() {
        return SoulseekClientState.CONNECTED.or(SoulseekClientState.LOGGED_IN);
    }

    /**
     * Returns the failure a blocking call produced.
     *
     * <p>Took a future before the API became blocking; the calls now throw
     * directly, so it takes the call itself.
     */
    private static Throwable completionCause(org.junit.jupiter.api.function.Executable body) {
        try {
            body.execute();
        } catch (java.util.concurrent.CompletionException wrapped) {
            return wrapped.getCause() == null ? wrapped : wrapped.getCause();
        } catch (Throwable failure) {
            return failure;
        }
        throw new AssertionError("expected the operation to fail");
    }

    private static Object defaultValue(Class<?> type) {
        if (type == CompletableFuture.class) {
            return CompletableFuture.completedFuture(null);
        }
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

    private static final class Fixture {
        private final SoulseekClientOptions options;
        private final ServerProbe server = new ServerProbe();
        private final DistributedProbe distributed = new DistributedProbe();
        private final ListenerProbe initialListener = new ListenerProbe();
        private final ListenerFactoryProbe listenerFactory = new ListenerFactoryProbe();
        private final TokenBucket uploadBucket;
        private final TokenBucket downloadBucket;
        private final SoulseekEngine client;

        private Fixture() {
            this(SoulseekClientOptions.builder().enableListener(false).build());
        }

        private Fixture(SoulseekClientOptions clientOptions) {
            options = clientOptions;
            uploadBucket = new TokenBucket((options.maximumUploadSpeed() * 1024L) / 10, Duration.ofMillis(100));
            downloadBucket = new TokenBucket((options.maximumDownloadSpeed() * 1024L) / 10, Duration.ofMillis(100));
            client = new SoulseekEngine(
                    9999,
                    options,
                    server.proxy,
                    null,
                    null,
                    distributed.proxy,
                    null,
                    null,
                    null,
                    initialListener.proxy,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    uploadBucket,
                    downloadBucket);
            client.setClientListenerFactoryForTest(listenerFactory::create);
        }

        private void close() {
            client.close();
        }
    }

    private static final class ServerProbe {
        private final List<OutgoingMessage> messages = new ArrayList<>();
        private final List<CancellationSignal> tokens = new ArrayList<>();
        private Throwable failure;
        private final MessageConnection proxy = (MessageConnection) Proxy.newProxyInstance(
                MessageConnection.class.getClassLoader(), new Class<?>[] {MessageConnection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) throws Exception {
            if (method.getName().equals("write")
                    && arguments.length == 2
                    && arguments[0] instanceof OutgoingMessage outgoing) {
                messages.add(outgoing);
                tokens.add((CancellationSignal) arguments[1]);
                if (failure != null) {
                    if (failure instanceof RuntimeException runtime) {
                        throw runtime;
                    }
                    return Outcomes.raise(CompletableFuture.<Void>failedFuture(failure));
                }
                return null;
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class DistributedProbe {
        private int updateCount;
        private CancellationSignal token;
        private final DistributedConnectionManager proxy = (DistributedConnectionManager) Proxy.newProxyInstance(
                DistributedConnectionManager.class.getClassLoader(),
                new Class<?>[] {DistributedConnectionManager.class},
                this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) throws Exception {
            if (method.getName().equals("updateStatus") && arguments != null) {
                updateCount++;
                token = (CancellationSignal) arguments[0];
                return null;
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class ListenerFactoryProbe {
        private final List<ListenerProbe> created = new ArrayList<>();
        private List<ListenerProbe> sequence = new ArrayList<>();
        private ListenerProbe next;

        private Listener create(InetAddress ipAddress, int port, ConnectionOptions options) {
            ListenerProbe result;
            if (!sequence.isEmpty()) {
                result = sequence.remove(0);
            } else if (next != null) {
                result = next;
                next = null;
            } else {
                result = new ListenerProbe();
            }
            result.ipAddress = ipAddress;
            result.port = port;
            result.options = options;
            created.add(result);
            return result.proxy;
        }
    }

    private static final class ListenerProbe {
        private InetAddress ipAddress;
        private int port;
        private ConnectionOptions options = new ConnectionOptions();
        private boolean listening;
        private int startCount;
        private int stopCount;
        private RuntimeException startFailure;
        private final Listener proxy = (Listener)
                Proxy.newProxyInstance(Listener.class.getClassLoader(), new Class<?>[] {Listener.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) throws Exception {
            return switch (method.getName()) {
                case "getIpAddress" -> ipAddress;
                case "getPort" -> port;
                case "getConnectionOptions" -> options;
                case "isListening" -> listening;
                case "start" -> {
                    startCount++;
                    if (startFailure != null) {
                        throw startFailure;
                    }
                    listening = true;
                    yield null;
                }
                case "stop" -> {
                    stopCount++;
                    listening = false;
                    yield null;
                }
                default -> defaultValue(method.getReturnType());
            };
        }
    }
}
