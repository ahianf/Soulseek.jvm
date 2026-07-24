// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.slsk.common.TokenBucket;
import dev.slsk.exceptions.ListenException;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.messaging.messages.OutgoingMessage;
import dev.slsk.messaging.messages.PrivateRoomToggle;
import dev.slsk.messaging.messages.SetListenPortCommand;
import dev.slsk.network.DistributedConnectionManager;
import dev.slsk.network.MessageConnection;
import dev.slsk.network.tcp.Listener;
import dev.slsk.options.ConnectionOptions;
import dev.slsk.options.SoulseekClientOptions;
import dev.slsk.options.SoulseekClientOptionsPatch;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class SoulseekClientReconfigureTest {
    private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();

    @Test
    void rejectsNullPatchAndListenerPreflightFailure() {
        Fixture fixture = new Fixture();
        assertThrows(NullPointerException.class, () -> fixture.client.reconfigureOptionsAsync(null));

        ListenerProbe failing = new ListenerProbe();
        failing.startFailure = new IllegalStateException("bind");
        fixture.listenerFactory.next = failing;
        SoulseekClientOptionsPatch patch =
                patch(null, LOOPBACK, 50_001, null, null, null, null, null, null, null, null, null);
        assertThrows(ListenException.class, () -> fixture.client.reconfigureOptionsAsync(patch));
        assertSame(fixture.options, fixture.client.getOptions());
        fixture.close();
    }

    @Test
    void emptyPatchSucceedsAndDisconnectedClientSendsNothing() {
        Fixture fixture = new Fixture();

        boolean reconnect = fixture.client
                .reconfigureOptionsAsync(new SoulseekClientOptionsPatch())
                .join();

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
        CancellationTokenSource source = new CancellationTokenSource();
        CancellationToken token = source.getToken();

        boolean reconnect = fixture.client
                .reconfigureOptionsAsync(new SoulseekClientOptionsPatch(), token)
                .join();

        assertFalse(reconnect);
        assertEquals(2, fixture.server.messages.size());
        assertInstanceOf(SetListenPortCommand.class, fixture.server.messages.get(0));
        assertInstanceOf(PrivateRoomToggle.class, fixture.server.messages.get(1));
        fixture.server.tokens.forEach(observed -> assertSame(token, observed));
        assertEquals(1, fixture.distributed.updateCount);
        assertSame(token, fixture.distributed.token);
        fixture.close();
    }

    @Test
    void disablesListenerAndHandlesNullOrStoppedListener() {
        Fixture fixture = new Fixture(new SoulseekClientOptions(true));
        fixture.initialListener.listening = true;
        SoulseekClientOptionsPatch disable =
                patch(false, null, null, null, null, null, null, null, null, null, null, null);

        fixture.client.reconfigureOptionsAsync(disable).join();

        assertEquals(1, fixture.initialListener.stopCount);
        assertNull(fixture.client.getListener());
        assertFalse(fixture.client.getOptions().isEnableListener());

        Fixture nullFixture = new Fixture(new SoulseekClientOptions(true));
        nullFixture.client.setListenerForTest(null);
        nullFixture.client.reconfigureOptionsAsync(disable).join();
        assertNull(nullFixture.client.getListener());
        fixture.close();
        nullFixture.close();
    }

    @Test
    void replacesListeningListenerWithPatchedSettings() {
        Fixture fixture = new Fixture(new SoulseekClientOptions(true));
        fixture.initialListener.listening = true;
        ListenerProbe preflight = new ListenerProbe();
        ListenerProbe replacement = new ListenerProbe();
        fixture.listenerFactory.sequence = new ArrayList<>(List.of(preflight, replacement));
        ConnectionOptions incoming = new ConnectionOptions(8192);
        SoulseekClientOptionsPatch patch =
                patch(null, LOOPBACK, 50_002, null, null, null, null, null, null, null, incoming, null);

        fixture.client.reconfigureOptionsAsync(patch).join();

        assertEquals(1, preflight.startCount);
        assertEquals(1, preflight.stopCount);
        assertEquals(1, fixture.initialListener.stopCount);
        assertSame(replacement.proxy, fixture.client.getListener());
        assertTrue(replacement.listening);
        assertEquals(50_002, replacement.port);
        assertEquals(LOOPBACK, replacement.ipAddress);
        assertSame(incoming, replacement.options);
        assertEquals(50_002, fixture.client.getOptions().getListenPort());
        assertSame(incoming, fixture.client.getOptions().getIncomingConnectionOptions());
        fixture.close();
    }

    @Test
    void stoppedListenerIsRemovedWithoutReplacement() {
        Fixture fixture = new Fixture();
        fixture.initialListener.listening = false;
        ConnectionOptions incoming = new ConnectionOptions(4096);
        SoulseekClientOptionsPatch patch =
                patch(null, null, null, null, null, null, null, null, null, null, incoming, null);

        fixture.client.reconfigureOptionsAsync(patch).join();

        assertNull(fixture.client.getListener());
        assertSame(incoming, fixture.client.getOptions().getIncomingConnectionOptions());
        assertEquals(0, fixture.listenerFactory.created.size());
        fixture.close();
    }

    @Test
    void updatesOptionsAndChangedTokenBucketCapacities() {
        Fixture fixture = new Fixture();
        ConnectionOptions peer = new ConnectionOptions(7000);
        SoulseekClientOptionsPatch patch = patch(false, null, null, false, false, 7, 50, 70, true, null, null, null);
        patch = new SoulseekClientOptionsPatch(
                patch.getEnableListener(),
                patch.getListenIpAddress(),
                patch.getListenPort(),
                patch.getEnableDistributedNetwork(),
                patch.getAcceptDistributedChildren(),
                patch.getDistributedChildLimit(),
                patch.getMaximumUploadSpeed(),
                patch.getMaximumDownloadSpeed(),
                false,
                false,
                false,
                patch.getAcceptPrivateRoomInvitations(),
                null,
                peer,
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

        fixture.client.reconfigureOptionsAsync(patch).join();

        SoulseekClientOptions updated = fixture.client.getOptions();
        assertFalse(updated.isEnableListener());
        assertFalse(updated.isEnableDistributedNetwork());
        assertFalse(updated.isAcceptDistributedChildren());
        assertEquals(7, updated.getDistributedChildLimit());
        assertEquals(50, updated.getMaximumUploadSpeed());
        assertEquals(70, updated.getMaximumDownloadSpeed());
        assertFalse(updated.isDeduplicateSearchRequests());
        assertFalse(updated.isAutoAcknowledgePrivateMessages());
        assertFalse(updated.isAutoAcknowledgePrivilegeNotifications());
        assertTrue(updated.isAcceptPrivateRoomInvitations());
        assertSame(peer, updated.getPeerConnectionOptions());
        assertEquals((50 * 1024L) / 10, fixture.uploadBucket.getCapacity());
        assertEquals((70 * 1024L) / 10, fixture.downloadBucket.getCapacity());
        fixture.close();
    }

    @Test
    void unchangedSpeedsDoNotResetConsumedBucketCounts() {
        Fixture fixture = new Fixture();
        fixture.uploadBucket.getAsync(25).join();
        fixture.downloadBucket.getAsync(25).join();
        long uploadCapacity = fixture.uploadBucket.getCapacity();
        long downloadCapacity = fixture.downloadBucket.getCapacity();

        fixture.client
                .reconfigureOptionsAsync(patch(
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        fixture.options.getMaximumUploadSpeed(),
                        fixture.options.getMaximumDownloadSpeed(),
                        null,
                        null,
                        null,
                        null))
                .join();

        assertEquals(uploadCapacity, fixture.uploadBucket.getCapacity());
        assertEquals(downloadCapacity, fixture.downloadBucket.getCapacity());
        fixture.close();
    }

    @Test
    void preservesCancellationAndTimeoutButWrapsOtherErrorsWithoutRollback() {
        Fixture cancelledFixture = new Fixture();
        CancellationTokenSource source = new CancellationTokenSource();
        source.cancel();
        assertInstanceOf(
                CancellationException.class,
                completionCause(cancelledFixture.client.reconfigureOptionsAsync(
                        new SoulseekClientOptionsPatch(), source.getToken())));
        assertSame(cancelledFixture.options, cancelledFixture.client.getOptions());

        Fixture timeoutFixture = new Fixture();
        timeoutFixture.client.setStateForTest(loggedIn());
        TimeoutException timeout = new TimeoutException("timeout");
        timeoutFixture.server.failure = timeout;
        assertSame(
                timeout,
                completionCause(timeoutFixture.client.reconfigureOptionsAsync(new SoulseekClientOptionsPatch())));

        Fixture failedFixture = new Fixture();
        failedFixture.client.setStateForTest(loggedIn());
        IllegalStateException failure = new IllegalStateException("write");
        failedFixture.server.failure = failure;
        SoulseekClientOptionsPatch changed =
                patch(null, null, null, false, null, null, null, null, null, null, null, null);
        SoulseekClientException wrapped = assertInstanceOf(
                SoulseekClientException.class, completionCause(failedFixture.client.reconfigureOptionsAsync(changed)));
        assertSame(failure, wrapped.getCause());
        assertFalse(failedFixture.client.getOptions().isEnableDistributedNetwork());
        cancelledFixture.close();
        timeoutFixture.close();
        failedFixture.close();
    }

    private static void assertReconnect(
            SoulseekClientOptions options, SoulseekClientOptionsPatch patch, boolean expected) {
        Fixture fixture = new Fixture(options);
        fixture.client.setStateForTest(loggedIn());
        assertEquals(expected, fixture.client.reconfigureOptionsAsync(patch).join());
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
        return new SoulseekClientOptionsPatch(
                enableListener,
                listenAddress,
                listenPort,
                enableDistributed,
                acceptChildren,
                childLimit,
                maximumUploadSpeed,
                maximumDownloadSpeed,
                null,
                null,
                null,
                acceptPrivateInvitations,
                serverOptions,
                null,
                null,
                incomingOptions,
                distributedOptions,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    private static SoulseekClientStates loggedIn() {
        return SoulseekClientStates.CONNECTED.or(SoulseekClientStates.LOGGED_IN);
    }

    private static Throwable completionCause(CompletableFuture<?> future) {
        try {
            future.join();
            throw new AssertionError("Expected failure");
        } catch (CancellationException failure) {
            return failure;
        } catch (CompletionException failure) {
            Throwable cause = failure;
            while (cause instanceof CompletionException && cause.getCause() != null) {
                cause = cause.getCause();
            }
            return cause;
        }
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
        private final DefaultSoulseekClient client;

        private Fixture() {
            this(new SoulseekClientOptions(false));
        }

        private Fixture(SoulseekClientOptions clientOptions) {
            options = clientOptions;
            uploadBucket = new TokenBucket((options.getMaximumUploadSpeed() * 1024L) / 10, 100);
            downloadBucket = new TokenBucket((options.getMaximumDownloadSpeed() * 1024L) / 10, 100);
            client = new DefaultSoulseekClient(
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
        private final List<CancellationToken> tokens = new ArrayList<>();
        private Throwable failure;
        private final MessageConnection proxy = (MessageConnection) Proxy.newProxyInstance(
                MessageConnection.class.getClassLoader(), new Class<?>[] {MessageConnection.class}, this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("writeAsync")
                    && arguments.length == 2
                    && arguments[0] instanceof OutgoingMessage outgoing) {
                messages.add(outgoing);
                tokens.add((CancellationToken) arguments[1]);
                if (failure != null) {
                    if (failure instanceof RuntimeException runtime) {
                        throw runtime;
                    }
                    return CompletableFuture.failedFuture(failure);
                }
                return CompletableFuture.completedFuture(null);
            }
            return defaultValue(method.getReturnType());
        }
    }

    private static final class DistributedProbe {
        private int updateCount;
        private CancellationToken token;
        private final DistributedConnectionManager proxy = (DistributedConnectionManager) Proxy.newProxyInstance(
                DistributedConnectionManager.class.getClassLoader(),
                new Class<?>[] {DistributedConnectionManager.class},
                this::invoke);

        private Object invoke(Object ignored, Method method, Object[] arguments) {
            if (method.getName().equals("updateStatusAsync")) {
                updateCount++;
                token = (CancellationToken) arguments[0];
                return CompletableFuture.completedFuture(null);
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

        private Object invoke(Object ignored, Method method, Object[] arguments) {
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
