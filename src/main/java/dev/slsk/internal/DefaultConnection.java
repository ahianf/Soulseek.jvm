// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.Attachment;
import dev.slsk.Connection;
import dev.slsk.EventStream;
import dev.slsk.connection.ConnectionState;
import dev.slsk.connection.ServerAddress;
import dev.slsk.connection.ServerInfo;
import dev.slsk.events.ConnectionEvent;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.LoginRejectedException;
import dev.slsk.internal.EngineEvents.Kind;
import dev.slsk.internal.concurrent.BlockingInvocation;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.connection.SoulseekClientState;
import dev.slsk.internal.events.EventBus;
import dev.slsk.internal.events.SoulseekClientDisconnectedEvent;
import dev.slsk.internal.events.SoulseekClientStateChangedEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * {@link Connection}, over the engine.
 *
 * <p>Most of what this does is translate two bit-flag sets into types that name
 * themselves. {@code SoulseekClientState} is a set of bits where {@code CONNECTED
 * | LOGGED_IN} means online and a caller has to know it; {@link ConnectionState}
 * says {@code Online}. The internal {@code ServerInfo} uses boxed {@code
 * Integer} and {@code Boolean} to mean "the server has not told us yet", which
 * is a {@code NullPointerException} waiting for the caller who forgets; the
 * public one says {@code OptionalInt}.
 *
 * <p>The {@code since} timestamp on {@code Online} has no source in the client,
 * which never recorded when a session began. It is captured here on the
 * transition into logged-in.
 */
final class DefaultConnection implements Connection {

    private final SoulseekEngine client;
    private final ServerLink server;
    private final Credentials credentials;
    private final EventBus<ConnectionEvent> events;

    /** Gets the connection back after it drops. */
    private final ReconnectSupervisor reconnects;

    /** When the current session began; {@code null} whenever not logged in. */
    private final AtomicReference<Instant> onlineSince = new AtomicReference<>();

    /** The last state we published, so a transition can report what it came from. */
    private final AtomicReference<ConnectionState> published = new AtomicReference<>(new ConnectionState.Offline());

    /**
     * Where the consumer last asked to connect; {@code null} for the default
     * server. A reconnect goes back to the same place, which for a private or
     * loopback server is the difference between recovering and wandering onto
     * the public network.
     */
    private final AtomicReference<ServerAddress> target = new AtomicReference<>();

    /**
     * Set while the consumer's own {@code connect} is running, so a disconnect
     * raised by a failing attempt is left for that call to handle rather than
     * being treated as a drop.
     */
    private final AtomicBoolean connecting = new AtomicBoolean();

    /**
     * Set by {@link #disconnect(String)}. A disconnect the consumer asked for is
     * not a drop, and reconnecting over the top of it would be the library
     * overruling the caller.
     */
    private final AtomicBoolean disconnected = new AtomicBoolean();

    DefaultConnection(SoulseekEngine client, Credentials credentials, EventBus<ConnectionEvent> events) {
        this.client = Objects.requireNonNull(client, "client");
        this.server = client.server();
        this.credentials = Objects.requireNonNull(credentials, "credentials");
        this.events = Objects.requireNonNull(events, "events");
        this.reconnects = new ReconnectSupervisor(this::connectOnce, this::onStateChanged, client.getDiagnostic());
        wire();
    }

    /** The credentials the builder was given, used by every {@code connect}. */
    record Credentials(String username, String password) {
        Credentials {
            Objects.requireNonNull(username, "username");
            Objects.requireNonNull(password, "password");
        }
    }

    private void wire() {
        // Four kinds for one transition: the engine raises connected, logged-in
        // and state-changed for what a consumer sees as a single move.
        client.events().on(Kind.STATE_CHANGED, (SoulseekClientStateChangedEvent event) -> onStateChanged());
        client.events().on(Kind.CONNECTED, (Void event) -> onStateChanged());
        client.events().on(Kind.DISCONNECTED, (SoulseekClientDisconnectedEvent event) -> onDisconnected(event));
        // Deliberately not cancelling the supervisor here. Logging in is how a
        // retry run ends, and the loop already returns on a connect that
        // succeeded — but this event can arrive on the supervisor's own thread,
        // from inside that connect, and cancelling interrupts the thread it is
        // delivered on.
        client.events().on(Kind.LOGGED_IN, (Void event) -> onStateChanged());

        client.events()
                .on(
                        Kind.SERVER_INFO_RECEIVED,
                        (dev.slsk.internal.connection.ServerInfo event) ->
                                events.publish(new ConnectionEvent.ServerInfoReceived(serverInfo(), Instant.now())));
        client.events()
                .on(
                        Kind.KICKED_FROM_SERVER,
                        (Void event) -> events.publish(
                                new ConnectionEvent.KickedFromServer("kicked from the server", Instant.now())));
        client.events()
                .on(
                        Kind.GLOBAL_MESSAGE_RECEIVED,
                        (String message) -> events.publish(
                                new ConnectionEvent.GlobalMessageReceived(String.valueOf(message), Instant.now())));
        client.events()
                .on(
                        Kind.EXCLUDED_SEARCH_PHRASES_RECEIVED,
                        (java.util.List<String> phrases) ->
                                events.publish(new ConnectionEvent.ExcludedSearchPhrasesReceived(
                                        phrases == null ? List.of() : List.copyOf(phrases), Instant.now())));
    }

    /**
     * Publishes a transition, under the bus lock so it cannot interleave with an
     * {@link #attach}. Nothing is published when the mapped state has not
     * actually changed: the client raises connected, logged-in and state-changed
     * for what is one transition, and a consumer should see one event.
     */
    private void onStateChanged() {
        events.mutateAndPublish(() -> {
            ConnectionState current = mapState();
            ConnectionState previous = published.getAndSet(current);
            return previous.equals(current) ? null : new ConnectionEvent.StateChanged(previous, current, Instant.now());
        });
    }

    private ConnectionState mapState() {
        SoulseekClientState state = client.getState();
        if (state.contains(SoulseekClientState.LOGGED_IN)) {
            Instant since = onlineSince.updateAndGet(existing -> existing == null ? Instant.now() : existing);
            return new ConnectionState.Online(since, serverInfo());
        }
        onlineSince.set(null);
        if (state.contains(SoulseekClientState.DISCONNECTING)) {
            return new ConnectionState.Disconnecting();
        }
        if (state.contains(SoulseekClientState.LOGGING_IN) || state.contains(SoulseekClientState.CONNECTED)) {
            return new ConnectionState.Authenticating();
        }
        if (state.contains(SoulseekClientState.CONNECTING)) {
            return new ConnectionState.Connecting(reconnects.attempt());
        }
        // The engine has no bit for "waiting to try again", so the supervisor
        // supplies the one state it cannot describe.
        ConnectionState.Reconnecting waiting = reconnects.pending();
        return waiting != null ? waiting : new ConnectionState.Offline();
    }

    /**
     * Handles a disconnect, arming the supervisor when it was not asked for.
     *
     * <p>A disconnect raised while the consumer's own {@code connect} is running
     * belongs to that call, which arms the supervisor itself if the attempt ends
     * up failing. Arming here as well would start retrying underneath a call
     * that has not returned yet.
     */
    private void onDisconnected(SoulseekClientDisconnectedEvent event) {
        onStateChanged();
        if (connecting.get() || disconnected.get()) {
            return;
        }
        Throwable cause = causeOf(event);
        if (retryable(cause)) {
            reconnects.arm(cause);
        }
    }

    /** The failure behind a disconnect, synthesised when the event carries none. */
    private static Throwable causeOf(SoulseekClientDisconnectedEvent event) {
        Throwable reported = event == null ? null : event.getException();
        if (reported != null) {
            return reported;
        }
        String message =
                event == null || event.getMessage() == null ? "The server connection was lost" : event.getMessage();
        return new ConnectionException(message);
    }

    /**
     * Returns whether a failure is worth trying again.
     *
     * <p>Credentials the server refused are the one thing that never becomes
     * right by waiting, and retrying them is abuse from the server's side.
     */
    private static boolean retryable(Throwable cause) {
        // Bounded rather than walked to the end: a cause chain that cycles is
        // rare but constructible, and this runs on the disconnect path, where
        // spinning would cost the reconnect it is deciding about. The same
        // guard, for the same reason, as Throwable's own stack-trace printing.
        Throwable walk = cause;
        for (int depth = 0; walk != null && depth < 32; depth++) {
            if (walk instanceof LoginRejectedException) {
                return false;
            }
            Throwable next = walk.getCause();
            if (next == walk) {
                break;
            }
            walk = next;
        }
        return true;
    }

    /** One reconnect attempt, aimed wherever the consumer last pointed us. */
    private void connectOnce() {
        ServerAddress address = target.get();
        if (address == null) {
            client.connect(credentials.username(), credentials.password(), CancellationSignal.none());
        } else {
            client.connect(
                    address.host(),
                    address.port(),
                    credentials.username(),
                    credentials.password(),
                    CancellationSignal.none());
        }
    }

    /**
     * Runs a consumer-initiated connect, keeping its contract and arming the
     * supervisor if it fails.
     *
     * <p>The call still blocks and still throws, because {@link Connection}
     * documents that it does. Arming as well is what turns a transient failure
     * at startup — a DNS lookup that fails twenty seconds after boot — from a
     * process that is offline until someone restarts it into one that comes back
     * on its own.
     */
    private void consumerConnect(ServerAddress address, Runnable attempt) {
        reconnects.cancel();
        target.set(address);
        disconnected.set(false);
        connecting.set(true);
        try {
            attempt.run();
        } catch (RuntimeException failure) {
            if (retryable(failure) && !(failure instanceof IllegalStateException)) {
                reconnects.arm(failure);
            }
            throw failure;
        } finally {
            connecting.set(false);
        }
    }

    private ServerInfo serverInfo() {
        dev.slsk.internal.connection.ServerInfo source = client.getServerInfo();
        if (source == null) {
            return ServerInfo.empty();
        }
        return new ServerInfo(
                source.getParentMinSpeed() == null ? OptionalInt.empty() : OptionalInt.of(source.getParentMinSpeed()),
                source.getParentSpeedRatio() == null
                        ? OptionalInt.empty()
                        : OptionalInt.of(source.getParentSpeedRatio()),
                source.getWishlistInterval() == null
                        ? Optional.empty()
                        : Optional.of(Duration.ofSeconds(source.getWishlistInterval())),
                Optional.ofNullable(source.isSupporter()));
    }

    @Override
    public void connect() throws InterruptedException {
        BlockingInvocation.run(signal -> {
            consumerConnect(null, () -> client.connect(credentials.username(), credentials.password(), signal));
            return null;
        });
    }

    @Override
    public void connect(Duration timeout) throws InterruptedException, TimeoutException {
        BlockingInvocation.run(client.getScheduler(), timeout, signal -> {
            consumerConnect(null, () -> client.connect(credentials.username(), credentials.password(), signal));
            return null;
        });
    }

    @Override
    public void connect(ServerAddress address) throws InterruptedException {
        BlockingInvocation.run(signal -> {
            Objects.requireNonNull(address, "address");
            consumerConnect(
                    address,
                    () -> client.connect(
                            address.host(), address.port(), credentials.username(), credentials.password(), signal));
            return null;
        });
    }

    @Override
    public void connect(ServerAddress address, Duration timeout) throws InterruptedException, TimeoutException {
        BlockingInvocation.run(client.getScheduler(), timeout, signal -> {
            Objects.requireNonNull(address, "address");
            consumerConnect(
                    address,
                    () -> client.connect(
                            address.host(), address.port(), credentials.username(), credentials.password(), signal));
            return null;
        });
    }

    @Override
    public void disconnect(String reason) {
        disconnected.set(true);
        reconnects.cancel();
        client.disconnect(reason == null ? "disconnect requested" : reason);
    }

    @Override
    public ConnectionState state() {
        return mapState();
    }

    @Override
    public Optional<ServerInfo> server() {
        return state().isOnline() ? Optional.of(serverInfo()) : Optional.empty();
    }

    @Override
    public Duration ping() throws InterruptedException {
        return BlockingInvocation.run(signal -> ping(signal));
    }

    @Override
    public Duration ping(Duration timeout) throws InterruptedException, TimeoutException {
        return BlockingInvocation.run(client.getScheduler(), timeout, signal -> ping(signal));
    }

    private Duration ping(CancellationSignal signal) {
        Long milliseconds = server.pingServer(signal);
        return Duration.ofMillis(milliseconds == null ? 0L : milliseconds);
    }

    @Override
    public EventStream<ConnectionEvent> events() {
        return events;
    }

    @Override
    public Attachment<ConnectionState> attach(Consumer<ConnectionEvent> listener) {
        return events.attach(this::mapState, listener);
    }

    /**
     * Stops reconnecting, for a client on its way down.
     *
     * <p>Must run before the engine is closed. Closing the engine disconnects,
     * and a supervisor still listening would read that as a drop and start
     * retrying against a client that is being disposed.
     */
    void close() {
        disconnected.set(true);
        reconnects.close();
    }
}
