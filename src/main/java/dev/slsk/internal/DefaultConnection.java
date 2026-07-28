// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.Attachment;
import dev.slsk.CancellationSignal;
import dev.slsk.Connection;
import dev.slsk.ConnectionState;
import dev.slsk.EventStream;
import dev.slsk.ServerAddress;
import dev.slsk.ServerInfo;
import dev.slsk.events.ConnectionEvent;
import dev.slsk.internal.common.Blocking;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
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

    private final DefaultSoulseekClient client;
    private final ServerSession server;
    private final Credentials credentials;
    private final EventBus<ConnectionEvent> events;

    /** When the current session began; {@code null} whenever not logged in. */
    private final AtomicReference<Instant> onlineSince = new AtomicReference<>();

    /** The last state we published, so a transition can report what it came from. */
    private final AtomicReference<ConnectionState> published = new AtomicReference<>(new ConnectionState.Offline());

    DefaultConnection(DefaultSoulseekClient client, Credentials credentials, EventBus<ConnectionEvent> events) {
        this.client = Objects.requireNonNull(client, "client");
        this.server = client.server();
        this.credentials = Objects.requireNonNull(credentials, "credentials");
        this.events = Objects.requireNonNull(events, "events");
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
        client.addStateChangedListener((sender, event) -> onStateChanged());
        client.addConnectedListener((sender, event) -> onStateChanged());
        client.addDisconnectedListener((sender, event) -> onStateChanged());
        client.addLoggedInListener((sender, event) -> onStateChanged());
        client.addServerInfoReceivedListener(
                (sender, event) -> events.publish(new ConnectionEvent.ServerInfoReceived(serverInfo(), Instant.now())));
        client.addKickedFromServerListener((sender, event) ->
                events.publish(new ConnectionEvent.KickedFromServer("kicked from the server", Instant.now())));
        client.addGlobalMessageReceivedListener((sender, message) ->
                events.publish(new ConnectionEvent.GlobalMessageReceived(String.valueOf(message), Instant.now())));
        client.addExcludedSearchPhrasesReceivedListener(
                (sender, phrases) -> events.publish(new ConnectionEvent.ExcludedSearchPhrasesReceived(
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
            return new ConnectionState.Connecting(1);
        }
        return new ConnectionState.Offline();
    }

    private ServerInfo serverInfo() {
        dev.slsk.internal.ServerInfo source = client.getServerInfo();
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
    public void connect(CancellationSignal signal) {
        Objects.requireNonNull(signal, "signal");
        client.connect(credentials.username(), credentials.password(), signal);
    }

    @Override
    public void connect(ServerAddress address, CancellationSignal signal) {
        Objects.requireNonNull(address, "address");
        Objects.requireNonNull(signal, "signal");
        client.connect(address.host(), address.port(), credentials.username(), credentials.password(), signal);
    }

    @Override
    public void disconnect(String reason) {
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
    public Duration ping(CancellationSignal signal) {
        Objects.requireNonNull(signal, "signal");
        Long milliseconds = Blocking.await(server.pingServer(signal));
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
}
