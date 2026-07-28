// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.CancellationSignal;
import dev.slsk.EventStream;
import dev.slsk.Me;
import dev.slsk.ServerInfo;
import dev.slsk.UserPresence;
import dev.slsk.UserProfile;
import dev.slsk.Username;
import dev.slsk.events.MeEvent;
import dev.slsk.internal.EngineEvents.Kind;
import dev.slsk.internal.common.Blocking;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.events.PrivilegeNotificationReceivedEvent;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link Me}, over the engine.
 *
 * <p>Presence is tracked here because the protocol has no way to ask. {@code
 * setStatus} tells the server, and nothing reads it back, so a consumer that
 * wanted to render its own presence had to remember what it last set. Keeping
 * the last published value is the only way to answer {@code presence()} at all,
 * and it makes the setter idempotent — publishing the presence we already have
 * sends nothing and raises nothing.
 *
 * <p>Privilege notifications are acknowledged automatically, on the same rule as
 * private messages, so {@code acknowledgePrivilegeNotification} does not appear
 * on the surface. That is a capability absorbed rather than lost: there is no
 * reason for a consumer to decide whether to acknowledge a notification it has
 * already been handed.
 */
final class DefaultMe implements Me {

    private final SoulseekEngine client;
    private final ServerSession server;
    private final UserDirectory users;
    private final EventBus<MeEvent> events;
    private final DiagnosticSink diagnostics;
    private final Username username;

    /** The last presence we published; the protocol offers no way to read it back. */
    private final AtomicReference<UserPresence> presence = new AtomicReference<>(UserPresence.ONLINE);

    DefaultMe(SoulseekEngine client, Username username, EventBus<MeEvent> events, DiagnosticSink diagnostics) {
        this.client = Objects.requireNonNull(client, "client");
        this.server = client.server();
        this.users = client.users();
        this.username = Objects.requireNonNull(username, "username");
        this.events = Objects.requireNonNull(events, "events");
        this.diagnostics = Objects.requireNonNull(diagnostics, "diagnostics");
        wire();
    }

    private void wire() {
        client.events()
                .on(
                        Kind.LOGGED_IN,
                        (Void ignored) -> events.publish(new MeEvent.LoggedIn(ServerInfo.empty(), Instant.now())));
        client.events()
                .on(
                        Kind.PRIVILEGED_USER_LIST_RECEIVED,
                        (java.util.List<String> users) -> events.publish(new MeEvent.PrivilegedUserListReceived(
                                users == null
                                        ? List.of()
                                        : users.stream().map(Username::of).toList(),
                                Instant.now())));
        client.events().on(Kind.PRIVILEGE_NOTIFICATION_RECEIVED, this::onPrivilegeNotification);
    }

    /**
     * Publishes a privilege notification and acknowledges it.
     *
     * <p>Acknowledgement is on the same rule as a private message: the consumer
     * is handed the notification and never asked to confirm it, because a
     * consumer that forgets is redelivered everything forever.
     */
    private void onPrivilegeNotification(PrivilegeNotificationReceivedEvent event) {
        if (event == null) {
            return;
        }
        events.publish(new MeEvent.PrivilegeNotificationReceived(
                event.getUsername() == null ? null : Username.of(event.getUsername()), Instant.now()));
        if (event.isRequiresAcknowlegement() && event.getId() != null) {
            try {
                Blocking.await(server.acknowledgePrivilegeNotification(event.getId()));
            } catch (RuntimeException exception) {
                diagnostics.warning("Failed to acknowledge privilege notification " + event.getId(), exception);
            }
        }
    }

    @Override
    public Username username() {
        String current = client.getUsername();
        return current == null ? username : Username.of(current);
    }

    @Override
    public UserPresence presence() {
        return presence.get();
    }

    @Override
    public void presence(UserPresence value) {
        Objects.requireNonNull(value, "presence");
        UserPresence previous = presence.getAndSet(value);
        if (previous == value) {
            return;
        }
        Blocking.await(server.setStatus(map(value)));
        events.publish(new MeEvent.PresenceChanged(previous, value, Instant.now()));
    }

    private static dev.slsk.internal.UserPresence map(UserPresence presence) {
        return switch (presence) {
            case OFFLINE -> dev.slsk.internal.UserPresence.OFFLINE;
            case AWAY -> dev.slsk.internal.UserPresence.AWAY;
            case ONLINE -> dev.slsk.internal.UserPresence.ONLINE;
        };
    }

    @Override
    public UserProfile profile() {
        return client.getProfile();
    }

    @Override
    public void profile(UserProfile value) {
        Objects.requireNonNull(value, "profile");
        client.setProfile(value);
    }

    @Override
    public int privileges(CancellationSignal signal) {
        Objects.requireNonNull(signal, "signal");
        Integer days = Blocking.await(server.getPrivileges(signal));
        return days == null ? 0 : days;
    }

    @Override
    public void giftPrivileges(Username to, int days, CancellationSignal signal) {
        Objects.requireNonNull(to, "to");
        Objects.requireNonNull(signal, "signal");
        if (days <= 0) {
            throw new IllegalArgumentException("days must be positive: " + days);
        }
        Blocking.await(users.grantUserPrivileges(to.value(), days, signal));
    }

    @Override
    public void changePassword(String newPassword, CancellationSignal signal) {
        Objects.requireNonNull(newPassword, "newPassword");
        Objects.requireNonNull(signal, "signal");
        Blocking.await(server.changePassword(newPassword, signal));
    }

    @Override
    public void reportUploadSpeed(long bytesPerSecond) {
        if (bytesPerSecond < 0) {
            throw new IllegalArgumentException("bytesPerSecond must not be negative: " + bytesPerSecond);
        }
        Blocking.await(server.sendUploadSpeed((int) Math.min(bytesPerSecond, Integer.MAX_VALUE)));
    }

    @Override
    public EventStream<MeEvent> events() {
        return events;
    }
}
