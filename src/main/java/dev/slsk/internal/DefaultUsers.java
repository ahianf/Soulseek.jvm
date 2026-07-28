// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.CancellationSignal;
import dev.slsk.EventStream;
import dev.slsk.UserInfo;
import dev.slsk.UserPresence;
import dev.slsk.UserStatistics;
import dev.slsk.UserStatus;
import dev.slsk.Username;
import dev.slsk.Users;
import dev.slsk.Watch;
import dev.slsk.events.UserEvent;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link Users} over the {@link SoulseekClient} seam.
 *
 * <p>The watch registry is the part worth reading. Soulseek's {@code AddUser} is
 * a server-side subscription with two properties that applications reliably get
 * wrong, and the old {@code watchUser} / {@code unwatchUser} pair exposed both.
 *
 * <p>It dies with the connection, so watches must be re-registered on every
 * login or status updates stop arriving after the first reconnect with nothing
 * to say so. {@link #reregister()} runs on login and re-adds all of them.
 *
 * <p>And it is one subscription per user, not per interested party, so two parts
 * of an application watching the same user share it and whichever unwatches
 * first breaks the other. Watches are reference-counted here, and the server is
 * told to stop only when the last one closes.
 */
final class DefaultUsers implements Users {

    private final SoulseekClient client;
    private final EventBus<UserEvent> events;
    private final DiagnosticSink diagnostics;

    /** Watched users, and how many holders each has. */
    private final Map<Username, Registration> watches = new ConcurrentHashMap<>();

    DefaultUsers(SoulseekClient client, EventBus<UserEvent> events, DiagnosticSink diagnostics) {
        this.client = Objects.requireNonNull(client, "client");
        this.events = Objects.requireNonNull(events, "events");
        this.diagnostics = Objects.requireNonNull(diagnostics, "diagnostics");
        client.addLoggedInListener((sender, ignored) -> reregister());
        client.addUserStatisticsChangedListener((sender, statistics) -> onStatistics(statistics));
    }

    /** One watched user: the last status seen, and how many holders remain. */
    private static final class Registration {
        private int holders;
        private volatile UserStatus status;

        Registration(UserStatus status) {
            this.status = status;
            this.holders = 0;
        }
    }

    /**
     * Re-adds every watch after a login, because the server forgot them all when
     * the connection went.
     */
    private void reregister() {
        for (Username user : Set.copyOf(watches.keySet())) {
            try {
                client.watchUser(user.value());
            } catch (RuntimeException exception) {
                diagnostics.warning("Failed to re-register the watch on " + user + " after login", exception);
            }
        }
    }

    private void onStatistics(dev.slsk.internal.UserStatistics source) {
        if (source == null || source.getUsername() == null) {
            return;
        }
        Username user = Username.of(source.getUsername());
        events.publish(new UserEvent.StatisticsChanged(user, statistics(source), Instant.now()));
    }

    private static UserStatistics statistics(dev.slsk.internal.UserStatistics source) {
        return new UserStatistics(
                Username.of(source.getUsername()),
                source.getAverageSpeed(),
                source.getUploadCount(),
                source.getFileCount(),
                source.getDirectoryCount());
    }

    private static UserStatus status(dev.slsk.internal.UserStatus source) {
        return new UserStatus(Username.of(source.getUsername()), presence(source.getPresence()), source.isPrivileged());
    }

    private static UserPresence presence(dev.slsk.internal.UserPresence source) {
        if (source == null) {
            return UserPresence.OFFLINE;
        }
        return switch (source) {
            case OFFLINE -> UserPresence.OFFLINE;
            case AWAY -> UserPresence.AWAY;
            case ONLINE -> UserPresence.ONLINE;
        };
    }

    @Override
    public UserInfo info(Username user, CancellationSignal signal) {
        Objects.requireNonNull(user, "user");
        Objects.requireNonNull(signal, "signal");
        dev.slsk.internal.UserInfo source = client.getUserInfo(user.value(), signal);
        return new UserInfo(
                user,
                source.getDescription() == null ? "" : source.getDescription(),
                Optional.empty(),
                source.getUploadSlots(),
                source.getQueueLength(),
                source.hasFreeUploadSlot());
    }

    @Override
    public UserStatistics statistics(Username user, CancellationSignal signal) {
        Objects.requireNonNull(user, "user");
        Objects.requireNonNull(signal, "signal");
        return statistics(client.getUserStatistics(user.value(), signal));
    }

    @Override
    public UserStatus status(Username user, CancellationSignal signal) {
        Objects.requireNonNull(user, "user");
        Objects.requireNonNull(signal, "signal");
        return status(client.getUserStatus(user.value(), signal));
    }

    @Override
    public InetSocketAddress endpoint(Username user, CancellationSignal signal) {
        Objects.requireNonNull(user, "user");
        Objects.requireNonNull(signal, "signal");
        return client.getUserEndpoint(user.value(), signal);
    }

    @Override
    public Watch watch(Username user) {
        Objects.requireNonNull(user, "user");
        Registration registration = watches.compute(user, (key, existing) -> {
            Registration current = existing;
            if (current == null) {
                current = new Registration(new UserStatus(key, UserPresence.OFFLINE, false));
                dev.slsk.internal.UserData data = client.watchUser(key.value());
                if (data != null) {
                    current.status = new UserStatus(key, presence(data.getStatus()), false);
                }
            }
            current.holders++;
            return current;
        });
        return new RefCountedWatch(user, registration);
    }

    @Override
    public Set<Username> watched() {
        return Set.copyOf(watches.keySet());
    }

    @Override
    public EventStream<UserEvent> events() {
        return events;
    }

    /** Releases the server-side subscription only when the last holder closes. */
    private final class RefCountedWatch implements Watch {

        private final Username user;
        private final Registration registration;
        private final AtomicBoolean closed = new AtomicBoolean();

        RefCountedWatch(Username user, Registration registration) {
            this.user = user;
            this.registration = registration;
        }

        @Override
        public Username user() {
            return user;
        }

        @Override
        public UserStatus status() {
            return registration.status;
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            watches.computeIfPresent(user, (key, current) -> {
                if (--current.holders > 0) {
                    return current;
                }
                try {
                    client.unwatchUser(key.value());
                } catch (RuntimeException exception) {
                    diagnostics.warning("Failed to release the watch on " + key, exception);
                }
                return null;
            });
        }
    }
}
