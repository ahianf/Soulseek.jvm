// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.EventStream;
import dev.slsk.Users;
import dev.slsk.events.UserEvent;
import dev.slsk.internal.EngineEvents.Kind;
import dev.slsk.internal.common.Usernames;
import dev.slsk.internal.concurrent.BlockingInvocation;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.events.EventBus;
import dev.slsk.internal.options.BrowseOptions;
import dev.slsk.search.FileAttributes;
import dev.slsk.search.SearchFile;
import dev.slsk.share.Directory;
import dev.slsk.user.Browse;
import dev.slsk.user.BrowseProgress;
import dev.slsk.user.BrowseRequest;
import dev.slsk.user.UserInfo;
import dev.slsk.user.UserPresence;
import dev.slsk.user.UserStatistics;
import dev.slsk.user.UserStatus;
import dev.slsk.user.Username;
import dev.slsk.user.Watch;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Users}, over the engine.
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
    private static final Logger LOG = LoggerFactory.getLogger(DefaultUsers.class);

    private final SoulseekEngine client;
    private final UserDirectory directory;
    private final EventBus<UserEvent> events;

    /** Watched users, and how many holders each has. */
    private final Map<Username, Registration> watches = new ConcurrentHashMap<>();

    DefaultUsers(SoulseekEngine client, EventBus<UserEvent> events) {
        this.client = Objects.requireNonNull(client, "client");
        this.directory = client.users();
        this.events = Objects.requireNonNull(events, "events");
        client.events().on(Kind.LOGGED_IN, (Void ignored) -> reregister());
        client.events()
                .on(
                        Kind.USER_STATISTICS_CHANGED,
                        (dev.slsk.internal.user.UserStatisticsSnapshot statistics) -> onStatistics(statistics));
        // These two kinds had no subscriber at all, so Watch.status() returned
        // the login-time status forever and UserEvent.StatusChanged and
        // CannotConnect — both public promises — were never published. The
        // status updates are the entire point of the server-side subscription.
        client.events()
                .on(Kind.USER_STATUS_CHANGED, (dev.slsk.internal.user.UserStatusSnapshot status) -> onStatus(status));
        client.events()
                .on(
                        Kind.USER_CANNOT_CONNECT,
                        (dev.slsk.internal.events.UserCannotConnectEvent event) -> onCannotConnect(event));
    }

    /** Updates the watch's snapshot and publishes the transition. */
    private void onStatus(dev.slsk.internal.user.UserStatusSnapshot source) {
        Username user = source == null ? null : Usernames.fromWire(source.username());
        if (user == null) {
            return;
        }
        UserStatus to = status(source);
        Registration registration = watches.get(user);
        UserStatus from;
        if (registration != null) {
            from = registration.status;
            registration.status = to;
        } else {
            // A status answer for a user nobody watches — a getUserStatus call
            // resolved through the same wire message. There is no previous
            // status to report, so the transition is published as a no-change
            // from the same status.
            from = to;
        }
        events.publish(new UserEvent.StatusChanged(user, from, to, Instant.now()));
    }

    private void onCannotConnect(dev.slsk.internal.events.UserCannotConnectEvent source) {
        Username user = source == null ? null : Usernames.fromWire(source.username());
        if (user == null) {
            return;
        }
        events.publish(new UserEvent.CannotConnect(
                user, "the user could not be reached, directly or through the server", Instant.now()));
    }

    /** One watched user: the last status seen, and how many holders remain. */
    private static final class Registration {
        private int holders;
        private volatile UserStatus status;

        /** Set when the last holder retires this registration; guarded by its monitor. */
        private boolean removed;

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
                directory.watchUser(user.value());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted re-registering watches after login", interrupted);
                return;
            } catch (RuntimeException exception) {
                LOG.warn("Failed to re-register the watch on {} after login", user, exception);
            }
        }
    }

    private void onStatistics(dev.slsk.internal.user.UserStatisticsSnapshot source) {
        Username user = source == null ? null : Usernames.fromWire(source.username());
        if (user == null) {
            return;
        }
        events.publish(new UserEvent.StatisticsChanged(user, statistics(source), Instant.now()));
    }

    private static UserStatistics statistics(dev.slsk.internal.user.UserStatisticsSnapshot source) {
        return new UserStatistics(
                Username.of(source.username()),
                source.averageSpeed(),
                source.uploadCount(),
                source.fileCount(),
                source.directoryCount());
    }

    private static UserStatus status(dev.slsk.internal.user.UserStatusSnapshot source) {
        return new UserStatus(Username.of(source.username()), presence(source.presence()), source.privileged());
    }

    private static UserPresence presence(dev.slsk.internal.user.WireUserPresence source) {
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
    public UserInfo info(Username user) throws InterruptedException {
        return BlockingInvocation.run(signal -> info(user, signal));
    }

    @Override
    public UserInfo info(Username user, Duration timeout) throws InterruptedException, TimeoutException {
        return BlockingInvocation.run(client.getScheduler(), timeout, signal -> info(user, signal));
    }

    private UserInfo info(Username user, CancellationSignal signal) throws InterruptedException {
        Objects.requireNonNull(user, "user");
        dev.slsk.internal.user.UserInfoMessage source = directory.getUserInfo(user.value(), signal);
        return new UserInfo(
                user,
                source.description() == null ? "" : source.description(),
                Optional.empty(),
                source.uploadSlots(),
                source.queueLength(),
                source.freeUploadSlot());
    }

    @Override
    public UserStatistics statistics(Username user) throws InterruptedException {
        return BlockingInvocation.run(signal -> statistics(user, signal));
    }

    @Override
    public UserStatistics statistics(Username user, Duration timeout) throws InterruptedException, TimeoutException {
        return BlockingInvocation.run(client.getScheduler(), timeout, signal -> statistics(user, signal));
    }

    private UserStatistics statistics(Username user, CancellationSignal signal) throws InterruptedException {
        Objects.requireNonNull(user, "user");
        return statistics(directory.getUserStatistics(user.value(), signal));
    }

    @Override
    public UserStatus status(Username user) throws InterruptedException {
        return BlockingInvocation.run(signal -> status(user, signal));
    }

    @Override
    public UserStatus status(Username user, Duration timeout) throws InterruptedException, TimeoutException {
        return BlockingInvocation.run(client.getScheduler(), timeout, signal -> status(user, signal));
    }

    private UserStatus status(Username user, CancellationSignal signal) throws InterruptedException {
        Objects.requireNonNull(user, "user");
        return status(directory.getUserStatus(user.value(), signal));
    }

    @Override
    public InetSocketAddress endpoint(Username user) throws InterruptedException {
        return BlockingInvocation.run(signal -> endpoint(user, signal));
    }

    @Override
    public InetSocketAddress endpoint(Username user, Duration timeout) throws InterruptedException, TimeoutException {
        return BlockingInvocation.run(client.getScheduler(), timeout, signal -> endpoint(user, signal));
    }

    private InetSocketAddress endpoint(Username user, CancellationSignal signal) throws InterruptedException {
        Objects.requireNonNull(user, "user");
        return directory.getUserEndpoint(user.value(), signal);
    }

    @Override
    public Browse browse(BrowseRequest request) throws InterruptedException {
        return BlockingInvocation.run(signal -> browse(request, signal));
    }

    @Override
    public Browse browse(BrowseRequest request, Duration timeout) throws InterruptedException, TimeoutException {
        return BlockingInvocation.run(client.getScheduler(), timeout, signal -> browse(request, signal));
    }

    private Browse browse(BrowseRequest request, CancellationSignal signal) throws InterruptedException {
        Objects.requireNonNull(request, "request");
        BrowseOptions options = BrowseOptions.builder()
                .responseTimeout(request.timeout())
                .progressUpdated(request.onProgress()
                        .<Consumer<dev.slsk.internal.options.BrowseProgressUpdate>>map(
                                listener -> progress -> listener.accept(new BrowseProgress(
                                        request.user(), progress.bytesTransferred(), progress.size())))
                        .orElse(null))
                .build();
        dev.slsk.internal.share.BrowseResponseMessage response =
                directory.browse(request.user().value(), options, signal);
        return new Browse(
                request.user(),
                Instant.now(),
                directories(response.directories()),
                directories(response.lockedDirectories()));
    }

    @Override
    public List<Directory> directory(Username user, String path) throws InterruptedException {
        return BlockingInvocation.run(signal -> directory(user, path, signal));
    }

    @Override
    public List<Directory> directory(Username user, String path, Duration timeout)
            throws InterruptedException, TimeoutException {
        return BlockingInvocation.run(client.getScheduler(), timeout, signal -> directory(user, path, signal));
    }

    private List<Directory> directory(Username user, String path, CancellationSignal signal)
            throws InterruptedException {
        Objects.requireNonNull(user, "user");
        Objects.requireNonNull(path, "path");
        return directories(directory.getDirectoryContents(user.value(), path, null, signal));
    }

    /** The wire's directories, as the surface describes them. */
    private static List<Directory> directories(List<dev.slsk.internal.share.SharedDirectory> source) {
        if (source == null) {
            return List.of();
        }
        List<Directory> converted = new ArrayList<>(source.size());
        for (dev.slsk.internal.share.SharedDirectory entry : source) {
            List<SearchFile> files = new ArrayList<>(entry.files().size());
            for (dev.slsk.internal.share.File file : entry.files()) {
                files.add(new SearchFile(file.filename(), file.size(), FileAttributes.none()));
            }
            converted.add(new Directory(entry.name(), files));
        }
        return List.copyOf(converted);
    }

    @Override
    public Watch watch(Username user) {
        Objects.requireNonNull(user, "user");
        while (true) {
            Registration registration = watches.computeIfAbsent(
                    user, key -> new Registration(new UserStatus(key, UserPresence.OFFLINE, false)));
            synchronized (registration) {
                if (registration.removed) {
                    // A concurrent close retired this registration between the
                    // map read and the lock; it is no longer in the map.
                    continue;
                }
                if (registration.holders == 0) {
                    // The server round trip runs under this registration's own
                    // monitor, never the map's bin lock. A slow watch stalls
                    // only same-user callers — the serialization it needs
                    // anyway — instead of every watch that hashes nearby, and
                    // ConcurrentHashMap forbids blocking mapping functions.
                    try {
                        dev.slsk.internal.user.UserData data = directory.watchUser(user.value());
                        if (data != null) {
                            registration.status = new UserStatus(user, presence(data.status()), false);
                        }
                    } catch (InterruptedException interrupted) {
                        // watch() is a frozen no-throws signature; the interrupt
                        // stays visible on the flag and the failure keeps the
                        // shape this surface has always used for it.
                        Thread.currentThread().interrupt();
                        registration.removed = true;
                        watches.remove(user, registration);
                        throw new dev.slsk.exceptions.SoulseekClientException(
                                "Failed to watch user " + user + ": interrupted", interrupted);
                    } catch (RuntimeException failure) {
                        registration.removed = true;
                        watches.remove(user, registration);
                        throw failure;
                    }
                }
                registration.holders++;
                return new RefCountedWatch(user, registration);
            }
        }
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
            synchronized (registration) {
                if (--registration.holders > 0) {
                    return;
                }
                registration.removed = true;
                watches.remove(user, registration);
                try {
                    directory.unwatchUser(user.value());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    LOG.warn("Interrupted releasing the watch on {}", user, interrupted);
                } catch (RuntimeException exception) {
                    LOG.warn("Failed to release the watch on {}", user, exception);
                }
            }
        }
    }
}
