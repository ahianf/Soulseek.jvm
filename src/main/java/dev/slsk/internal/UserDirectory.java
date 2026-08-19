// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.CancellationSignal;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.UserEndpointCacheException;
import dev.slsk.exceptions.UserEndpointException;
import dev.slsk.exceptions.UserNotFoundException;
import dev.slsk.exceptions.UserOfflineException;
import dev.slsk.internal.common.CacheLookupResult;
import dev.slsk.internal.common.CommonUtils;
import dev.slsk.internal.common.Constants;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.Permits;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.handlers.BrowseResponseConnection;
import dev.slsk.internal.messaging.messages.BrowseRequest;
import dev.slsk.internal.messaging.messages.FolderContentsRequest;
import dev.slsk.internal.messaging.messages.GivePrivilegesCommand;
import dev.slsk.internal.messaging.messages.UnwatchUserCommand;
import dev.slsk.internal.messaging.messages.UserAddressRequest;
import dev.slsk.internal.messaging.messages.UserAddressResponse;
import dev.slsk.internal.messaging.messages.UserInfoRequest;
import dev.slsk.internal.messaging.messages.UserPrivilegesRequest;
import dev.slsk.internal.messaging.messages.UserStatisticsRequest;
import dev.slsk.internal.messaging.messages.UserStatusRequest;
import dev.slsk.internal.messaging.messages.WatchUserRequest;
import dev.slsk.internal.messaging.messages.WatchUserResponse;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.options.BrowseOptions;
import dev.slsk.internal.share.BrowseResponse;
import dev.slsk.internal.share.Directory;
import dev.slsk.internal.user.UserData;
import dev.slsk.internal.user.UserEndpointCache;
import dev.slsk.internal.user.UserInfo;
import dev.slsk.internal.user.UserStatistics;
import dev.slsk.internal.user.UserStatus;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Everything the client knows about other users: info, statistics, presence,
 * privileges, endpoint resolution and browsing their shares.
 *
 * <p>The second piece lifted out of {@code SoulseekEngine}. These
 * operations are peer-facing rather than server-facing, but they share the same
 * correlate-and-translate shape. What they still reach the engine for is the
 * peer connection manager, the correlator and the browse-progress event; the
 * first two are ports of their own and the third is the engine's own event
 * machinery.
 */
final class UserDirectory {

    private final SoulseekEngine context;
    private final ServerLink server;

    /**
     * Serialises endpoint lookups per user, so concurrent callers asking about
     * the same peer issue one request rather than several. Owned here now that
     * endpoint resolution lives here.
     */
    private final java.util.Map<String, java.util.concurrent.Semaphore> userEndpointSemaphores =
            new java.util.concurrent.ConcurrentHashMap<>();

    private final java.util.concurrent.Semaphore userEndpointSemaphoreSyncRoot = new java.util.concurrent.Semaphore(1);

    UserDirectory(SoulseekEngine context, ServerLink server) {
        this.context = Objects.requireNonNull(context, "context");
        this.server = Objects.requireNonNull(server, "server");
    }

    UserInfo getUserInfo(String requestedUsername) {
        return getUserInfo(requestedUsername, CancellationSignal.none());
    }

    UserInfo getUserInfo(String requestedUsername, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        server.requireLoggedIn("fetch user information");
        CancellationSignal token = CommonUtils.token(cancellationSignal);
        try {
            Wait<UserInfo> infoWait = context.getWaiter()
                    .register(
                            new WaitKey(MessageCode.Peer.INFO_RESPONSE, requestedUsername),
                            UserInfo.class,
                            null,
                            token);
            InetSocketAddress endpoint = getUserEndpoint(requestedUsername, token);
            MessageConnection connection =
                    context.getPeerConnectionManager().getOrAddMessageConnection(requestedUsername, endpoint, token);
            connection.write(new UserInfoRequest(), CommonUtils.token(token));
            return infoWait.await();
        } catch (Throwable failure) {
            throw Failures.raise(
                    failure,
                    "Failed to retrieve information for user " + requestedUsername + ": ",
                    UserOfflineException.class);
        }
    }

    Boolean getUserPrivileged(String requestedUsername) {
        return getUserPrivileged(requestedUsername, CancellationSignal.none());
    }

    Boolean getUserPrivileged(String requestedUsername, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        server.requireLoggedIn("check user privileges");
        return server.request(
                new UserPrivilegesRequest(requestedUsername),
                new WaitKey(MessageCode.Server.USER_PRIVILEGES, requestedUsername),
                Boolean.class,
                cancellationSignal,
                "Failed to get privileges for " + requestedUsername + ": ",
                UserOfflineException.class);
    }

    UserStatistics getUserStatistics(String requestedUsername) {
        return getUserStatistics(requestedUsername, CancellationSignal.none());
    }

    UserStatistics getUserStatistics(String requestedUsername, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        server.requireLoggedIn("fetch user statistics");
        return server.request(
                new UserStatisticsRequest(requestedUsername),
                new WaitKey(MessageCode.Server.GET_USER_STATS, requestedUsername),
                UserStatistics.class,
                cancellationSignal,
                "Failed to retrieve statistics for user " + requestedUsername + ": ");
    }

    UserStatus getUserStatus(String requestedUsername) {
        return getUserStatus(requestedUsername, CancellationSignal.none());
    }

    UserStatus getUserStatus(String requestedUsername, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        server.requireLoggedIn("fetch user status");
        return server.request(
                new UserStatusRequest(requestedUsername),
                new WaitKey(MessageCode.Server.GET_STATUS, requestedUsername),
                UserStatus.class,
                cancellationSignal,
                "Failed to retrieve status for user " + requestedUsername + ": ",
                UserOfflineException.class);
    }

    UserData watchUser(String requestedUsername) {
        return watchUser(requestedUsername, CancellationSignal.none());
    }

    UserData watchUser(String requestedUsername, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        server.requireLoggedIn("add users");
        WatchUserResponse response = server.request(
                new WatchUserRequest(requestedUsername),
                new WaitKey(MessageCode.Server.WATCH_USER, requestedUsername),
                WatchUserResponse.class,
                cancellationSignal,
                "Failed to watch user " + requestedUsername + ": ",
                UserNotFoundException.class);
        if (!response.isExists()) {
            throw new UserNotFoundException("User " + requestedUsername + " does not exist");
        }
        return response.getUserData();
    }

    void unwatchUser(String requestedUsername) {
        unwatchUser(requestedUsername, CancellationSignal.none());
    }

    void unwatchUser(String requestedUsername, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        server.requireLoggedIn("add users");
        try {
            server.write(new UnwatchUserCommand(requestedUsername), CommonUtils.token(cancellationSignal));
        } catch (Throwable failure) {
            throw Failures.raise(failure, "Failed to unwatch user " + requestedUsername + ": ");
        }
    }

    void grantUserPrivileges(String requestedUsername, int days) {
        grantUserPrivileges(requestedUsername, days, CancellationSignal.none());
    }

    void grantUserPrivileges(String requestedUsername, int days, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        if (days <= 0) {
            throw new IllegalArgumentException("The number of days granted must be greater than zero");
        }
        server.requireLoggedIn("grant user privileges");
        try {
            server.write(new GivePrivilegesCommand(requestedUsername, days), CommonUtils.token(cancellationSignal));
        } catch (Throwable failure) {
            throw Failures.raise(
                    failure, "Failed to grant " + days + " days of privileges to " + requestedUsername + ": ");
        }
    }

    BrowseResponse browse(String requestedUsername) {
        return browse(requestedUsername, null, CancellationSignal.none());
    }

    BrowseResponse browse(String requestedUsername, BrowseOptions browseOptions) {
        return browse(requestedUsername, browseOptions, CancellationSignal.none());
    }

    BrowseResponse browse(String requestedUsername, CancellationSignal cancellationSignal) {
        return browse(requestedUsername, null, cancellationSignal);
    }

    BrowseResponse browse(
            String requestedUsername, BrowseOptions browseOptions, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        server.requireLoggedIn("browse");
        BrowseOptions operationOptions = browseOptions == null ? new BrowseOptions() : browseOptions;
        CancellationSignal token = CommonUtils.token(cancellationSignal);
        WaitKey browseWaitKey = new WaitKey(MessageCode.Peer.BROWSE_RESPONSE, requestedUsername);
        try {
            Wait<BrowseResponse> browseWait =
                    context.getWaiter().registerIndefinitely(browseWaitKey, BrowseResponse.class, token);
            Wait<BrowseResponseConnection> connectionWait = context.getWaiter()
                    .register(
                            new WaitKey(Constants.WaitKey.BROWSE_RESPONSE_CONNECTION, requestedUsername),
                            BrowseResponseConnection.class,
                            operationOptions.getResponseTimeout(),
                            token);

            BrowseResponseConnection responseConnection;
            try {
                InetSocketAddress endpoint = getUserEndpoint(requestedUsername, token);
                MessageConnection peer = context.getPeerConnectionManager()
                        .getOrAddMessageConnection(requestedUsername, endpoint, token);
                peer.write(new BrowseRequest(), CommonUtils.token(token));
                responseConnection = connectionWait.await();
            } catch (Throwable failure) {
                // The browse wait has no deadline, so nothing else would ever
                // release it if getting to the peer failed.
                Throwable cause = Failures.unwrap(failure);
                context.getWaiter().fail(browseWaitKey, cause);
                throw Failures.propagate(cause);
            }

            MessageConnection connection = responseConnection.connection();
            long responseLength = responseConnection.eventData().getLength() - 4;
            AtomicBoolean completionEventFired = new AtomicBoolean();
            dev.slsk.internal.network.MessageConnectionEventListener<dev.slsk.internal.network.MessageDataEvent>
                    progressListener = (sender, eventData) -> context.reportBrowseProgress(
                    requestedUsername,
                    operationOptions,
                    eventData.getCurrentLength(),
                    eventData.getTotalLength(),
                    completionEventFired);
            connection.addDisconnectedListener((sender, eventData) -> context.getWaiter()
                    .fail(
                            browseWaitKey,
                            new ConnectionException(
                                    "Peer connection disconnected " + "unexpectedly: " + eventData.getMessage(),
                                    eventData.getException())));
            connection.addMessageDataReadListener(progressListener);
            context.reportBrowseProgress(requestedUsername, operationOptions, 0, responseLength, completionEventFired);
            BrowseResponse response = browseWait.await();
            connection.removeMessageDataReadListener(progressListener);
            if (!completionEventFired.get()) {
                context.reportBrowseProgress(
                        requestedUsername, operationOptions, responseLength, responseLength, completionEventFired);
            }
            return response;
        } catch (Throwable failure) {
            throw Failures.raise(
                    failure, "Failed to browse user " + requestedUsername + ": ", UserOfflineException.class);
        }
    }

    void connectToUser(String requestedUsername) {
        connectToUser(requestedUsername, false, CancellationSignal.none());
    }

    void connectToUser(String requestedUsername, boolean invalidateCache) {
        connectToUser(requestedUsername, invalidateCache, CancellationSignal.none());
    }

    void connectToUser(String requestedUsername, CancellationSignal cancellationSignal) {
        connectToUser(requestedUsername, false, cancellationSignal);
    }

    void connectToUser(String requestedUsername, boolean invalidateCache, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        server.requireLoggedIn("connect to other users");
        CancellationSignal token = CommonUtils.token(cancellationSignal);
        try {
            InetSocketAddress endpoint = getUserEndpoint(requestedUsername, token);
            if (invalidateCache
                    && context.getPeerConnectionManager().tryInvalidateMessageConnectionCache(requestedUsername)) {
                context.getDiagnostic().debug("Invalidated message connection cache for " + requestedUsername);
            }
            context.getPeerConnectionManager().getOrAddMessageConnection(requestedUsername, endpoint, token);
        } catch (Throwable failure) {
            throw Failures.raise(
                    failure, "Failed to connect to user " + requestedUsername + ": ", UserOfflineException.class);
        }
    }

    InetSocketAddress getUserEndpoint(String requestedUsername) {
        return getUserEndpoint(requestedUsername, CancellationSignal.none());
    }

    InetSocketAddress getUserEndpoint(String requestedUsername, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        server.requireLoggedIn("fetch user endpoint");
        CancellationSignal token = CommonUtils.token(cancellationSignal);
        UserEndpointCache cache = context.getClientOptions().getUserEndpointCache();
        if (cache == null) {
            return retrieveUserEndpoint(requestedUsername, token, null);
        }

        CacheLookupResult<InetSocketAddress> cached = tryCacheGet(cache, requestedUsername);
        if (cached.found()) {
            context.getDiagnostic().debug("Endpoint cache HIT for " + requestedUsername + ": " + cached.value());
            return cached.value();
        }

        // The source serializes same-user lookups only when a cache is configured, so the first
        // caller populates it and the rest read it back. Each caller still issues its own request
        // under its own cancellation signal; sharing one in-flight request would let one caller's
        // cancellation or failure surface in another's.
        Semaphore semaphore;
        try {
            Permits.acquire(userEndpointSemaphoreSyncRoot, token);
        } catch (InterruptedException interrupted) {
            throw new CancellationException("The endpoint-cache lookup was interrupted");
        }
        try {
            semaphore = userEndpointSemaphores.computeIfAbsent(requestedUsername, ignored -> new Semaphore(1));
        } finally {
            userEndpointSemaphoreSyncRoot.release();
        }

        // The permit is released only on the path that acquired it; a cancelled acquisition must
        // not release a permit it never held, which is why the acquire is outside the try.
        try {
            Permits.acquire(semaphore, token);
        } catch (InterruptedException interrupted) {
            throw new CancellationException("The endpoint lookup was interrupted");
        }

        try {
            CacheLookupResult<InetSocketAddress> second = tryCacheGet(cache, requestedUsername);
            if (second.found()) {
                context.getDiagnostic().debug("Endpoint cache HIT for " + requestedUsername + ": " + second.value());
                return second.value();
            }
            return retrieveUserEndpoint(requestedUsername, token, cache);
        } finally {
            semaphore.release();
        }
    }

    InetSocketAddress retrieveUserEndpoint(
            String requestedUsername, CancellationSignal cancellationSignal, UserEndpointCache cache) {
        try {
            Wait<UserAddressResponse> wait = context.getWaiter()
                    .register(
                            new dev.slsk.internal.common.WaitKey(
                                    MessageCode.Server.GET_PEER_ADDRESS, requestedUsername),
                            UserAddressResponse.class,
                            null,
                            cancellationSignal);
            server.write(new UserAddressRequest(requestedUsername), cancellationSignal);
            UserAddressResponse response = wait.await();
            if (response.getIpAddress().isAnyLocalAddress()) {
                throw new UserOfflineException("User " + requestedUsername + " appears to be offline");
            }
            InetSocketAddress result = response.getIpEndpoint();
            if (cache != null) {
                try {
                    cache.put(requestedUsername, result);
                } catch (Throwable failure) {
                    throw new UserEndpointCacheException(
                            "Exception retrieving or updating user " + "endpoint cache: " + Failures.message(failure),
                            failure);
                }
                context.getDiagnostic().debug("Endpoint cache MISS for " + requestedUsername + ": " + result);
            }
            return result;
        } catch (Throwable failure) {
            throw raiseUserEndpointFailure(failure, requestedUsername);
        }
    }

    List<Directory> getDirectoryContents(String requestedUsername, String directoryName) {
        return getDirectoryContents(requestedUsername, directoryName, null, CancellationSignal.none());
    }

    List<Directory> getDirectoryContents(String requestedUsername, String directoryName, int operationToken) {
        return getDirectoryContents(requestedUsername, directoryName, operationToken, CancellationSignal.none());
    }

    List<Directory> getDirectoryContents(
            String requestedUsername, String directoryName, CancellationSignal cancellationSignal) {
        return getDirectoryContents(requestedUsername, directoryName, null, cancellationSignal);
    }

    List<Directory> getDirectoryContents(
            String requestedUsername,
            String directoryName,
            Integer operationToken,
            CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        CommonUtils.requireText(directoryName, "directoryName");
        server.requireLoggedIn("fetch directory contents");
        int tokenValue = operationToken == null ? context.getTokenFactory().nextToken() : operationToken;
        CancellationSignal token = CommonUtils.token(cancellationSignal);
        try {
            @SuppressWarnings("unchecked")
            Wait<List<Directory>> contentsWait = (Wait<List<Directory>>) (Wait<?>) context.getWaiter()
                    .register(
                            new WaitKey(MessageCode.Peer.FOLDER_CONTENTS_RESPONSE, requestedUsername, tokenValue),
                            List.class,
                            null,
                            token);
            InetSocketAddress endpoint = getUserEndpoint(requestedUsername, token);
            MessageConnection connection =
                    context.getPeerConnectionManager().getOrAddMessageConnection(requestedUsername, endpoint, token);
            connection.write(new FolderContentsRequest(tokenValue, directoryName), CommonUtils.token(token));
            return Collections.unmodifiableList(new ArrayList<>(contentsWait.await()));
        } catch (Throwable failure) {
            throw Failures.raise(
                    failure,
                    "Failed to retrieve directory contents for " + directoryName + " from " + requestedUsername + ": ",
                    UserOfflineException.class);
        }
    }

    static RuntimeException raiseUserEndpointFailure(Throwable failure, String requestedUsername) {
        Throwable cause = Failures.unwrap(failure);
        if (cause instanceof UserOfflineException
                || cause instanceof UserEndpointCacheException
                || cause instanceof CancellationException
                || cause instanceof TimeoutException) {
            throw Failures.surface(cause);
        }
        throw new UserEndpointException(
                "Failed to retrieve endpoint for user " + requestedUsername + ": " + Failures.message(cause), cause);
    }

    static CacheLookupResult<InetSocketAddress> tryCacheGet(UserEndpointCache cache, String requestedUsername) {
        try {
            return cache.lookup(requestedUsername);
        } catch (Throwable failure) {
            throw new UserEndpointCacheException(
                    "Exception retrieving or updating user endpoint cache: " + Failures.message(failure), failure);
        }
    }
    /**
     * Releases per-user endpoint semaphores that nobody is waiting on.
     *
     * <p>Runs periodically from the client's shared timer. Skips any semaphore
     * currently held rather than blocking on it, so a lookup in flight is never
     * disturbed.
     *
     */
    void cleanupUserEndpointSemaphores() {
        if (!userEndpointSemaphoreSyncRoot.tryAcquire()) {
            return;
        }
        try {
            for (java.util.Map.Entry<String, java.util.concurrent.Semaphore> entry :
                    userEndpointSemaphores.entrySet()) {
                java.util.concurrent.Semaphore semaphore = entry.getValue();
                if (!semaphore.tryAcquire()) {
                    continue;
                }
                if (userEndpointSemaphores.remove(entry.getKey(), semaphore)) {
                    // Released even though it just left the map: a caller that
                    // fetched this semaphore before the removal may be about
                    // to block on it, and a removed semaphore nobody releases
                    // would strand that caller forever. The cost is one
                    // unserialized duplicate lookup in a tiny window; the
                    // alternative was an unbounded hang.
                    semaphore.release();
                    context.getDiagnostic().debug("Cleaned up user endpoint semaphore for " + entry.getKey());
                } else {
                    semaphore.release();
                }
            }
        } finally {
            userEndpointSemaphoreSyncRoot.release();
        }
    }

    /** Exposes the per-user endpoint semaphores for the client's test accessor. */
    java.util.Map<String, java.util.concurrent.Semaphore> getUserEndpointSemaphores() {
        return userEndpointSemaphores;
    }
}
