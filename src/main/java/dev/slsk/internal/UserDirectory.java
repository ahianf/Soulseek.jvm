// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.CancellationSignal;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.UserEndpointCacheException;
import dev.slsk.exceptions.UserEndpointException;
import dev.slsk.exceptions.UserNotFoundException;
import dev.slsk.exceptions.UserOfflineException;
import dev.slsk.internal.common.CommonUtils;
import dev.slsk.internal.common.Constants;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.Permits;
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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Everything the client knows about other users: info, statistics, presence,
 * privileges, endpoint resolution and browsing their shares.
 *
 * <p>The second piece lifted out of {@code SoulseekEngine}. These
 * operations are peer-facing rather than server-facing, but they share the same
 * correlate-and-translate shape, so they reach the rest of the client through
 * {@link EngineContext} like the rooms do.
 */
final class UserDirectory {

    private final EngineContext context;

    /**
     * Serialises endpoint lookups per user, so concurrent callers asking about
     * the same peer issue one request rather than several. Owned here now that
     * endpoint resolution lives here.
     */
    private final java.util.Map<String, java.util.concurrent.Semaphore> userEndpointSemaphores =
            new java.util.concurrent.ConcurrentHashMap<>();

    private final java.util.concurrent.Semaphore userEndpointSemaphoreSyncRoot = new java.util.concurrent.Semaphore(1);

    UserDirectory(EngineContext context) {
        this.context = Objects.requireNonNull(context, "context");
    }

    CompletableFuture<UserInfo> getUserInfo(String requestedUsername) {
        return getUserInfo(requestedUsername, CancellationSignal.none());
    }

    CompletableFuture<UserInfo> getUserInfo(String requestedUsername, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        context.requireLoggedIn("fetch user information");
        CancellationSignal token = context.defaultToken(cancellationSignal);
        CompletableFuture<UserInfo> infoWait;
        try {
            infoWait = context.getWaiter()
                    .waitAsync(
                            new WaitKey(MessageCode.Peer.INFO_RESPONSE, requestedUsername),
                            UserInfo.class,
                            null,
                            token);
        } catch (Throwable failure) {
            return Failures.map(
                    CompletableFuture.failedFuture(failure),
                    "Failed to retrieve information for user " + requestedUsername + ": ",
                    UserOfflineException.class);
        }
        CompletableFuture<UserInfo> operation = getUserEndpoint(requestedUsername, token)
                .thenCompose(endpoint -> context.getPeerConnectionManager()
                        .getOrAddMessageConnectionAsync(requestedUsername, endpoint, token))
                .thenCompose(connection -> context.writeToPeer(connection, new UserInfoRequest(), token))
                .thenCompose(ignored -> infoWait);
        return Failures.map(
                operation,
                "Failed to retrieve information for user " + requestedUsername + ": ",
                UserOfflineException.class);
    }

    CompletableFuture<Boolean> getUserPrivileged(String requestedUsername) {
        return getUserPrivileged(requestedUsername, CancellationSignal.none());
    }

    CompletableFuture<Boolean> getUserPrivileged(String requestedUsername, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        context.requireLoggedIn("check user privileges");
        return context.executeCorrelatedRequest(
                new UserPrivilegesRequest(requestedUsername),
                new WaitKey(MessageCode.Server.USER_PRIVILEGES, requestedUsername),
                Boolean.class,
                cancellationSignal,
                "Failed to get privileges for " + requestedUsername + ": ",
                UserOfflineException.class);
    }

    CompletableFuture<UserStatistics> getUserStatistics(String requestedUsername) {
        return getUserStatistics(requestedUsername, CancellationSignal.none());
    }

    CompletableFuture<UserStatistics> getUserStatistics(
            String requestedUsername, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        context.requireLoggedIn("fetch user statistics");
        return context.executeCorrelatedRequest(
                new UserStatisticsRequest(requestedUsername),
                new WaitKey(MessageCode.Server.GET_USER_STATS, requestedUsername),
                UserStatistics.class,
                cancellationSignal,
                "Failed to retrieve statistics for user " + context.getLoggedInUsername() + ": ");
    }

    CompletableFuture<UserStatus> getUserStatus(String requestedUsername) {
        return getUserStatus(requestedUsername, CancellationSignal.none());
    }

    CompletableFuture<UserStatus> getUserStatus(String requestedUsername, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        context.requireLoggedIn("fetch user status");
        return context.executeCorrelatedRequest(
                new UserStatusRequest(requestedUsername),
                new WaitKey(MessageCode.Server.GET_STATUS, requestedUsername),
                UserStatus.class,
                cancellationSignal,
                "Failed to retrieve status for user " + context.getLoggedInUsername() + ": ",
                UserOfflineException.class);
    }

    CompletableFuture<UserData> watchUser(String requestedUsername) {
        return watchUser(requestedUsername, CancellationSignal.none());
    }

    CompletableFuture<UserData> watchUser(String requestedUsername, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        context.requireLoggedIn("add users");
        return context.executeCorrelatedRequest(
                        new WatchUserRequest(requestedUsername),
                        new WaitKey(MessageCode.Server.WATCH_USER, requestedUsername),
                        WatchUserResponse.class,
                        cancellationSignal,
                        "Failed to watch user " + requestedUsername + ": ",
                        UserNotFoundException.class)
                .thenApply(response -> {
                    if (!response.isExists()) {
                        throw new UserNotFoundException("User " + requestedUsername + " does not exist");
                    }
                    return response.getUserData();
                });
    }

    CompletableFuture<Void> unwatchUser(String requestedUsername) {
        return unwatchUser(requestedUsername, CancellationSignal.none());
    }

    CompletableFuture<Void> unwatchUser(String requestedUsername, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        context.requireLoggedIn("add users");
        return Failures.map(
                context.writeToServer(
                        new UnwatchUserCommand(requestedUsername), context.defaultToken(cancellationSignal)),
                "Failed to unwatch user " + requestedUsername + ": ");
    }

    CompletableFuture<Void> grantUserPrivileges(String requestedUsername, int days) {
        return grantUserPrivileges(requestedUsername, days, CancellationSignal.none());
    }

    CompletableFuture<Void> grantUserPrivileges(
            String requestedUsername, int days, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        if (days <= 0) {
            throw new IllegalArgumentException("The number of days granted must be greater than zero");
        }
        context.requireLoggedIn("grant user privileges");
        return Failures.map(
                context.writeToServer(
                        new GivePrivilegesCommand(requestedUsername, days), context.defaultToken(cancellationSignal)),
                "Failed to grant " + days + " days of privileges to " + requestedUsername + ": ");
    }

    CompletableFuture<BrowseResponse> browse(String requestedUsername) {
        return browse(requestedUsername, null, CancellationSignal.none());
    }

    CompletableFuture<BrowseResponse> browse(String requestedUsername, BrowseOptions browseOptions) {
        return browse(requestedUsername, browseOptions, CancellationSignal.none());
    }

    CompletableFuture<BrowseResponse> browse(String requestedUsername, CancellationSignal cancellationSignal) {
        return browse(requestedUsername, null, cancellationSignal);
    }

    CompletableFuture<BrowseResponse> browse(
            String requestedUsername, BrowseOptions browseOptions, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        context.requireLoggedIn("browse");
        BrowseOptions operationOptions = browseOptions == null ? new BrowseOptions() : browseOptions;
        CancellationSignal token = context.defaultToken(cancellationSignal);
        WaitKey browseWaitKey = new WaitKey(MessageCode.Peer.BROWSE_RESPONSE, requestedUsername);
        CompletableFuture<BrowseResponse> browseWait;
        CompletableFuture<BrowseResponseConnection> connectionWait;
        try {
            browseWait = context.getWaiter().waitIndefinitelyAsync(browseWaitKey, BrowseResponse.class, token);
            connectionWait = context.getWaiter()
                    .waitAsync(
                            new WaitKey(Constants.WaitKey.BROWSE_RESPONSE_CONNECTION, requestedUsername),
                            BrowseResponseConnection.class,
                            operationOptions.getResponseTimeout(),
                            token);
        } catch (Throwable failure) {
            return Failures.map(
                    CompletableFuture.failedFuture(failure),
                    "Failed to browse user " + requestedUsername + ": ",
                    UserOfflineException.class);
        }

        CompletableFuture<BrowseResponseConnection> setup = getUserEndpoint(requestedUsername, token)
                .thenCompose(endpoint -> context.getPeerConnectionManager()
                        .getOrAddMessageConnectionAsync(requestedUsername, endpoint, token))
                .thenCompose(connection -> context.writeToPeer(connection, new BrowseRequest(), token))
                .thenCompose(ignored -> connectionWait);
        CompletableFuture<BrowseResponse> operation = setup.handle((responseConnection, failure) -> {
                    if (failure == null) {
                        return responseConnection;
                    }
                    Throwable cause = Failures.unwrap(failure);
                    context.getWaiter().fail(browseWaitKey, cause);
                    throw new CompletionException(cause);
                })
                .thenCompose(responseConnection -> {
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
                    context.reportBrowseProgress(
                            requestedUsername, operationOptions, 0, responseLength, completionEventFired);
                    return browseWait.thenApply(response -> {
                        connection.removeMessageDataReadListener(progressListener);
                        if (!completionEventFired.get()) {
                            context.reportBrowseProgress(
                                    requestedUsername,
                                    operationOptions,
                                    responseLength,
                                    responseLength,
                                    completionEventFired);
                        }
                        return response;
                    });
                });
        return Failures.map(operation, "Failed to browse user " + requestedUsername + ": ", UserOfflineException.class);
    }

    CompletableFuture<Void> connectToUser(String requestedUsername) {
        return connectToUser(requestedUsername, false, CancellationSignal.none());
    }

    CompletableFuture<Void> connectToUser(String requestedUsername, boolean invalidateCache) {
        return connectToUser(requestedUsername, invalidateCache, CancellationSignal.none());
    }

    CompletableFuture<Void> connectToUser(String requestedUsername, CancellationSignal cancellationSignal) {
        return connectToUser(requestedUsername, false, cancellationSignal);
    }

    CompletableFuture<Void> connectToUser(
            String requestedUsername, boolean invalidateCache, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        context.requireLoggedIn("connect to other users");
        CancellationSignal token = context.defaultToken(cancellationSignal);
        CompletableFuture<Void> operation = getUserEndpoint(requestedUsername, token)
                .thenCompose(endpoint -> {
                    if (invalidateCache
                            && context.getPeerConnectionManager()
                                    .tryInvalidateMessageConnectionCache(requestedUsername)) {
                        context.getDiagnostic().debug("Invalidated message connection cache for " + requestedUsername);
                    }
                    return context.getPeerConnectionManager()
                            .getOrAddMessageConnectionAsync(requestedUsername, endpoint, token)
                            .thenApply(ignored -> null);
                });
        return Failures.map(
                operation, "Failed to connect to user " + requestedUsername + ": ", UserOfflineException.class);
    }

    CompletableFuture<InetSocketAddress> getUserEndpoint(String requestedUsername) {
        return getUserEndpoint(requestedUsername, CancellationSignal.none());
    }

    CompletableFuture<InetSocketAddress> getUserEndpoint(
            String requestedUsername, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        context.requireLoggedIn("fetch user endpoint");
        CancellationSignal token = context.defaultToken(cancellationSignal);
        UserEndpointCache cache = context.getClientOptions().getUserEndpointCache();
        if (cache == null) {
            return retrieveUserEndpoint(requestedUsername, token, null);
        }

        CacheLookupResult<InetSocketAddress> cached = tryCacheGet(cache, requestedUsername);
        if (cached.found()) {
            context.getDiagnostic().debug("Endpoint cache HIT for " + requestedUsername + ": " + cached.value());
            return CompletableFuture.completedFuture(cached.value());
        }

        // The source serializes same-user lookups only when a cache is configured, so the first
        // caller populates it and the rest read it back. Each caller still issues its own request
        // under its own cancellation signal; sharing one in-flight request would let one caller's
        // cancellation or failure surface in another's.
        Semaphore semaphore;
        userEndpointSemaphoreSyncRoot.acquireUninterruptibly();
        try {
            semaphore = userEndpointSemaphores.computeIfAbsent(requestedUsername, ignored -> new Semaphore(1));
        } finally {
            userEndpointSemaphoreSyncRoot.release();
        }

        // The permit is released only on the path that acquired it; a cancelled acquisition must
        // not release a permit it never held, which is why the acquire is outside the try.
        try {
            Permits.acquire(semaphore, token);
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }

        CompletableFuture<InetSocketAddress> operation;
        try {
            CacheLookupResult<InetSocketAddress> second = tryCacheGet(cache, requestedUsername);
            if (second.found()) {
                context.getDiagnostic().debug("Endpoint cache HIT for " + requestedUsername + ": " + second.value());
                operation = CompletableFuture.completedFuture(second.value());
            } else {
                operation = retrieveUserEndpoint(requestedUsername, token, cache);
            }
        } catch (Throwable failure) {
            semaphore.release();
            throw failure;
        }
        return operation.whenComplete((result, failure) -> semaphore.release());
    }

    CompletableFuture<InetSocketAddress> retrieveUserEndpoint(
            String requestedUsername, CancellationSignal cancellationSignal, UserEndpointCache cache) {
        CompletableFuture<UserAddressResponse> wait;
        try {
            wait = context.getWaiter()
                    .waitAsync(
                            new dev.slsk.internal.common.WaitKey(
                                    MessageCode.Server.GET_PEER_ADDRESS, requestedUsername),
                            UserAddressResponse.class,
                            null,
                            cancellationSignal);
        } catch (Throwable failure) {
            return mapUserEndpointFailure(CompletableFuture.failedFuture(failure), requestedUsername);
        }
        CompletableFuture<InetSocketAddress> operation = context.writeToServer(
                        new UserAddressRequest(requestedUsername), cancellationSignal)
                .thenCompose(ignored -> wait)
                .thenApply(response -> {
                    if (response.getIpAddress().isAnyLocalAddress()) {
                        throw new UserOfflineException("User " + requestedUsername + " appears to be offline");
                    }
                    InetSocketAddress result = response.getIpEndpoint();
                    if (cache != null) {
                        try {
                            cache.put(requestedUsername, result);
                        } catch (Throwable failure) {
                            throw new UserEndpointCacheException(
                                    "Exception retrieving or updating user "
                                            + "endpoint cache: "
                                            + Failures.message(failure),
                                    failure);
                        }
                        context.getDiagnostic().debug("Endpoint cache MISS for " + requestedUsername + ": " + result);
                    }
                    return result;
                });
        return mapUserEndpointFailure(operation, requestedUsername);
    }

    CompletableFuture<List<Directory>> getDirectoryContents(String requestedUsername, String directoryName) {
        return getDirectoryContents(requestedUsername, directoryName, null, CancellationSignal.none());
    }

    CompletableFuture<List<Directory>> getDirectoryContents(
            String requestedUsername, String directoryName, int operationToken) {
        return getDirectoryContents(requestedUsername, directoryName, operationToken, CancellationSignal.none());
    }

    CompletableFuture<List<Directory>> getDirectoryContents(
            String requestedUsername, String directoryName, CancellationSignal cancellationSignal) {
        return getDirectoryContents(requestedUsername, directoryName, null, cancellationSignal);
    }

    CompletableFuture<List<Directory>> getDirectoryContents(
            String requestedUsername,
            String directoryName,
            Integer operationToken,
            CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        CommonUtils.requireText(directoryName, "directoryName");
        context.requireLoggedIn("fetch directory contents");
        int tokenValue = operationToken == null ? context.getTokenFactory().nextToken() : operationToken;
        CancellationSignal token = context.defaultToken(cancellationSignal);
        CompletableFuture<List<Directory>> contentsWait;
        try {
            @SuppressWarnings("unchecked")
            CompletableFuture<List<Directory>> typedWait =
                    (CompletableFuture<List<Directory>>) (CompletableFuture<?>) context.getWaiter()
                            .waitAsync(
                                    new WaitKey(
                                            MessageCode.Peer.FOLDER_CONTENTS_RESPONSE, requestedUsername, tokenValue),
                                    List.class,
                                    null,
                                    token);
            contentsWait = typedWait;
        } catch (Throwable failure) {
            return Failures.map(
                    CompletableFuture.failedFuture(failure),
                    "Failed to retrieve directory contents for " + directoryName + " from " + requestedUsername + ": ",
                    UserOfflineException.class);
        }
        CompletableFuture<List<Directory>> operation = getUserEndpoint(requestedUsername, token)
                .thenCompose(endpoint -> context.getPeerConnectionManager()
                        .getOrAddMessageConnectionAsync(requestedUsername, endpoint, token))
                .thenCompose(connection ->
                        context.writeToPeer(connection, new FolderContentsRequest(tokenValue, directoryName), token))
                .thenCompose(ignored -> contentsWait)
                .thenApply(response -> Collections.unmodifiableList(new ArrayList<>(response)));
        return Failures.map(
                operation,
                "Failed to retrieve directory contents for " + directoryName + " from " + requestedUsername + ": ",
                UserOfflineException.class);
    }

    static CompletableFuture<InetSocketAddress> mapUserEndpointFailure(
            CompletableFuture<InetSocketAddress> operation, String requestedUsername) {
        return operation.handle((result, failure) -> {
            if (failure == null) {
                return result;
            }
            Throwable cause = Failures.unwrap(failure);
            if (cause instanceof UserOfflineException
                    || cause instanceof UserEndpointCacheException
                    || cause instanceof CancellationException
                    || cause instanceof TimeoutException) {
                throw new CompletionException(cause);
            }
            throw new CompletionException(new UserEndpointException(
                    "Failed to retrieve endpoint for user " + requestedUsername + ": " + Failures.message(cause),
                    cause));
        });
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
     * @return a future completed when the sweep finishes
     */
    CompletableFuture<Void> cleanupUserEndpointSemaphoresAsync() {
        if (!userEndpointSemaphoreSyncRoot.tryAcquire()) {
            return CompletableFuture.completedFuture(null);
        }
        try {
            for (java.util.Map.Entry<String, java.util.concurrent.Semaphore> entry :
                    userEndpointSemaphores.entrySet()) {
                java.util.concurrent.Semaphore semaphore = entry.getValue();
                if (!semaphore.tryAcquire()) {
                    continue;
                }
                if (userEndpointSemaphores.remove(entry.getKey(), semaphore)) {
                    context.getDiagnostic().debug("Cleaned up user endpoint semaphore for " + entry.getKey());
                } else {
                    semaphore.release();
                }
            }
            return CompletableFuture.completedFuture(null);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        } finally {
            userEndpointSemaphoreSyncRoot.release();
        }
    }

    /** Exposes the per-user endpoint semaphores for the client's test accessor. */
    java.util.Map<String, java.util.concurrent.Semaphore> getUserEndpointSemaphores() {
        return userEndpointSemaphores;
    }
}
