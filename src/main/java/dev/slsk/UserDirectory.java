// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import static dev.slsk.ClientSupport.mapClientFailure;
import static dev.slsk.ClientSupport.requireText;
import static dev.slsk.ClientSupport.unwrap;

import dev.slsk.common.Constants;
import dev.slsk.common.WaitKey;
import dev.slsk.exceptions.ConnectionException;
import dev.slsk.exceptions.UserNotFoundException;
import dev.slsk.exceptions.UserOfflineException;
import dev.slsk.messaging.MessageCode;
import dev.slsk.messaging.handlers.BrowseResponseConnection;
import dev.slsk.messaging.messages.BrowseRequest;
import dev.slsk.messaging.messages.GivePrivilegesCommand;
import dev.slsk.messaging.messages.UnwatchUserCommand;
import dev.slsk.messaging.messages.UserInfoRequest;
import dev.slsk.messaging.messages.UserPrivilegesRequest;
import dev.slsk.messaging.messages.UserStatisticsRequest;
import dev.slsk.messaging.messages.UserStatusRequest;
import dev.slsk.messaging.messages.WatchUserRequest;
import dev.slsk.messaging.messages.WatchUserResponse;
import dev.slsk.network.MessageConnection;
import dev.slsk.options.BrowseOptions;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Everything the client knows about other users: info, statistics, presence,
 * privileges, endpoint resolution and browsing their shares.
 *
 * <p>The second piece lifted out of {@code DefaultSoulseekClient}. These
 * operations are peer-facing rather than server-facing, but they share the same
 * correlate-and-translate shape, so they reach the rest of the client through
 * {@link ClientContext} like the rooms do.
 */
final class UserDirectory {

    private final ClientContext context;

    UserDirectory(ClientContext context) {
        this.context = Objects.requireNonNull(context, "context");
    }

    CompletableFuture<UserInfo> getUserInfo(String requestedUsername) {
        return getUserInfo(requestedUsername, CancellationSignal.none());
    }

    CompletableFuture<UserInfo> getUserInfo(String requestedUsername, CancellationSignal cancellationSignal) {
        requireText(requestedUsername, "username");
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
            return mapClientFailure(
                    CompletableFuture.failedFuture(failure),
                    "Failed to retrieve information for user " + requestedUsername + ": ",
                    UserOfflineException.class);
        }
        CompletableFuture<UserInfo> operation = context.resolveUserEndpoint(requestedUsername, token)
                .thenCompose(endpoint -> context.getPeerConnectionManager()
                        .getOrAddMessageConnectionAsync(requestedUsername, endpoint, token))
                .thenCompose(connection -> context.writeToPeer(connection, new UserInfoRequest(), token))
                .thenCompose(ignored -> infoWait);
        return mapClientFailure(
                operation,
                "Failed to retrieve information for user " + requestedUsername + ": ",
                UserOfflineException.class);
    }

    CompletableFuture<Boolean> getUserPrivileged(String requestedUsername) {
        return getUserPrivileged(requestedUsername, CancellationSignal.none());
    }

    CompletableFuture<Boolean> getUserPrivileged(String requestedUsername, CancellationSignal cancellationSignal) {
        requireText(requestedUsername, "username");
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
        requireText(requestedUsername, "username");
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
        requireText(requestedUsername, "username");
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
        requireText(requestedUsername, "username");
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
        requireText(requestedUsername, "username");
        context.requireLoggedIn("add users");
        return mapClientFailure(
                context.writeToServer(
                        new UnwatchUserCommand(requestedUsername), context.defaultToken(cancellationSignal)),
                "Failed to unwatch user " + requestedUsername + ": ");
    }

    CompletableFuture<Void> grantUserPrivileges(String requestedUsername, int days) {
        return grantUserPrivileges(requestedUsername, days, CancellationSignal.none());
    }

    CompletableFuture<Void> grantUserPrivileges(
            String requestedUsername, int days, CancellationSignal cancellationSignal) {
        requireText(requestedUsername, "username");
        if (days <= 0) {
            throw new IllegalArgumentException("The number of days granted must be greater than zero");
        }
        context.requireLoggedIn("grant user privileges");
        return mapClientFailure(
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
        requireText(requestedUsername, "username");
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
            return mapClientFailure(
                    CompletableFuture.failedFuture(failure),
                    "Failed to browse user " + requestedUsername + ": ",
                    UserOfflineException.class);
        }

        CompletableFuture<BrowseResponseConnection> setup = context.resolveUserEndpoint(requestedUsername, token)
                .thenCompose(endpoint -> context.getPeerConnectionManager()
                        .getOrAddMessageConnectionAsync(requestedUsername, endpoint, token))
                .thenCompose(connection -> context.writeToPeer(connection, new BrowseRequest(), token))
                .thenCompose(ignored -> connectionWait);
        CompletableFuture<BrowseResponse> operation = setup.handle((responseConnection, failure) -> {
                    if (failure == null) {
                        return responseConnection;
                    }
                    Throwable cause = unwrap(failure);
                    context.getWaiter().fail(browseWaitKey, cause);
                    throw new CompletionException(cause);
                })
                .thenCompose(responseConnection -> {
                    MessageConnection connection = responseConnection.connection();
                    long responseLength = responseConnection.eventData().getLength() - 4;
                    AtomicBoolean completionEventFired = new AtomicBoolean();
                    dev.slsk.network.MessageConnectionEventListener<dev.slsk.network.MessageDataEvent>
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
        return mapClientFailure(
                operation, "Failed to browse user " + requestedUsername + ": ", UserOfflineException.class);
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
        requireText(requestedUsername, "username");
        context.requireLoggedIn("connect to other users");
        CancellationSignal token = context.defaultToken(cancellationSignal);
        CompletableFuture<Void> operation = context.resolveUserEndpoint(requestedUsername, token)
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
        return mapClientFailure(
                operation, "Failed to connect to user " + requestedUsername + ": ", UserOfflineException.class);
    }
}
