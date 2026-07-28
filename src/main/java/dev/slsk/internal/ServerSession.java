// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.CancellationSignal;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.internal.common.CommonUtils;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.messages.AcknowledgePrivateMessageCommand;
import dev.slsk.internal.messaging.messages.AcknowledgePrivilegeNotificationCommand;
import dev.slsk.internal.messaging.messages.CheckPrivilegesRequest;
import dev.slsk.internal.messaging.messages.NewPassword;
import dev.slsk.internal.messaging.messages.PrivateMessageCommand;
import dev.slsk.internal.messaging.messages.SendUploadSpeedCommand;
import dev.slsk.internal.messaging.messages.ServerPing;
import dev.slsk.internal.messaging.messages.SetOnlineStatusCommand;
import dev.slsk.internal.messaging.messages.SetSharedCountsCommand;
import dev.slsk.internal.messaging.messages.StartPublicChatCommand;
import dev.slsk.internal.messaging.messages.StopPublicChatCommand;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Server-facing commands that are not about rooms, users, searches or
 * transfers: presence, privileges, shared counts, private messages and the
 * server ping.
 *
 * <p>Connection and login lifecycle stay on the client — they own its state
 * machine — so this is the stateless remainder of the server protocol.
 */
final class ServerSession {

    private final EngineContext context;

    ServerSession(EngineContext context) {
        this.context = java.util.Objects.requireNonNull(context, "context");
    }

    CompletableFuture<Void> acknowledgePrivateMessage(int privateMessageId) {
        return acknowledgePrivateMessage(privateMessageId, CancellationSignal.none());
    }

    CompletableFuture<Void> acknowledgePrivateMessage(int privateMessageId, CancellationSignal cancellationSignal) {
        if (privateMessageId < 0) {
            throw new IllegalArgumentException("The private message ID must be greater than zero");
        }
        context.requireLoggedIn("acknowledge private messages");
        CompletableFuture<Void> write = Failures.map(
                context.writeToServer(
                        new AcknowledgePrivateMessageCommand(privateMessageId),
                        context.defaultToken(cancellationSignal)),
                "Failed to acknowledge private message with ID " + privateMessageId + ": ");
        return write.thenRun(
                () -> context.getDiagnostic().debug("Acknowledged private message ID " + privateMessageId));
    }

    CompletableFuture<Void> acknowledgePrivilegeNotification(int privilegeNotificationId) {
        return acknowledgePrivilegeNotification(privilegeNotificationId, CancellationSignal.none());
    }

    CompletableFuture<Void> acknowledgePrivilegeNotification(
            int privilegeNotificationId, CancellationSignal cancellationSignal) {
        if (privilegeNotificationId < 0) {
            throw new IllegalArgumentException("The privilege notification ID must be greater than zero");
        }
        context.requireLoggedIn("acknowledge privilege notifications");
        return Failures.map(
                context.writeToServer(
                        new AcknowledgePrivilegeNotificationCommand(privilegeNotificationId),
                        context.defaultToken(cancellationSignal)),
                "Failed to acknowledge privilege notification with ID " + privilegeNotificationId + ": ");
    }

    CompletableFuture<Void> changePassword(String password) {
        return changePassword(password, CancellationSignal.none());
    }

    CompletableFuture<Void> changePassword(String password, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(password, "password");
        context.requireLoggedIn("change a password");
        return context.executeCorrelatedRequest(
                        new NewPassword(password),
                        new WaitKey(MessageCode.Server.NEW_PASSWORD),
                        String.class,
                        cancellationSignal,
                        "Failed to change password: ")
                .thenApply(response -> {
                    if (!password.equals(response)) {
                        throw new SoulseekClientException("Probably failed to change password; the response "
                                + "from the server doesn't match the specified "
                                + "password");
                    }
                    return null;
                });
    }

    CompletableFuture<Integer> getPrivileges() {
        return getPrivileges(CancellationSignal.none());
    }

    CompletableFuture<Integer> getPrivileges(CancellationSignal cancellationSignal) {
        context.requireLoggedIn("check privileges");
        return context.executeCorrelatedRequest(
                new CheckPrivilegesRequest(),
                new WaitKey(MessageCode.Server.CHECK_PRIVILEGES),
                Integer.class,
                cancellationSignal,
                "Failed to get privileges: ");
    }

    CompletableFuture<Long> pingServer() {
        return pingServer(CancellationSignal.none());
    }

    CompletableFuture<Long> pingServer(CancellationSignal cancellationSignal) {
        context.requireLoggedIn("send a ping");
        CancellationSignal token = context.defaultToken(cancellationSignal);
        CompletableFuture<Void> wait;
        try {
            wait = context.getWaiter().waitAsync(new WaitKey(MessageCode.Server.PING), null, token);
        } catch (Throwable failure) {
            return Failures.map(CompletableFuture.failedFuture(failure), "Failed to ping the server: ");
        }
        long started = System.nanoTime();
        CompletableFuture<Void> responseWait = wait;
        CompletableFuture<Long> operation = context.writeToServer(new ServerPing(), token)
                .thenCompose(ignored -> responseWait)
                .thenApply(ignored -> TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
        return Failures.map(operation, "Failed to ping the server: ");
    }

    CompletableFuture<Void> sendPrivateMessage(String requestedUsername, String message) {
        return sendPrivateMessage(requestedUsername, message, CancellationSignal.none());
    }

    CompletableFuture<Void> sendPrivateMessage(
            String requestedUsername, String message, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        CommonUtils.requireNonEmpty(message, "message");
        context.requireLoggedIn("send a private message");
        return Failures.map(
                context.writeToServer(
                        new PrivateMessageCommand(requestedUsername, message),
                        context.defaultToken(cancellationSignal)),
                "Failed to send private message to user " + requestedUsername + ": ");
    }

    CompletableFuture<Void> sendUploadSpeed(int speed) {
        return sendUploadSpeed(speed, CancellationSignal.none());
    }

    CompletableFuture<Void> sendUploadSpeed(int speed, CancellationSignal cancellationSignal) {
        context.requireLoggedIn("set upload speed");
        if (speed <= 0) {
            throw new IllegalArgumentException("The upload speed must be greater than zero");
        }
        return Failures.map(
                context.writeToServer(new SendUploadSpeedCommand(speed), context.defaultToken(cancellationSignal)),
                "Failed to set upload speed: ");
    }

    CompletableFuture<Void> setSharedCounts(int directories, int files) {
        return setSharedCounts(directories, files, CancellationSignal.none());
    }

    CompletableFuture<Void> setSharedCounts(int directories, int files, CancellationSignal cancellationSignal) {
        if (directories < 0) {
            throw new IllegalArgumentException("The directory count must be equal to or greater than zero");
        }
        if (files < 0) {
            throw new IllegalArgumentException("The file count must be equal to or greater than zero");
        }
        context.requireLoggedIn("set shared counts");
        return Failures.map(
                context.writeToServer(
                        new SetSharedCountsCommand(directories, files), context.defaultToken(cancellationSignal)),
                "Failed to set shared counts to " + directories + " directories and " + files + " files: ");
    }

    CompletableFuture<Void> setStatus(UserPresence status) {
        return setStatus(status, CancellationSignal.none());
    }

    CompletableFuture<Void> setStatus(UserPresence status, CancellationSignal cancellationSignal) {
        context.requireLoggedIn("set online status");
        return Failures.map(
                context.writeToServer(new SetOnlineStatusCommand(status), context.defaultToken(cancellationSignal)),
                "Failed to set user status to " + status + ": ");
    }

    CompletableFuture<Void> startPublicChat() {
        return startPublicChat(CancellationSignal.none());
    }

    CompletableFuture<Void> startPublicChat(CancellationSignal cancellationSignal) {
        context.requireLoggedIn("start public chat");
        return Failures.map(
                context.writeToServer(new StartPublicChatCommand(), context.defaultToken(cancellationSignal)),
                "Failed to start public chat: ");
    }

    CompletableFuture<Void> stopPublicChat() {
        return stopPublicChat(CancellationSignal.none());
    }

    CompletableFuture<Void> stopPublicChat(CancellationSignal cancellationSignal) {
        context.requireLoggedIn("stop public chat");
        return Failures.map(
                context.writeToServer(new StopPublicChatCommand(), context.defaultToken(cancellationSignal)),
                "Failed to stop public chat: ");
    }
}
