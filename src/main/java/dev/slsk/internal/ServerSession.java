// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.CancellationSignal;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.internal.common.CommonUtils;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.Wait;
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

    void acknowledgePrivateMessage(int privateMessageId) {
        acknowledgePrivateMessage(privateMessageId, CancellationSignal.none());
    }

    void acknowledgePrivateMessage(int privateMessageId, CancellationSignal cancellationSignal) {
        if (privateMessageId < 0) {
            throw new IllegalArgumentException("The private message ID must be greater than zero");
        }
        context.requireLoggedIn("acknowledge private messages");
        write(
                new AcknowledgePrivateMessageCommand(privateMessageId),
                cancellationSignal,
                "Failed to acknowledge private message with ID " + privateMessageId + ": ");
        context.getDiagnostic().debug("Acknowledged private message ID " + privateMessageId);
    }

    void acknowledgePrivilegeNotification(int privilegeNotificationId) {
        acknowledgePrivilegeNotification(privilegeNotificationId, CancellationSignal.none());
    }

    void acknowledgePrivilegeNotification(int privilegeNotificationId, CancellationSignal cancellationSignal) {
        if (privilegeNotificationId < 0) {
            throw new IllegalArgumentException("The privilege notification ID must be greater than zero");
        }
        context.requireLoggedIn("acknowledge privilege notifications");
        write(
                new AcknowledgePrivilegeNotificationCommand(privilegeNotificationId),
                cancellationSignal,
                "Failed to acknowledge privilege notification with ID " + privilegeNotificationId + ": ");
    }

    void changePassword(String password) {
        changePassword(password, CancellationSignal.none());
    }

    void changePassword(String password, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(password, "password");
        context.requireLoggedIn("change a password");
        String response = context.executeCorrelatedRequest(
                new NewPassword(password),
                new WaitKey(MessageCode.Server.NEW_PASSWORD),
                String.class,
                cancellationSignal,
                "Failed to change password: ");
        if (!password.equals(response)) {
            throw new SoulseekClientException("Probably failed to change password; the response "
                    + "from the server doesn't match the specified "
                    + "password");
        }
    }

    Integer getPrivileges() {
        return getPrivileges(CancellationSignal.none());
    }

    Integer getPrivileges(CancellationSignal cancellationSignal) {
        context.requireLoggedIn("check privileges");
        return context.executeCorrelatedRequest(
                new CheckPrivilegesRequest(),
                new WaitKey(MessageCode.Server.CHECK_PRIVILEGES),
                Integer.class,
                cancellationSignal,
                "Failed to get privileges: ");
    }

    long pingServer() {
        return pingServer(CancellationSignal.none());
    }

    long pingServer(CancellationSignal cancellationSignal) {
        context.requireLoggedIn("send a ping");
        CancellationSignal token = context.defaultToken(cancellationSignal);
        try {
            Wait<Void> wait = context.getWaiter().register(new WaitKey(MessageCode.Server.PING), null, token);
            long started = System.nanoTime();
            context.writeToServer(new ServerPing(), token);
            wait.await();
            return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        } catch (Throwable failure) {
            throw Failures.raise(failure, "Failed to ping the server: ");
        }
    }

    void sendPrivateMessage(String requestedUsername, String message) {
        sendPrivateMessage(requestedUsername, message, CancellationSignal.none());
    }

    void sendPrivateMessage(String requestedUsername, String message, CancellationSignal cancellationSignal) {
        CommonUtils.requireText(requestedUsername, "username");
        CommonUtils.requireNonEmpty(message, "message");
        context.requireLoggedIn("send a private message");
        write(
                new PrivateMessageCommand(requestedUsername, message),
                cancellationSignal,
                "Failed to send private message to user " + requestedUsername + ": ");
    }

    void sendUploadSpeed(int speed) {
        sendUploadSpeed(speed, CancellationSignal.none());
    }

    void sendUploadSpeed(int speed, CancellationSignal cancellationSignal) {
        context.requireLoggedIn("set upload speed");
        if (speed <= 0) {
            throw new IllegalArgumentException("The upload speed must be greater than zero");
        }
        write(new SendUploadSpeedCommand(speed), cancellationSignal, "Failed to set upload speed: ");
    }

    void setSharedCounts(int directories, int files) {
        setSharedCounts(directories, files, CancellationSignal.none());
    }

    void setSharedCounts(int directories, int files, CancellationSignal cancellationSignal) {
        if (directories < 0) {
            throw new IllegalArgumentException("The directory count must be equal to or greater than zero");
        }
        if (files < 0) {
            throw new IllegalArgumentException("The file count must be equal to or greater than zero");
        }
        context.requireLoggedIn("set shared counts");
        write(
                new SetSharedCountsCommand(directories, files),
                cancellationSignal,
                "Failed to set shared counts to " + directories + " directories and " + files + " files: ");
    }

    void setStatus(UserPresence status) {
        setStatus(status, CancellationSignal.none());
    }

    void setStatus(UserPresence status, CancellationSignal cancellationSignal) {
        context.requireLoggedIn("set online status");
        write(new SetOnlineStatusCommand(status), cancellationSignal, "Failed to set user status to " + status + ": ");
    }

    void startPublicChat() {
        startPublicChat(CancellationSignal.none());
    }

    void startPublicChat(CancellationSignal cancellationSignal) {
        context.requireLoggedIn("start public chat");
        write(new StartPublicChatCommand(), cancellationSignal, "Failed to start public chat: ");
    }

    void stopPublicChat() {
        stopPublicChat(CancellationSignal.none());
    }

    void stopPublicChat(CancellationSignal cancellationSignal) {
        context.requireLoggedIn("stop public chat");
        write(new StopPublicChatCommand(), cancellationSignal, "Failed to stop public chat: ");
    }

    /** Writes a fire-and-forget command, saying what failed if it does. */
    private void write(
            dev.slsk.internal.messaging.messages.OutgoingMessage message,
            CancellationSignal cancellationSignal,
            String failurePrefix) {
        try {
            context.writeToServer(message, context.defaultToken(cancellationSignal));
        } catch (Throwable failure) {
            throw Failures.raise(failure, failurePrefix);
        }
    }
}
