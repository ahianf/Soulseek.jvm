// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.internal.common.CancellationSignals;
import dev.slsk.internal.common.Failures;
import dev.slsk.internal.common.Text;
import dev.slsk.internal.common.Wait;
import dev.slsk.internal.common.WaitKey;
import dev.slsk.internal.common.Waiter;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.connection.SoulseekClientState;
import dev.slsk.internal.diagnostics.DiagnosticSink;
import dev.slsk.internal.messaging.MessageCode;
import dev.slsk.internal.messaging.messages.AcknowledgePrivateMessageCommand;
import dev.slsk.internal.messaging.messages.AcknowledgePrivilegeNotificationCommand;
import dev.slsk.internal.messaging.messages.CheckPrivilegesRequest;
import dev.slsk.internal.messaging.messages.NewPassword;
import dev.slsk.internal.messaging.messages.OutgoingMessage;
import dev.slsk.internal.messaging.messages.PrivateMessageCommand;
import dev.slsk.internal.messaging.messages.SendUploadSpeedCommand;
import dev.slsk.internal.messaging.messages.ServerPing;
import dev.slsk.internal.messaging.messages.SetOnlineStatusCommand;
import dev.slsk.internal.messaging.messages.SetSharedCountsCommand;
import dev.slsk.internal.messaging.messages.StartPublicChatCommand;
import dev.slsk.internal.messaging.messages.StopPublicChatCommand;
import dev.slsk.internal.network.MessageConnection;
import dev.slsk.internal.user.WireUserPresence;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * The connection to the server, and everything said over it.
 *
 * <p>Owns the server connection itself, the three ways to write to it — a
 * fire-and-forget command, a command whose acknowledgement is correlated, a
 * request whose typed response is — and the server-facing commands that are
 * about none of the other domains: presence, privileges, shared counts, private
 * messages and the ping.
 *
 * <p><strong>Register, write, await</strong> is the shape of nearly every
 * server operation, and the order is the correlation: the server can answer
 * before the write returns, so a wait registered afterwards would miss its own
 * reply. {@link #command} and {@link #request} exist so that no caller has to
 * remember that, and so no caller has to reimplement the failure translation
 * that follows it.
 *
 * <p>The client's <em>state machine</em> is not here. Connecting, logging in
 * and disconnecting move the client between states and raise events about it,
 * which is the engine's job; this is asked what the state is — through the
 * supplier it is built with — only to refuse work that needs a login.
 *
 * <p>Public, and package-private member by member, because the peer and
 * distributed networks live in another package and talk to the server. What
 * they may say to it is {@link #write} and who we are; the rest of this is the
 * facets' and stays where only they can reach it.
 */
public final class ServerLink {

    private final Waiter waiter;
    private final DiagnosticSink diagnostic;
    private final Supplier<SoulseekClientState> clientState;

    /**
     * The live server connection, or {@code null} before the first connect.
     *
     * <p>Replaced rather than reused: every connect builds a new one, and the
     * old one is closed by the engine's disconnect.
     */
    private volatile MessageConnection connection;

    /**
     * Who the server accepted us as, or {@code null} when not logged in.
     *
     * <p>Ours rather than the engine's: it is what the login handshake
     * established over this connection, it goes away when this connection does,
     * and everything that reads it — a {@code PeerInit} naming us to a peer, a
     * distributed message we must not answer because it is our own search — is
     * asking about the session rather than about the client.
     */
    private volatile String username;

    ServerLink(Waiter waiter, DiagnosticSink diagnostic, Supplier<SoulseekClientState> clientState) {
        this.waiter = java.util.Objects.requireNonNull(waiter, "waiter");
        this.diagnostic = DiagnosticSink.forSource(diagnostic, ServerLink.class);
        this.clientState = java.util.Objects.requireNonNull(clientState, "clientState");
    }

    /**
     * Returns the server connection, or {@code null} if there is none.
     *
     * @return the connection
     */
    MessageConnection connection() {
        return connection;
    }

    /**
     * Adopts a server connection.
     *
     * @param value the connection, or {@code null} to forget the last one
     */
    void connection(MessageConnection value) {
        connection = value;
    }

    /**
     * Returns who we are logged in as, or {@code null} if we are not.
     *
     * @return the logged-in username
     */
    public String username() {
        return username;
    }

    /**
     * Records who the server accepted us as.
     *
     * @param value the username, or {@code null} on disconnect
     */
    void username(String value) {
        username = value;
    }

    /**
     * Returns what state the client is in.
     *
     * @return the client state
     */
    public SoulseekClientState state() {
        return clientState.get();
    }

    /**
     * Throws unless the client is connected and logged in.
     *
     * @param operation what the caller is trying to do, for the message
     */
    public void requireLoggedIn(String operation) {
        SoulseekClientState state = clientState.get();
        if (!state.isLoggedIn()) {
            throw new IllegalStateException("The server connection must be connected and logged in to " + operation
                    + " (currently: " + state + ")");
        }
    }

    /**
     * Writes a message to the server.
     *
     * @param message the message to send
     * @param cancellationSignal the cancellation signal
     */
    public void write(OutgoingMessage message, CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
        connection.write(message, CancellationSignals.orNone(cancellationSignal));
    }

    /**
     * Writes pre-encoded bytes to the server.
     *
     * <p>The login handshake is two messages that have to arrive in one write,
     * which is the only reason a caller ever encodes its own bytes.
     *
     * @param message the encoded message
     * @param cancellationSignal the cancellation signal
     */
    public void writeBytes(byte[] message, CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
        connection.write(message, CancellationSignals.orNone(cancellationSignal));
    }

    /**
     * Sends a command and waits for the server's acknowledgement.
     *
     * @param message the command to send
     * @param waitKey correlates the acknowledgement
     * @param cancellationSignal the cancellation signal
     * @param failurePrefix prefixes any wrapped failure
     */
    void command(OutgoingMessage message, WaitKey waitKey, CancellationSignal cancellationSignal, String failurePrefix)
            throws InterruptedException {
        CancellationSignal signal = CancellationSignals.orNone(cancellationSignal);
        try {
            Wait<Void> wait = waiter.register(waitKey, waiter.getDefaultTimeout(), signal);
            write(message, signal);
            wait.await();
        } catch (Throwable failure) {
            throw Failures.raise(failure, failurePrefix);
        }
    }

    /**
     * Sends a request and waits for a typed response.
     *
     * @param message the request to send
     * @param waitKey correlates the response
     * @param resultType the expected response type
     * @param cancellationSignal the cancellation signal
     * @param failurePrefix prefixes any wrapped failure
     * @param preservedFailures failure types to pass through untranslated
     * @param <T> the response type
     * @return the response
     */
    @SafeVarargs
    final <T> T request(
            OutgoingMessage message,
            WaitKey waitKey,
            Class<T> resultType,
            CancellationSignal cancellationSignal,
            String failurePrefix,
            Class<? extends Throwable>... preservedFailures)
            throws InterruptedException {
        CancellationSignal signal = CancellationSignals.orNone(cancellationSignal);
        try {
            Wait<T> wait = waiter.register(waitKey, resultType, waiter.getDefaultTimeout(), signal);
            write(message, signal);
            return wait.await();
        } catch (Throwable failure) {
            throw Failures.raise(failure, failurePrefix, preservedFailures);
        }
    }

    void acknowledgePrivateMessage(int privateMessageId) throws InterruptedException {
        acknowledgePrivateMessage(privateMessageId, CancellationSignal.none());
    }

    public void acknowledgePrivateMessage(int privateMessageId, CancellationSignal cancellationSignal)
            throws InterruptedException {
        if (privateMessageId < 0) {
            throw new IllegalArgumentException("privateMessageId must not be negative: " + privateMessageId);
        }
        requireLoggedIn("acknowledge private messages");
        send(
                new AcknowledgePrivateMessageCommand(privateMessageId),
                cancellationSignal,
                "Failed to acknowledge private message with ID " + privateMessageId + ": ");
        diagnostic.debug("Acknowledged private message ID " + privateMessageId);
    }

    void acknowledgePrivilegeNotification(int privilegeNotificationId) throws InterruptedException {
        acknowledgePrivilegeNotification(privilegeNotificationId, CancellationSignal.none());
    }

    public void acknowledgePrivilegeNotification(int privilegeNotificationId, CancellationSignal cancellationSignal)
            throws InterruptedException {
        if (privilegeNotificationId < 0) {
            throw new IllegalArgumentException(
                    "privilegeNotificationId must not be negative: " + privilegeNotificationId);
        }
        requireLoggedIn("acknowledge privilege notifications");
        send(
                new AcknowledgePrivilegeNotificationCommand(privilegeNotificationId),
                cancellationSignal,
                "Failed to acknowledge privilege notification with ID " + privilegeNotificationId + ": ");
    }

    void changePassword(String password) throws InterruptedException {
        changePassword(password, CancellationSignal.none());
    }

    void changePassword(String password, CancellationSignal cancellationSignal) throws InterruptedException {
        Text.requireText(password, "password");
        requireLoggedIn("change a password");
        String response = request(
                new NewPassword(password),
                new WaitKey.ServerMessage(MessageCode.Server.NEW_PASSWORD),
                String.class,
                cancellationSignal,
                "Failed to change password: ");
        if (!password.equals(response)) {
            throw new SoulseekClientException("Probably failed to change password; the response "
                    + "from the server doesn't match the specified "
                    + "password");
        }
    }

    Integer getPrivileges() throws InterruptedException {
        return getPrivileges(CancellationSignal.none());
    }

    Integer getPrivileges(CancellationSignal cancellationSignal) throws InterruptedException {
        requireLoggedIn("check privileges");
        return request(
                new CheckPrivilegesRequest(),
                new WaitKey.ServerMessage(MessageCode.Server.CHECK_PRIVILEGES),
                Integer.class,
                cancellationSignal,
                "Failed to get privileges: ");
    }

    long pingServer() throws InterruptedException {
        return pingServer(CancellationSignal.none());
    }

    long pingServer(CancellationSignal cancellationSignal) throws InterruptedException {
        requireLoggedIn("send a ping");
        CancellationSignal token = CancellationSignals.orNone(cancellationSignal);
        try {
            Wait<Void> wait = waiter.register(
                    new WaitKey.ServerMessage(MessageCode.Server.PING), waiter.getDefaultTimeout(), token);
            long started = System.nanoTime();
            write(new ServerPing(), token);
            wait.await();
            return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        } catch (Throwable failure) {
            throw Failures.raise(failure, "Failed to ping the server: ");
        }
    }

    void sendPrivateMessage(String requestedUsername, String message) throws InterruptedException {
        sendPrivateMessage(requestedUsername, message, CancellationSignal.none());
    }

    void sendPrivateMessage(String requestedUsername, String message, CancellationSignal cancellationSignal)
            throws InterruptedException {
        Text.requireText(requestedUsername, "username");
        Text.requireNonEmpty(message, "message");
        requireLoggedIn("send a private message");
        send(
                new PrivateMessageCommand(requestedUsername, message),
                cancellationSignal,
                "Failed to send private message to user " + requestedUsername + ": ");
    }

    void sendUploadSpeed(int speed) throws InterruptedException {
        sendUploadSpeed(speed, CancellationSignal.none());
    }

    void sendUploadSpeed(int speed, CancellationSignal cancellationSignal) throws InterruptedException {
        requireLoggedIn("set upload speed");
        if (speed <= 0) {
            throw new IllegalArgumentException("speed must be greater than zero: " + speed);
        }
        send(new SendUploadSpeedCommand(speed), cancellationSignal, "Failed to set upload speed: ");
    }

    void setSharedCounts(int directories, int files) throws InterruptedException {
        setSharedCounts(directories, files, CancellationSignal.none());
    }

    void setSharedCounts(int directories, int files, CancellationSignal cancellationSignal)
            throws InterruptedException {
        if (directories < 0) {
            throw new IllegalArgumentException("directories must not be negative: " + directories);
        }
        if (files < 0) {
            throw new IllegalArgumentException("files must not be negative: " + files);
        }
        requireLoggedIn("set shared counts");
        send(
                new SetSharedCountsCommand(directories, files),
                cancellationSignal,
                "Failed to set shared counts to " + directories + " directories and " + files + " files: ");
    }

    void setStatus(WireUserPresence status) throws InterruptedException {
        setStatus(status, CancellationSignal.none());
    }

    void setStatus(WireUserPresence status, CancellationSignal cancellationSignal) throws InterruptedException {
        requireLoggedIn("set online status");
        send(new SetOnlineStatusCommand(status), cancellationSignal, "Failed to set user status to " + status + ": ");
    }

    void startPublicChat() throws InterruptedException {
        startPublicChat(CancellationSignal.none());
    }

    void startPublicChat(CancellationSignal cancellationSignal) throws InterruptedException {
        requireLoggedIn("start public chat");
        send(new StartPublicChatCommand(), cancellationSignal, "Failed to start public chat: ");
    }

    void stopPublicChat() throws InterruptedException {
        stopPublicChat(CancellationSignal.none());
    }

    void stopPublicChat(CancellationSignal cancellationSignal) throws InterruptedException {
        requireLoggedIn("stop public chat");
        send(new StopPublicChatCommand(), cancellationSignal, "Failed to stop public chat: ");
    }

    /** Writes a fire-and-forget command, saying what failed if it does. */
    private void send(OutgoingMessage message, CancellationSignal cancellationSignal, String failurePrefix)
            throws InterruptedException {
        try {
            write(message, CancellationSignals.orNone(cancellationSignal));
        } catch (Throwable failure) {
            throw Failures.raise(failure, failurePrefix);
        }
    }
}
