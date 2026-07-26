// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.options.BrowseOptions;
import java.util.concurrent.CompletableFuture;

/**
 * The blocking public surface: every {@link SoulseekClient} operation, each one
 * line of delegation to the component that does the work.
 *
 * <p>Eighty-two methods of pure forwarding. Like {@link ClientEventSupport} it
 * is a base class rather than a collaborator, because these methods <em>are</em>
 * the API and must be members of the client's type. Separating them keeps the
 * client itself down to what is genuinely its own: the connection state
 * machine, the component wiring, and the collaborator callbacks.
 *
 * <p>Failure translation lives in {@code unwrapped}; see D11 in
 * {@code docs/fork-divergence.md} for why a lapsed deadline becomes
 * {@code NoResponseException}.
 */
abstract class ClientOperations extends ClientEventSupport implements SoulseekClient {

    /** Chat rooms. */
    abstract RoomRegistry rooms();

    /** User info, presence and browsing. */
    abstract UserDirectory users();

    /** Stateless server commands. */
    abstract ServerSession server();

    /** Search lifecycle. */
    abstract SearchCoordinator searchCoordinator();

    /** Transfer orchestration. */
    abstract TransferEngine transfers();

    /** Waits for an internal operation and translates its failure. */
    abstract <T> T unwrapped(CompletableFuture<T> operation);

    @Override
    public void acknowledgePrivateMessage(int privateMessageId) {
        unwrapped(server().acknowledgePrivateMessage(privateMessageId));
    }

    @Override
    public void acknowledgePrivateMessage(int privateMessageId, CancellationSignal cancellationSignal) {
        unwrapped(server().acknowledgePrivateMessage(privateMessageId, cancellationSignal));
    }

    @Override
    public void acknowledgePrivilegeNotification(int privilegeNotificationId) {
        unwrapped(server().acknowledgePrivilegeNotification(privilegeNotificationId));
    }

    @Override
    public void acknowledgePrivilegeNotification(int privilegeNotificationId, CancellationSignal cancellationSignal) {
        unwrapped(server().acknowledgePrivilegeNotification(privilegeNotificationId, cancellationSignal));
    }

    @Override
    public void addPrivateRoomMember(String roomName, String username) {
        unwrapped(rooms().addPrivateRoomMember(roomName, username));
    }

    @Override
    public void addPrivateRoomMember(String roomName, String username, CancellationSignal cancellationSignal) {
        unwrapped(rooms().addPrivateRoomMember(roomName, username, cancellationSignal));
    }

    @Override
    public void addPrivateRoomModerator(String roomName, String username) {
        unwrapped(rooms().addPrivateRoomModerator(roomName, username));
    }

    @Override
    public void addPrivateRoomModerator(String roomName, String username, CancellationSignal cancellationSignal) {
        unwrapped(rooms().addPrivateRoomModerator(roomName, username, cancellationSignal));
    }

    @Override
    public BrowseResponse browse(String username) {
        return unwrapped(users().browse(username));
    }

    @Override
    public BrowseResponse browse(String username, BrowseOptions options) {
        return unwrapped(users().browse(username, options));
    }

    @Override
    public BrowseResponse browse(String username, CancellationSignal cancellationSignal) {
        return unwrapped(users().browse(username, cancellationSignal));
    }

    @Override
    public BrowseResponse browse(String username, BrowseOptions options, CancellationSignal cancellationSignal) {
        return unwrapped(users().browse(username, options, cancellationSignal));
    }

    @Override
    public void changePassword(String password) {
        unwrapped(server().changePassword(password));
    }

    @Override
    public void changePassword(String password, CancellationSignal cancellationSignal) {
        unwrapped(server().changePassword(password, cancellationSignal));
    }

    @Override
    public void connectToUser(String username) {
        unwrapped(users().connectToUser(username));
    }

    @Override
    public void connectToUser(String username, boolean invalidateCache) {
        unwrapped(users().connectToUser(username, invalidateCache));
    }

    @Override
    public void connectToUser(String username, CancellationSignal cancellationSignal) {
        unwrapped(users().connectToUser(username, cancellationSignal));
    }

    @Override
    public void connectToUser(String username, boolean invalidateCache, CancellationSignal cancellationSignal) {
        unwrapped(users().connectToUser(username, invalidateCache, cancellationSignal));
    }

    @Override
    public void dropPrivateRoomMembership(String roomName) {
        unwrapped(rooms().dropPrivateRoomMembership(roomName));
    }

    @Override
    public void dropPrivateRoomMembership(String roomName, CancellationSignal cancellationSignal) {
        unwrapped(rooms().dropPrivateRoomMembership(roomName, cancellationSignal));
    }

    @Override
    public void dropPrivateRoomOwnership(String roomName) {
        unwrapped(rooms().dropPrivateRoomOwnership(roomName));
    }

    @Override
    public void dropPrivateRoomOwnership(String roomName, CancellationSignal cancellationSignal) {
        unwrapped(rooms().dropPrivateRoomOwnership(roomName, cancellationSignal));
    }

    @Override
    public Integer getDownloadPlaceInQueue(String username, String filename) {
        return unwrapped(transfers().getDownloadPlaceInQueue(username, filename));
    }

    @Override
    public Integer getDownloadPlaceInQueue(String username, String filename, CancellationSignal cancellationSignal) {
        return unwrapped(transfers().getDownloadPlaceInQueue(username, filename, cancellationSignal));
    }

    @Override
    public Integer getPrivileges() {
        return unwrapped(server().getPrivileges());
    }

    @Override
    public Integer getPrivileges(CancellationSignal cancellationSignal) {
        return unwrapped(server().getPrivileges(cancellationSignal));
    }

    @Override
    public RoomList getRoomList() {
        return unwrapped(rooms().getRoomList());
    }

    @Override
    public RoomList getRoomList(CancellationSignal cancellationSignal) {
        return unwrapped(rooms().getRoomList(cancellationSignal));
    }

    @Override
    public UserInfo getUserInfo(String username) {
        return unwrapped(users().getUserInfo(username));
    }

    @Override
    public UserInfo getUserInfo(String username, CancellationSignal cancellationSignal) {
        return unwrapped(users().getUserInfo(username, cancellationSignal));
    }

    @Override
    public Boolean getUserPrivileged(String username) {
        return unwrapped(users().getUserPrivileged(username));
    }

    @Override
    public Boolean getUserPrivileged(String username, CancellationSignal cancellationSignal) {
        return unwrapped(users().getUserPrivileged(username, cancellationSignal));
    }

    @Override
    public UserStatistics getUserStatistics(String username) {
        return unwrapped(users().getUserStatistics(username));
    }

    @Override
    public UserStatistics getUserStatistics(String username, CancellationSignal cancellationSignal) {
        return unwrapped(users().getUserStatistics(username, cancellationSignal));
    }

    @Override
    public UserStatus getUserStatus(String username) {
        return unwrapped(users().getUserStatus(username));
    }

    @Override
    public UserStatus getUserStatus(String username, CancellationSignal cancellationSignal) {
        return unwrapped(users().getUserStatus(username, cancellationSignal));
    }

    @Override
    public void grantUserPrivileges(String username, int days) {
        unwrapped(users().grantUserPrivileges(username, days));
    }

    @Override
    public void grantUserPrivileges(String username, int days, CancellationSignal cancellationSignal) {
        unwrapped(users().grantUserPrivileges(username, days, cancellationSignal));
    }

    @Override
    public RoomData joinRoom(String roomName) {
        return unwrapped(rooms().joinRoom(roomName));
    }

    @Override
    public RoomData joinRoom(String roomName, boolean isPrivate) {
        return unwrapped(rooms().joinRoom(roomName, isPrivate));
    }

    @Override
    public RoomData joinRoom(String roomName, CancellationSignal cancellationSignal) {
        return unwrapped(rooms().joinRoom(roomName, cancellationSignal));
    }

    @Override
    public RoomData joinRoom(String roomName, boolean isPrivate, CancellationSignal cancellationSignal) {
        return unwrapped(rooms().joinRoom(roomName, isPrivate, cancellationSignal));
    }

    @Override
    public void leaveRoom(String roomName) {
        unwrapped(rooms().leaveRoom(roomName));
    }

    @Override
    public void leaveRoom(String roomName, CancellationSignal cancellationSignal) {
        unwrapped(rooms().leaveRoom(roomName, cancellationSignal));
    }

    @Override
    public Long pingServer() {
        return unwrapped(server().pingServer());
    }

    @Override
    public Long pingServer(CancellationSignal cancellationSignal) {
        return unwrapped(server().pingServer(cancellationSignal));
    }

    @Override
    public void removePrivateRoomMember(String roomName, String username) {
        unwrapped(rooms().removePrivateRoomMember(roomName, username));
    }

    @Override
    public void removePrivateRoomMember(String roomName, String username, CancellationSignal cancellationSignal) {
        unwrapped(rooms().removePrivateRoomMember(roomName, username, cancellationSignal));
    }

    @Override
    public void removePrivateRoomModerator(String roomName, String username) {
        unwrapped(rooms().removePrivateRoomModerator(roomName, username));
    }

    @Override
    public void removePrivateRoomModerator(String roomName, String username, CancellationSignal cancellationSignal) {
        unwrapped(rooms().removePrivateRoomModerator(roomName, username, cancellationSignal));
    }

    @Override
    public void sendPrivateMessage(String username, String message) {
        unwrapped(server().sendPrivateMessage(username, message));
    }

    @Override
    public void sendPrivateMessage(String username, String message, CancellationSignal cancellationSignal) {
        unwrapped(server().sendPrivateMessage(username, message, cancellationSignal));
    }

    @Override
    public void sendRoomMessage(String roomName, String message) {
        unwrapped(rooms().sendRoomMessage(roomName, message));
    }

    @Override
    public void sendRoomMessage(String roomName, String message, CancellationSignal cancellationSignal) {
        unwrapped(rooms().sendRoomMessage(roomName, message, cancellationSignal));
    }

    @Override
    public void sendUploadSpeed(int speed) {
        unwrapped(server().sendUploadSpeed(speed));
    }

    @Override
    public void sendUploadSpeed(int speed, CancellationSignal cancellationSignal) {
        unwrapped(server().sendUploadSpeed(speed, cancellationSignal));
    }

    @Override
    public void setRoomTicker(String roomName, String message) {
        unwrapped(rooms().setRoomTicker(roomName, message));
    }

    @Override
    public void setRoomTicker(String roomName, String message, CancellationSignal cancellationSignal) {
        unwrapped(rooms().setRoomTicker(roomName, message, cancellationSignal));
    }

    @Override
    public void setSharedCounts(int directories, int files) {
        unwrapped(server().setSharedCounts(directories, files));
    }

    @Override
    public void setSharedCounts(int directories, int files, CancellationSignal cancellationSignal) {
        unwrapped(server().setSharedCounts(directories, files, cancellationSignal));
    }

    @Override
    public void setStatus(UserPresence status) {
        unwrapped(server().setStatus(status));
    }

    @Override
    public void setStatus(UserPresence status, CancellationSignal cancellationSignal) {
        unwrapped(server().setStatus(status, cancellationSignal));
    }

    @Override
    public void startPublicChat() {
        unwrapped(server().startPublicChat());
    }

    @Override
    public void startPublicChat(CancellationSignal cancellationSignal) {
        unwrapped(server().startPublicChat(cancellationSignal));
    }

    @Override
    public void stopPublicChat() {
        unwrapped(server().stopPublicChat());
    }

    @Override
    public void stopPublicChat(CancellationSignal cancellationSignal) {
        unwrapped(server().stopPublicChat(cancellationSignal));
    }

    @Override
    public void unwatchUser(String username) {
        unwrapped(users().unwatchUser(username));
    }

    @Override
    public void unwatchUser(String username, CancellationSignal cancellationSignal) {
        unwrapped(users().unwatchUser(username, cancellationSignal));
    }

    @Override
    public UserData watchUser(String username) {
        return unwrapped(users().watchUser(username));
    }

    @Override
    public UserData watchUser(String username, CancellationSignal cancellationSignal) {
        return unwrapped(users().watchUser(username, cancellationSignal));
    }
}
