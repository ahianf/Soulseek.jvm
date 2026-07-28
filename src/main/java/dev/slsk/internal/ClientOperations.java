// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import dev.slsk.CancellationSignal;
import dev.slsk.internal.options.BrowseOptions;
import java.util.concurrent.CompletableFuture;

/**
 * The blocking operations, each one line of delegation to the component that
 * does the work.
 *
 * <p>Eighty-two methods of pure forwarding, and now nobody's public API: the
 * interface they implemented is gone. They survive only as the wrapper bodies
 * the facets are about to take, one facet at a time, and this class disappears
 * when the last of them has moved.
 *
 * <p>Failure translation lives in {@code unwrapped}: a lapsed deadline becomes
 * {@code NoResponseException} rather than a checked
 * {@code java.util.concurrent.TimeoutException}.
 */
abstract class ClientOperations extends ClientEventSupport {

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

    public void acknowledgePrivateMessage(int privateMessageId) {
        unwrapped(server().acknowledgePrivateMessage(privateMessageId));
    }

    public void acknowledgePrivateMessage(int privateMessageId, CancellationSignal cancellationSignal) {
        unwrapped(server().acknowledgePrivateMessage(privateMessageId, cancellationSignal));
    }

    public void acknowledgePrivilegeNotification(int privilegeNotificationId) {
        unwrapped(server().acknowledgePrivilegeNotification(privilegeNotificationId));
    }

    public void acknowledgePrivilegeNotification(int privilegeNotificationId, CancellationSignal cancellationSignal) {
        unwrapped(server().acknowledgePrivilegeNotification(privilegeNotificationId, cancellationSignal));
    }

    public void addPrivateRoomMember(String roomName, String username) {
        unwrapped(rooms().addPrivateRoomMember(roomName, username));
    }

    public void addPrivateRoomMember(String roomName, String username, CancellationSignal cancellationSignal) {
        unwrapped(rooms().addPrivateRoomMember(roomName, username, cancellationSignal));
    }

    public void addPrivateRoomModerator(String roomName, String username) {
        unwrapped(rooms().addPrivateRoomModerator(roomName, username));
    }

    public void addPrivateRoomModerator(String roomName, String username, CancellationSignal cancellationSignal) {
        unwrapped(rooms().addPrivateRoomModerator(roomName, username, cancellationSignal));
    }

    public BrowseResponse browse(String username) {
        return unwrapped(users().browse(username));
    }

    public BrowseResponse browse(String username, BrowseOptions options) {
        return unwrapped(users().browse(username, options));
    }

    public BrowseResponse browse(String username, CancellationSignal cancellationSignal) {
        return unwrapped(users().browse(username, cancellationSignal));
    }

    public BrowseResponse browse(String username, BrowseOptions options, CancellationSignal cancellationSignal) {
        return unwrapped(users().browse(username, options, cancellationSignal));
    }

    public void changePassword(String password) {
        unwrapped(server().changePassword(password));
    }

    public void changePassword(String password, CancellationSignal cancellationSignal) {
        unwrapped(server().changePassword(password, cancellationSignal));
    }

    public void connectToUser(String username) {
        unwrapped(users().connectToUser(username));
    }

    public void connectToUser(String username, boolean invalidateCache) {
        unwrapped(users().connectToUser(username, invalidateCache));
    }

    public void connectToUser(String username, CancellationSignal cancellationSignal) {
        unwrapped(users().connectToUser(username, cancellationSignal));
    }

    public void connectToUser(String username, boolean invalidateCache, CancellationSignal cancellationSignal) {
        unwrapped(users().connectToUser(username, invalidateCache, cancellationSignal));
    }

    public void dropPrivateRoomMembership(String roomName) {
        unwrapped(rooms().dropPrivateRoomMembership(roomName));
    }

    public void dropPrivateRoomMembership(String roomName, CancellationSignal cancellationSignal) {
        unwrapped(rooms().dropPrivateRoomMembership(roomName, cancellationSignal));
    }

    public void dropPrivateRoomOwnership(String roomName) {
        unwrapped(rooms().dropPrivateRoomOwnership(roomName));
    }

    public void dropPrivateRoomOwnership(String roomName, CancellationSignal cancellationSignal) {
        unwrapped(rooms().dropPrivateRoomOwnership(roomName, cancellationSignal));
    }

    public Integer getDownloadPlaceInQueue(String username, String filename) {
        return unwrapped(transfers().getDownloadPlaceInQueue(username, filename));
    }

    public Integer getDownloadPlaceInQueue(String username, String filename, CancellationSignal cancellationSignal) {
        return unwrapped(transfers().getDownloadPlaceInQueue(username, filename, cancellationSignal));
    }

    public Integer getPrivileges() {
        return unwrapped(server().getPrivileges());
    }

    public Integer getPrivileges(CancellationSignal cancellationSignal) {
        return unwrapped(server().getPrivileges(cancellationSignal));
    }

    public RoomList getRoomList() {
        return unwrapped(rooms().getRoomList());
    }

    public RoomList getRoomList(CancellationSignal cancellationSignal) {
        return unwrapped(rooms().getRoomList(cancellationSignal));
    }

    public UserInfo getUserInfo(String username) {
        return unwrapped(users().getUserInfo(username));
    }

    public UserInfo getUserInfo(String username, CancellationSignal cancellationSignal) {
        return unwrapped(users().getUserInfo(username, cancellationSignal));
    }

    public Boolean getUserPrivileged(String username) {
        return unwrapped(users().getUserPrivileged(username));
    }

    public Boolean getUserPrivileged(String username, CancellationSignal cancellationSignal) {
        return unwrapped(users().getUserPrivileged(username, cancellationSignal));
    }

    public UserStatistics getUserStatistics(String username) {
        return unwrapped(users().getUserStatistics(username));
    }

    public UserStatistics getUserStatistics(String username, CancellationSignal cancellationSignal) {
        return unwrapped(users().getUserStatistics(username, cancellationSignal));
    }

    public UserStatus getUserStatus(String username) {
        return unwrapped(users().getUserStatus(username));
    }

    public UserStatus getUserStatus(String username, CancellationSignal cancellationSignal) {
        return unwrapped(users().getUserStatus(username, cancellationSignal));
    }

    public void grantUserPrivileges(String username, int days) {
        unwrapped(users().grantUserPrivileges(username, days));
    }

    public void grantUserPrivileges(String username, int days, CancellationSignal cancellationSignal) {
        unwrapped(users().grantUserPrivileges(username, days, cancellationSignal));
    }

    public RoomData joinRoom(String roomName) {
        return unwrapped(rooms().joinRoom(roomName));
    }

    public RoomData joinRoom(String roomName, boolean isPrivate) {
        return unwrapped(rooms().joinRoom(roomName, isPrivate));
    }

    public RoomData joinRoom(String roomName, CancellationSignal cancellationSignal) {
        return unwrapped(rooms().joinRoom(roomName, cancellationSignal));
    }

    public RoomData joinRoom(String roomName, boolean isPrivate, CancellationSignal cancellationSignal) {
        return unwrapped(rooms().joinRoom(roomName, isPrivate, cancellationSignal));
    }

    public void leaveRoom(String roomName) {
        unwrapped(rooms().leaveRoom(roomName));
    }

    public void leaveRoom(String roomName, CancellationSignal cancellationSignal) {
        unwrapped(rooms().leaveRoom(roomName, cancellationSignal));
    }

    public Long pingServer() {
        return unwrapped(server().pingServer());
    }

    public Long pingServer(CancellationSignal cancellationSignal) {
        return unwrapped(server().pingServer(cancellationSignal));
    }

    public void removePrivateRoomMember(String roomName, String username) {
        unwrapped(rooms().removePrivateRoomMember(roomName, username));
    }

    public void removePrivateRoomMember(String roomName, String username, CancellationSignal cancellationSignal) {
        unwrapped(rooms().removePrivateRoomMember(roomName, username, cancellationSignal));
    }

    public void removePrivateRoomModerator(String roomName, String username) {
        unwrapped(rooms().removePrivateRoomModerator(roomName, username));
    }

    public void removePrivateRoomModerator(String roomName, String username, CancellationSignal cancellationSignal) {
        unwrapped(rooms().removePrivateRoomModerator(roomName, username, cancellationSignal));
    }

    public void sendPrivateMessage(String username, String message) {
        unwrapped(server().sendPrivateMessage(username, message));
    }

    public void sendPrivateMessage(String username, String message, CancellationSignal cancellationSignal) {
        unwrapped(server().sendPrivateMessage(username, message, cancellationSignal));
    }

    public void sendRoomMessage(String roomName, String message) {
        unwrapped(rooms().sendRoomMessage(roomName, message));
    }

    public void sendRoomMessage(String roomName, String message, CancellationSignal cancellationSignal) {
        unwrapped(rooms().sendRoomMessage(roomName, message, cancellationSignal));
    }

    public void sendUploadSpeed(int speed) {
        unwrapped(server().sendUploadSpeed(speed));
    }

    public void sendUploadSpeed(int speed, CancellationSignal cancellationSignal) {
        unwrapped(server().sendUploadSpeed(speed, cancellationSignal));
    }

    public void setRoomTicker(String roomName, String message) {
        unwrapped(rooms().setRoomTicker(roomName, message));
    }

    public void setRoomTicker(String roomName, String message, CancellationSignal cancellationSignal) {
        unwrapped(rooms().setRoomTicker(roomName, message, cancellationSignal));
    }

    public void setSharedCounts(int directories, int files) {
        unwrapped(server().setSharedCounts(directories, files));
    }

    public void setSharedCounts(int directories, int files, CancellationSignal cancellationSignal) {
        unwrapped(server().setSharedCounts(directories, files, cancellationSignal));
    }

    public void setStatus(UserPresence status) {
        unwrapped(server().setStatus(status));
    }

    public void setStatus(UserPresence status, CancellationSignal cancellationSignal) {
        unwrapped(server().setStatus(status, cancellationSignal));
    }

    public void startPublicChat() {
        unwrapped(server().startPublicChat());
    }

    public void startPublicChat(CancellationSignal cancellationSignal) {
        unwrapped(server().startPublicChat(cancellationSignal));
    }

    public void stopPublicChat() {
        unwrapped(server().stopPublicChat());
    }

    public void stopPublicChat(CancellationSignal cancellationSignal) {
        unwrapped(server().stopPublicChat(cancellationSignal));
    }

    public void unwatchUser(String username) {
        unwrapped(users().unwatchUser(username));
    }

    public void unwatchUser(String username, CancellationSignal cancellationSignal) {
        unwrapped(users().unwatchUser(username, cancellationSignal));
    }

    public UserData watchUser(String username) {
        return unwrapped(users().watchUser(username));
    }

    public UserData watchUser(String username, CancellationSignal cancellationSignal) {
        return unwrapped(users().watchUser(username, cancellationSignal));
    }
}
