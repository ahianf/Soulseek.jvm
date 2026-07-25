// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.diagnostics.DiagnosticSource;
import dev.slsk.events.BrowseProgressUpdatedEvent;
import dev.slsk.events.DistributedChildEvent;
import dev.slsk.events.DistributedParentEvent;
import dev.slsk.events.DownloadDeniedEvent;
import dev.slsk.events.DownloadFailedEvent;
import dev.slsk.events.PrivateMessageReceivedEvent;
import dev.slsk.events.PrivilegeNotificationReceivedEvent;
import dev.slsk.events.PublicChatMessageReceivedEvent;
import dev.slsk.events.RoomJoinedEvent;
import dev.slsk.events.RoomLeftEvent;
import dev.slsk.events.RoomMessageReceivedEvent;
import dev.slsk.events.RoomTickerAddedEvent;
import dev.slsk.events.RoomTickerListReceivedEvent;
import dev.slsk.events.RoomTickerRemovedEvent;
import dev.slsk.events.SearchRequestEvent;
import dev.slsk.events.SearchRequestResponseEvent;
import dev.slsk.events.SearchResponseReceivedEvent;
import dev.slsk.events.SearchStateChangedEvent;
import dev.slsk.events.SoulseekClientDisconnectedEvent;
import dev.slsk.events.SoulseekClientStateChangedEvent;
import dev.slsk.events.TransferProgressUpdatedEvent;
import dev.slsk.events.TransferStateChangedEvent;
import dev.slsk.events.UserCannotConnectEvent;
import dev.slsk.options.BrowseOptions;
import dev.slsk.options.SoulseekClientOptions;
import dev.slsk.options.SoulseekClientOptionsPatch;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.function.Consumer;

/**
 * A client for the Soulseek file sharing network.
 *
 * <p>C# optional parameters from the pinned behavioral baseline are
 * represented by progressive overloads ending in a canonical overload that
 * accepts a {@link CancellationSignal}.</p>
 */
public interface SoulseekClient extends AutoCloseable, DiagnosticSource {
    /**
     * Creates a client with default options.
     *
     * @param minorVersion the application-specific network minor version
     * @return a new client
     * @throws IllegalArgumentException when {@code minorVersion <= 100}
     */
    static SoulseekClient create(int minorVersion) {
        return new DefaultSoulseekClient(minorVersion);
    }

    /**
     * Creates a client with the supplied options.
     *
     * @param minorVersion the application-specific network minor version
     * @param options the client options, or {@code null} for defaults
     * @return a new client
     * @throws IllegalArgumentException when {@code minorVersion <= 100}
     */
    static SoulseekClient create(int minorVersion, SoulseekClientOptions options) {
        return new DefaultSoulseekClient(minorVersion, options);
    }

    /**
     * Returns whether client events are configured as asynchronous.
     *
     * @return the process-wide event-dispatch setting
     */
    static boolean isRaiseEventsAsynchronously() {
        return DefaultSoulseekClient.isRaiseEventsAsynchronously();
    }

    /**
     * Changes the process-wide client event-dispatch setting.
     *
     * @param value whether events should be dispatched asynchronously
     */
    static void setRaiseEventsAsynchronously(boolean value) {
        DefaultSoulseekClient.setRaiseEventsAsynchronously(value);
    }

    String getAddress();

    DistributedNetworkInfo getDistributedNetwork();

    List<Transfer> getDownloads();

    InetAddress getIpAddress();

    InetSocketAddress getIpEndpoint();

    int getMajorVersion();

    int getMinorVersion();

    SoulseekClientOptions getOptions();

    Integer getPort();

    ServerInfo getServerInfo();

    SoulseekClientState getState();

    List<Transfer> getUploads();

    String getUsername();

    void addBrowseProgressUpdatedListener(SoulseekClientEventListener<BrowseProgressUpdatedEvent> listener);

    void removeBrowseProgressUpdatedListener(SoulseekClientEventListener<BrowseProgressUpdatedEvent> listener);

    void addConnectedListener(SoulseekClientEventListener<Void> listener);

    void removeConnectedListener(SoulseekClientEventListener<Void> listener);

    void addDemotedFromDistributedBranchRootListener(SoulseekClientEventListener<Void> listener);

    void removeDemotedFromDistributedBranchRootListener(SoulseekClientEventListener<Void> listener);

    void addDisconnectedListener(SoulseekClientEventListener<SoulseekClientDisconnectedEvent> listener);

    void removeDisconnectedListener(SoulseekClientEventListener<SoulseekClientDisconnectedEvent> listener);

    void addDistributedChildAddedListener(SoulseekClientEventListener<DistributedChildEvent> listener);

    void removeDistributedChildAddedListener(SoulseekClientEventListener<DistributedChildEvent> listener);

    void addDistributedChildDisconnectedListener(SoulseekClientEventListener<DistributedChildEvent> listener);

    void removeDistributedChildDisconnectedListener(SoulseekClientEventListener<DistributedChildEvent> listener);

    void addDistributedNetworkResetListener(SoulseekClientEventListener<Void> listener);

    void removeDistributedNetworkResetListener(SoulseekClientEventListener<Void> listener);

    void addDistributedNetworkStateChangedListener(SoulseekClientEventListener<DistributedNetworkInfo> listener);

    void removeDistributedNetworkStateChangedListener(SoulseekClientEventListener<DistributedNetworkInfo> listener);

    void addDistributedParentAdoptedListener(SoulseekClientEventListener<DistributedParentEvent> listener);

    void removeDistributedParentAdoptedListener(SoulseekClientEventListener<DistributedParentEvent> listener);

    void addDistributedParentDisconnectedListener(SoulseekClientEventListener<DistributedParentEvent> listener);

    void removeDistributedParentDisconnectedListener(SoulseekClientEventListener<DistributedParentEvent> listener);

    void addDownloadDeniedListener(SoulseekClientEventListener<DownloadDeniedEvent> listener);

    void removeDownloadDeniedListener(SoulseekClientEventListener<DownloadDeniedEvent> listener);

    void addDownloadFailedListener(SoulseekClientEventListener<DownloadFailedEvent> listener);

    void removeDownloadFailedListener(SoulseekClientEventListener<DownloadFailedEvent> listener);

    void addExcludedSearchPhrasesReceivedListener(SoulseekClientEventListener<List<String>> listener);

    void removeExcludedSearchPhrasesReceivedListener(SoulseekClientEventListener<List<String>> listener);

    void addGlobalMessageReceivedListener(SoulseekClientEventListener<String> listener);

    void removeGlobalMessageReceivedListener(SoulseekClientEventListener<String> listener);

    void addKickedFromServerListener(SoulseekClientEventListener<Void> listener);

    void removeKickedFromServerListener(SoulseekClientEventListener<Void> listener);

    void addLoggedInListener(SoulseekClientEventListener<Void> listener);

    void removeLoggedInListener(SoulseekClientEventListener<Void> listener);

    void addPrivateMessageReceivedListener(SoulseekClientEventListener<PrivateMessageReceivedEvent> listener);

    void removePrivateMessageReceivedListener(SoulseekClientEventListener<PrivateMessageReceivedEvent> listener);

    void addPrivateRoomMembershipAddedListener(SoulseekClientEventListener<String> listener);

    void removePrivateRoomMembershipAddedListener(SoulseekClientEventListener<String> listener);

    void addPrivateRoomMembershipRemovedListener(SoulseekClientEventListener<String> listener);

    void removePrivateRoomMembershipRemovedListener(SoulseekClientEventListener<String> listener);

    void addPrivateRoomModeratedUserListReceivedListener(SoulseekClientEventListener<RoomInfo> listener);

    void removePrivateRoomModeratedUserListReceivedListener(SoulseekClientEventListener<RoomInfo> listener);

    void addPrivateRoomModerationAddedListener(SoulseekClientEventListener<String> listener);

    void removePrivateRoomModerationAddedListener(SoulseekClientEventListener<String> listener);

    void addPrivateRoomModerationRemovedListener(SoulseekClientEventListener<String> listener);

    void removePrivateRoomModerationRemovedListener(SoulseekClientEventListener<String> listener);

    void addPrivateRoomUserListReceivedListener(SoulseekClientEventListener<RoomInfo> listener);

    void removePrivateRoomUserListReceivedListener(SoulseekClientEventListener<RoomInfo> listener);

    void addPrivilegedUserListReceivedListener(SoulseekClientEventListener<List<String>> listener);

    void removePrivilegedUserListReceivedListener(SoulseekClientEventListener<List<String>> listener);

    void addPrivilegeNotificationReceivedListener(
            SoulseekClientEventListener<PrivilegeNotificationReceivedEvent> listener);

    void removePrivilegeNotificationReceivedListener(
            SoulseekClientEventListener<PrivilegeNotificationReceivedEvent> listener);

    void addPromotedToDistributedBranchRootListener(SoulseekClientEventListener<Void> listener);

    void removePromotedToDistributedBranchRootListener(SoulseekClientEventListener<Void> listener);

    void addPublicChatMessageReceivedListener(SoulseekClientEventListener<PublicChatMessageReceivedEvent> listener);

    void removePublicChatMessageReceivedListener(SoulseekClientEventListener<PublicChatMessageReceivedEvent> listener);

    void addRoomJoinedListener(SoulseekClientEventListener<RoomJoinedEvent> listener);

    void removeRoomJoinedListener(SoulseekClientEventListener<RoomJoinedEvent> listener);

    void addRoomLeftListener(SoulseekClientEventListener<RoomLeftEvent> listener);

    void removeRoomLeftListener(SoulseekClientEventListener<RoomLeftEvent> listener);

    void addRoomListReceivedListener(SoulseekClientEventListener<RoomList> listener);

    void removeRoomListReceivedListener(SoulseekClientEventListener<RoomList> listener);

    void addRoomMessageReceivedListener(SoulseekClientEventListener<RoomMessageReceivedEvent> listener);

    void removeRoomMessageReceivedListener(SoulseekClientEventListener<RoomMessageReceivedEvent> listener);

    void addRoomTickerAddedListener(SoulseekClientEventListener<RoomTickerAddedEvent> listener);

    void removeRoomTickerAddedListener(SoulseekClientEventListener<RoomTickerAddedEvent> listener);

    void addRoomTickerListReceivedListener(SoulseekClientEventListener<RoomTickerListReceivedEvent> listener);

    void removeRoomTickerListReceivedListener(SoulseekClientEventListener<RoomTickerListReceivedEvent> listener);

    void addRoomTickerRemovedListener(SoulseekClientEventListener<RoomTickerRemovedEvent> listener);

    void removeRoomTickerRemovedListener(SoulseekClientEventListener<RoomTickerRemovedEvent> listener);

    void addSearchRequestReceivedListener(SoulseekClientEventListener<SearchRequestEvent> listener);

    void removeSearchRequestReceivedListener(SoulseekClientEventListener<SearchRequestEvent> listener);

    void addSearchResponseDeliveredListener(SoulseekClientEventListener<SearchRequestResponseEvent> listener);

    void removeSearchResponseDeliveredListener(SoulseekClientEventListener<SearchRequestResponseEvent> listener);

    void addSearchResponseDeliveryFailedListener(SoulseekClientEventListener<SearchRequestResponseEvent> listener);

    void removeSearchResponseDeliveryFailedListener(SoulseekClientEventListener<SearchRequestResponseEvent> listener);

    void addSearchResponseReceivedListener(SoulseekClientEventListener<SearchResponseReceivedEvent> listener);

    void removeSearchResponseReceivedListener(SoulseekClientEventListener<SearchResponseReceivedEvent> listener);

    void addSearchStateChangedListener(SoulseekClientEventListener<SearchStateChangedEvent> listener);

    void removeSearchStateChangedListener(SoulseekClientEventListener<SearchStateChangedEvent> listener);

    void addServerInfoReceivedListener(SoulseekClientEventListener<ServerInfo> listener);

    void removeServerInfoReceivedListener(SoulseekClientEventListener<ServerInfo> listener);

    void addStateChangedListener(SoulseekClientEventListener<SoulseekClientStateChangedEvent> listener);

    void removeStateChangedListener(SoulseekClientEventListener<SoulseekClientStateChangedEvent> listener);

    void addTransferProgressUpdatedListener(SoulseekClientEventListener<TransferProgressUpdatedEvent> listener);

    void removeTransferProgressUpdatedListener(SoulseekClientEventListener<TransferProgressUpdatedEvent> listener);

    void addTransferStateChangedListener(SoulseekClientEventListener<TransferStateChangedEvent> listener);

    void removeTransferStateChangedListener(SoulseekClientEventListener<TransferStateChangedEvent> listener);

    void addUserCannotConnectListener(SoulseekClientEventListener<UserCannotConnectEvent> listener);

    void removeUserCannotConnectListener(SoulseekClientEventListener<UserCannotConnectEvent> listener);

    void addUserStatisticsChangedListener(SoulseekClientEventListener<UserStatistics> listener);

    void removeUserStatisticsChangedListener(SoulseekClientEventListener<UserStatistics> listener);

    void addUserStatusChangedListener(SoulseekClientEventListener<UserStatus> listener);

    void removeUserStatusChangedListener(SoulseekClientEventListener<UserStatus> listener);

    int getNextToken();

    void acknowledgePrivateMessage(int privateMessageId);

    void acknowledgePrivateMessage(int privateMessageId, CancellationSignal cancellationSignal);

    void acknowledgePrivilegeNotification(int privilegeNotificationId);

    void acknowledgePrivilegeNotification(int privilegeNotificationId, CancellationSignal cancellationSignal);

    void addPrivateRoomMember(String roomName, String username);

    void addPrivateRoomMember(String roomName, String username, CancellationSignal cancellationSignal);

    void addPrivateRoomModerator(String roomName, String username);

    void addPrivateRoomModerator(String roomName, String username, CancellationSignal cancellationSignal);

    BrowseResponse browse(String username);

    BrowseResponse browse(String username, BrowseOptions options);

    BrowseResponse browse(String username, CancellationSignal cancellationSignal);

    BrowseResponse browse(String username, BrowseOptions options, CancellationSignal cancellationSignal);

    void changePassword(String password);

    void changePassword(String password, CancellationSignal cancellationSignal);

    void connect(String username, String password);

    void connect(String username, String password, CancellationSignal cancellationSignal);

    void connect(String address, int port, String username, String password);

    void connect(String address, int port, String username, String password, CancellationSignal cancellationSignal);

    void connectToUser(String username);

    void connectToUser(String username, boolean invalidateCache);

    void connectToUser(String username, CancellationSignal cancellationSignal);

    void connectToUser(String username, boolean invalidateCache, CancellationSignal cancellationSignal);

    void disconnect();

    void disconnect(String message);

    void disconnect(String message, Exception exception);

    void dropPrivateRoomMembership(String roomName);

    void dropPrivateRoomMembership(String roomName, CancellationSignal cancellationSignal);

    void dropPrivateRoomOwnership(String roomName);

    void dropPrivateRoomOwnership(String roomName, CancellationSignal cancellationSignal);

    List<Directory> getDirectoryContents(String username, String directoryName);

    List<Directory> getDirectoryContents(String username, String directoryName, int token);

    List<Directory> getDirectoryContents(String username, String directoryName, CancellationSignal cancellationSignal);

    List<Directory> getDirectoryContents(
            String username, String directoryName, Integer token, CancellationSignal cancellationSignal);

    Integer getDownloadPlaceInQueue(String username, String filename);

    Integer getDownloadPlaceInQueue(String username, String filename, CancellationSignal cancellationSignal);

    Integer getPrivileges();

    Integer getPrivileges(CancellationSignal cancellationSignal);

    RoomList getRoomList();

    RoomList getRoomList(CancellationSignal cancellationSignal);

    InetSocketAddress getUserEndpoint(String username);

    InetSocketAddress getUserEndpoint(String username, CancellationSignal cancellationSignal);

    UserInfo getUserInfo(String username);

    UserInfo getUserInfo(String username, CancellationSignal cancellationSignal);

    Boolean getUserPrivileged(String username);

    Boolean getUserPrivileged(String username, CancellationSignal cancellationSignal);

    UserStatistics getUserStatistics(String username);

    UserStatistics getUserStatistics(String username, CancellationSignal cancellationSignal);

    UserStatus getUserStatus(String username);

    UserStatus getUserStatus(String username, CancellationSignal cancellationSignal);

    void grantUserPrivileges(String username, int days);

    void grantUserPrivileges(String username, int days, CancellationSignal cancellationSignal);

    RoomData joinRoom(String roomName);

    RoomData joinRoom(String roomName, boolean isPrivate);

    RoomData joinRoom(String roomName, CancellationSignal cancellationSignal);

    RoomData joinRoom(String roomName, boolean isPrivate, CancellationSignal cancellationSignal);

    void leaveRoom(String roomName);

    void leaveRoom(String roomName, CancellationSignal cancellationSignal);

    Long pingServer();

    Long pingServer(CancellationSignal cancellationSignal);

    Boolean reconfigureOptions(SoulseekClientOptionsPatch patch);

    Boolean reconfigureOptions(SoulseekClientOptionsPatch patch, CancellationSignal cancellationSignal);

    void removePrivateRoomMember(String roomName, String username);

    void removePrivateRoomMember(String roomName, String username, CancellationSignal cancellationSignal);

    void removePrivateRoomModerator(String roomName, String username);

    void removePrivateRoomModerator(String roomName, String username, CancellationSignal cancellationSignal);

    /**
     * Downloads a file, blocking until the transfer finishes.
     *
     * @param request the download to perform
     * @return the completed transfer
     */
    Transfer download(DownloadRequest request);

    /**
     * Asks a peer to queue a download, blocking only until it accepts.
     *
     * @param request the download to enqueue
     * @return a handle whose {@link TransferHandle#await()} blocks for completion
     */
    TransferHandle enqueueDownload(DownloadRequest request);

    /**
     * Uploads a file, blocking until the transfer finishes.
     *
     * @param request the upload to perform
     * @return the completed transfer
     */
    Transfer upload(UploadRequest request);

    /**
     * Offers a peer an upload, blocking only until it accepts.
     *
     * @param request the upload to enqueue
     * @return a handle whose {@link TransferHandle#await()} blocks for completion
     */
    TransferHandle enqueueUpload(UploadRequest request);

    /**
     * Searches, blocking until the search completes, and returns everything it
     * collected.
     *
     * @param request the search to perform
     * @return the collected result
     */
    SearchResult search(SearchRequest request);

    /**
     * Searches, streaming each response to {@code responseHandler} as it
     * arrives.
     *
     * @param request the search to perform
     * @param responseHandler receives each response
     * @return the completed search
     */
    Search search(SearchRequest request, Consumer<SearchResponse> responseHandler);

    void sendPrivateMessage(String username, String message);

    void sendPrivateMessage(String username, String message, CancellationSignal cancellationSignal);

    void sendRoomMessage(String roomName, String message);

    void sendRoomMessage(String roomName, String message, CancellationSignal cancellationSignal);

    void sendUploadSpeed(int speed);

    void sendUploadSpeed(int speed, CancellationSignal cancellationSignal);

    void setRoomTicker(String roomName, String message);

    void setRoomTicker(String roomName, String message, CancellationSignal cancellationSignal);

    void setSharedCounts(int directories, int files);

    void setSharedCounts(int directories, int files, CancellationSignal cancellationSignal);

    void setStatus(UserPresence status);

    void setStatus(UserPresence status, CancellationSignal cancellationSignal);

    void startPublicChat();

    void startPublicChat(CancellationSignal cancellationSignal);

    void stopPublicChat();

    void stopPublicChat(CancellationSignal cancellationSignal);

    void unwatchUser(String username);

    void unwatchUser(String username, CancellationSignal cancellationSignal);

    UserData watchUser(String username);

    UserData watchUser(String username, CancellationSignal cancellationSignal);

    @Override
    void close();
}
