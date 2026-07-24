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
import dev.slsk.options.DownloadStreamFactory;
import dev.slsk.options.SearchOptions;
import dev.slsk.options.SoulseekClientOptions;
import dev.slsk.options.SoulseekClientOptionsPatch;
import dev.slsk.options.TransferOptions;
import dev.slsk.options.UploadStreamFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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

    CompletableFuture<Void> acknowledgePrivateMessageAsync(int privateMessageId);

    CompletableFuture<Void> acknowledgePrivateMessageAsync(int privateMessageId, CancellationSignal cancellationSignal);

    CompletableFuture<Void> acknowledgePrivilegeNotificationAsync(int privilegeNotificationId);

    CompletableFuture<Void> acknowledgePrivilegeNotificationAsync(
            int privilegeNotificationId, CancellationSignal cancellationSignal);

    CompletableFuture<Void> addPrivateRoomMemberAsync(String roomName, String username);

    CompletableFuture<Void> addPrivateRoomMemberAsync(
            String roomName, String username, CancellationSignal cancellationSignal);

    CompletableFuture<Void> addPrivateRoomModeratorAsync(String roomName, String username);

    CompletableFuture<Void> addPrivateRoomModeratorAsync(
            String roomName, String username, CancellationSignal cancellationSignal);

    CompletableFuture<BrowseResponse> browseAsync(String username);

    CompletableFuture<BrowseResponse> browseAsync(String username, BrowseOptions options);

    CompletableFuture<BrowseResponse> browseAsync(String username, CancellationSignal cancellationSignal);

    CompletableFuture<BrowseResponse> browseAsync(
            String username, BrowseOptions options, CancellationSignal cancellationSignal);

    CompletableFuture<Void> changePasswordAsync(String password);

    CompletableFuture<Void> changePasswordAsync(String password, CancellationSignal cancellationSignal);

    CompletableFuture<Void> connectAsync(String username, String password);

    CompletableFuture<Void> connectAsync(String username, String password, CancellationSignal cancellationSignal);

    CompletableFuture<Void> connectAsync(String address, int port, String username, String password);

    CompletableFuture<Void> connectAsync(
            String address, int port, String username, String password, CancellationSignal cancellationSignal);

    CompletableFuture<Void> connectToUserAsync(String username);

    CompletableFuture<Void> connectToUserAsync(String username, boolean invalidateCache);

    CompletableFuture<Void> connectToUserAsync(String username, CancellationSignal cancellationSignal);

    CompletableFuture<Void> connectToUserAsync(
            String username, boolean invalidateCache, CancellationSignal cancellationSignal);

    void disconnect();

    void disconnect(String message);

    void disconnect(String message, Exception exception);

    CompletableFuture<Transfer> downloadAsync(String username, String remoteFilename, String localFilename);

    CompletableFuture<Transfer> downloadAsync(String username, String remoteFilename, String localFilename, Long size);

    CompletableFuture<Transfer> downloadAsync(
            String username, String remoteFilename, String localFilename, CancellationSignal cancellationSignal);

    CompletableFuture<Transfer> downloadAsync(
            String username, String remoteFilename, String localFilename, Long size, long startOffset);

    CompletableFuture<Transfer> downloadAsync(
            String username, String remoteFilename, String localFilename, Long size, long startOffset, Integer token);

    CompletableFuture<Transfer> downloadAsync(
            String username,
            String remoteFilename,
            String localFilename,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions options);

    CompletableFuture<Transfer> downloadAsync(
            String username,
            String remoteFilename,
            String localFilename,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions options,
            CancellationSignal cancellationSignal);

    CompletableFuture<Transfer> downloadAsync(
            String username, String remoteFilename, DownloadStreamFactory outputStreamFactory);

    CompletableFuture<Transfer> downloadAsync(
            String username, String remoteFilename, DownloadStreamFactory outputStreamFactory, Long size);

    CompletableFuture<Transfer> downloadAsync(
            String username,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            CancellationSignal cancellationSignal);

    CompletableFuture<Transfer> downloadAsync(
            String username,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset);

    CompletableFuture<Transfer> downloadAsync(
            String username,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token);

    CompletableFuture<Transfer> downloadAsync(
            String username,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions options);

    CompletableFuture<Transfer> downloadAsync(
            String username,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions options,
            CancellationSignal cancellationSignal);

    CompletableFuture<Void> dropPrivateRoomMembershipAsync(String roomName);

    CompletableFuture<Void> dropPrivateRoomMembershipAsync(String roomName, CancellationSignal cancellationSignal);

    CompletableFuture<Void> dropPrivateRoomOwnershipAsync(String roomName);

    CompletableFuture<Void> dropPrivateRoomOwnershipAsync(String roomName, CancellationSignal cancellationSignal);

    CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String username, String remoteFilename, String localFilename);

    CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String username, String remoteFilename, String localFilename, Long size);

    CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String username, String remoteFilename, String localFilename, Long size, long startOffset);

    CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String username, String remoteFilename, String localFilename, Long size, long startOffset, Integer token);

    CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String username,
            String remoteFilename,
            String localFilename,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions options);

    CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String username,
            String remoteFilename,
            String localFilename,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions options,
            CancellationSignal cancellationSignal);

    CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String username, String remoteFilename, DownloadStreamFactory outputStreamFactory);

    CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String username, String remoteFilename, DownloadStreamFactory outputStreamFactory, Long size);

    CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String username,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset);

    CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String username,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token);

    CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String username,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions options);

    CompletableFuture<CompletableFuture<Transfer>> enqueueDownloadAsync(
            String username,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            Long size,
            long startOffset,
            Integer token,
            TransferOptions options,
            CancellationSignal cancellationSignal);

    CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String username, String remoteFilename, String localFilename);

    CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String username, String remoteFilename, String localFilename, Integer token);

    CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String username, String remoteFilename, String localFilename, CancellationSignal cancellationSignal);

    CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String username, String remoteFilename, String localFilename, Integer token, TransferOptions options);

    CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String username,
            String remoteFilename,
            String localFilename,
            Integer token,
            TransferOptions options,
            CancellationSignal cancellationSignal);

    CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String username, String remoteFilename, long size, UploadStreamFactory inputStreamFactory);

    CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String username, String remoteFilename, long size, UploadStreamFactory inputStreamFactory, Integer token);

    CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String username,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            CancellationSignal cancellationSignal);

    CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String username,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            Integer token,
            TransferOptions options);

    CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String username,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            Integer token,
            TransferOptions options,
            CancellationSignal cancellationSignal);

    CompletableFuture<List<Directory>> getDirectoryContentsAsync(String username, String directoryName);

    CompletableFuture<List<Directory>> getDirectoryContentsAsync(String username, String directoryName, int token);

    CompletableFuture<List<Directory>> getDirectoryContentsAsync(
            String username, String directoryName, CancellationSignal cancellationSignal);

    CompletableFuture<List<Directory>> getDirectoryContentsAsync(
            String username, String directoryName, Integer token, CancellationSignal cancellationSignal);

    CompletableFuture<Integer> getDownloadPlaceInQueueAsync(String username, String filename);

    CompletableFuture<Integer> getDownloadPlaceInQueueAsync(
            String username, String filename, CancellationSignal cancellationSignal);

    CompletableFuture<Integer> getPrivilegesAsync();

    CompletableFuture<Integer> getPrivilegesAsync(CancellationSignal cancellationSignal);

    CompletableFuture<RoomList> getRoomListAsync();

    CompletableFuture<RoomList> getRoomListAsync(CancellationSignal cancellationSignal);

    CompletableFuture<InetSocketAddress> getUserEndpointAsync(String username);

    CompletableFuture<InetSocketAddress> getUserEndpointAsync(String username, CancellationSignal cancellationSignal);

    CompletableFuture<UserInfo> getUserInfoAsync(String username);

    CompletableFuture<UserInfo> getUserInfoAsync(String username, CancellationSignal cancellationSignal);

    CompletableFuture<Boolean> getUserPrivilegedAsync(String username);

    CompletableFuture<Boolean> getUserPrivilegedAsync(String username, CancellationSignal cancellationSignal);

    CompletableFuture<UserStatistics> getUserStatisticsAsync(String username);

    CompletableFuture<UserStatistics> getUserStatisticsAsync(String username, CancellationSignal cancellationSignal);

    CompletableFuture<UserStatus> getUserStatusAsync(String username);

    CompletableFuture<UserStatus> getUserStatusAsync(String username, CancellationSignal cancellationSignal);

    CompletableFuture<Void> grantUserPrivilegesAsync(String username, int days);

    CompletableFuture<Void> grantUserPrivilegesAsync(String username, int days, CancellationSignal cancellationSignal);

    CompletableFuture<RoomData> joinRoomAsync(String roomName);

    CompletableFuture<RoomData> joinRoomAsync(String roomName, boolean isPrivate);

    CompletableFuture<RoomData> joinRoomAsync(String roomName, CancellationSignal cancellationSignal);

    CompletableFuture<RoomData> joinRoomAsync(
            String roomName, boolean isPrivate, CancellationSignal cancellationSignal);

    CompletableFuture<Void> leaveRoomAsync(String roomName);

    CompletableFuture<Void> leaveRoomAsync(String roomName, CancellationSignal cancellationSignal);

    CompletableFuture<Long> pingServerAsync();

    CompletableFuture<Long> pingServerAsync(CancellationSignal cancellationSignal);

    CompletableFuture<Boolean> reconfigureOptionsAsync(SoulseekClientOptionsPatch patch);

    CompletableFuture<Boolean> reconfigureOptionsAsync(
            SoulseekClientOptionsPatch patch, CancellationSignal cancellationSignal);

    CompletableFuture<Void> removePrivateRoomMemberAsync(String roomName, String username);

    CompletableFuture<Void> removePrivateRoomMemberAsync(
            String roomName, String username, CancellationSignal cancellationSignal);

    CompletableFuture<Void> removePrivateRoomModeratorAsync(String roomName, String username);

    CompletableFuture<Void> removePrivateRoomModeratorAsync(
            String roomName, String username, CancellationSignal cancellationSignal);

    CompletableFuture<SearchResult> searchAsync(SearchQuery query);

    CompletableFuture<SearchResult> searchAsync(SearchQuery query, CancellationSignal cancellationSignal);

    CompletableFuture<SearchResult> searchAsync(SearchQuery query, SearchScope scope);

    CompletableFuture<SearchResult> searchAsync(SearchQuery query, SearchScope scope, Integer token);

    CompletableFuture<SearchResult> searchAsync(
            SearchQuery query, SearchScope scope, Integer token, SearchOptions options);

    CompletableFuture<SearchResult> searchAsync(
            SearchQuery query,
            SearchScope scope,
            Integer token,
            SearchOptions options,
            CancellationSignal cancellationSignal);

    CompletableFuture<Search> searchAsync(SearchQuery query, Consumer<SearchResponse> responseHandler);

    CompletableFuture<Search> searchAsync(
            SearchQuery query, Consumer<SearchResponse> responseHandler, CancellationSignal cancellationSignal);

    CompletableFuture<Search> searchAsync(
            SearchQuery query, Consumer<SearchResponse> responseHandler, SearchScope scope);

    CompletableFuture<Search> searchAsync(
            SearchQuery query, Consumer<SearchResponse> responseHandler, SearchScope scope, Integer token);

    CompletableFuture<Search> searchAsync(
            SearchQuery query,
            Consumer<SearchResponse> responseHandler,
            SearchScope scope,
            Integer token,
            SearchOptions options);

    CompletableFuture<Search> searchAsync(
            SearchQuery query,
            Consumer<SearchResponse> responseHandler,
            SearchScope scope,
            Integer token,
            SearchOptions options,
            CancellationSignal cancellationSignal);

    CompletableFuture<Void> sendPrivateMessageAsync(String username, String message);

    CompletableFuture<Void> sendPrivateMessageAsync(
            String username, String message, CancellationSignal cancellationSignal);

    CompletableFuture<Void> sendRoomMessageAsync(String roomName, String message);

    CompletableFuture<Void> sendRoomMessageAsync(
            String roomName, String message, CancellationSignal cancellationSignal);

    CompletableFuture<Void> sendUploadSpeedAsync(int speed);

    CompletableFuture<Void> sendUploadSpeedAsync(int speed, CancellationSignal cancellationSignal);

    CompletableFuture<Void> setRoomTickerAsync(String roomName, String message);

    CompletableFuture<Void> setRoomTickerAsync(String roomName, String message, CancellationSignal cancellationSignal);

    CompletableFuture<Void> setSharedCountsAsync(int directories, int files);

    CompletableFuture<Void> setSharedCountsAsync(int directories, int files, CancellationSignal cancellationSignal);

    CompletableFuture<Void> setStatusAsync(UserPresence status);

    CompletableFuture<Void> setStatusAsync(UserPresence status, CancellationSignal cancellationSignal);

    CompletableFuture<Void> startPublicChatAsync();

    CompletableFuture<Void> startPublicChatAsync(CancellationSignal cancellationSignal);

    CompletableFuture<Void> stopPublicChatAsync();

    CompletableFuture<Void> stopPublicChatAsync(CancellationSignal cancellationSignal);

    CompletableFuture<Void> unwatchUserAsync(String username);

    CompletableFuture<Void> unwatchUserAsync(String username, CancellationSignal cancellationSignal);

    CompletableFuture<Transfer> uploadAsync(String username, String remoteFilename, String localFilename);

    CompletableFuture<Transfer> uploadAsync(
            String username, String remoteFilename, String localFilename, Integer token);

    CompletableFuture<Transfer> uploadAsync(
            String username, String remoteFilename, String localFilename, CancellationSignal cancellationSignal);

    CompletableFuture<Transfer> uploadAsync(
            String username, String remoteFilename, String localFilename, TransferOptions options);

    CompletableFuture<Transfer> uploadAsync(
            String username, String remoteFilename, String localFilename, Integer token, TransferOptions options);

    CompletableFuture<Transfer> uploadAsync(
            String username,
            String remoteFilename,
            String localFilename,
            Integer token,
            TransferOptions options,
            CancellationSignal cancellationSignal);

    CompletableFuture<Transfer> uploadAsync(
            String username, String remoteFilename, long size, UploadStreamFactory inputStreamFactory);

    CompletableFuture<Transfer> uploadAsync(
            String username, String remoteFilename, long size, UploadStreamFactory inputStreamFactory, Integer token);

    CompletableFuture<Transfer> uploadAsync(
            String username,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            CancellationSignal cancellationSignal);

    CompletableFuture<Transfer> uploadAsync(
            String username,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            TransferOptions options);

    CompletableFuture<Transfer> uploadAsync(
            String username,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            Integer token,
            TransferOptions options);

    CompletableFuture<Transfer> uploadAsync(
            String username,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            Integer token,
            TransferOptions options,
            CancellationSignal cancellationSignal);

    CompletableFuture<UserData> watchUserAsync(String username);

    CompletableFuture<UserData> watchUserAsync(String username, CancellationSignal cancellationSignal);

    @Override
    void close();
}
