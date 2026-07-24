// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.diagnostics.IDiagnosticGenerator;
import dev.slsk.eventargs.BrowseProgressUpdatedEventArgs;
import dev.slsk.eventargs.DistributedChildEventArgs;
import dev.slsk.eventargs.DistributedParentEventArgs;
import dev.slsk.eventargs.DownloadDeniedEventArgs;
import dev.slsk.eventargs.DownloadFailedEventArgs;
import dev.slsk.eventargs.PrivateMessageReceivedEventArgs;
import dev.slsk.eventargs.PrivilegeNotificationReceivedEventArgs;
import dev.slsk.eventargs.PublicChatMessageReceivedEventArgs;
import dev.slsk.eventargs.RoomJoinedEventArgs;
import dev.slsk.eventargs.RoomLeftEventArgs;
import dev.slsk.eventargs.RoomMessageReceivedEventArgs;
import dev.slsk.eventargs.RoomTickerAddedEventArgs;
import dev.slsk.eventargs.RoomTickerListReceivedEventArgs;
import dev.slsk.eventargs.RoomTickerRemovedEventArgs;
import dev.slsk.eventargs.SearchRequestEventArgs;
import dev.slsk.eventargs.SearchRequestResponseEventArgs;
import dev.slsk.eventargs.SearchResponseReceivedEventArgs;
import dev.slsk.eventargs.SearchStateChangedEventArgs;
import dev.slsk.eventargs.SoulseekClientDisconnectedEventArgs;
import dev.slsk.eventargs.SoulseekClientStateChangedEventArgs;
import dev.slsk.eventargs.TransferProgressUpdatedEventArgs;
import dev.slsk.eventargs.TransferStateChangedEventArgs;
import dev.slsk.eventargs.UserCannotConnectEventArgs;
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
 * <p>This is the direct Java counterpart of the public C#
 * {@code ISoulseekClient} contract. C# optional parameters are represented by
 * progressive overloads ending in a canonical overload that accepts a
 * {@link CancellationToken}.</p>
 */
public interface ISoulseekClient extends AutoCloseable, IDiagnosticGenerator {
    String getAddress();

    DistributedNetworkInfo getDistributedNetwork();

    List<Transfer> getDownloads();

    InetAddress getIpAddress();

    InetSocketAddress getIpEndPoint();

    int getMajorVersion();

    int getMinorVersion();

    SoulseekClientOptions getOptions();

    Integer getPort();

    ServerInfo getServerInfo();

    SoulseekClientStates getState();

    List<Transfer> getUploads();

    String getUsername();

    void addBrowseProgressUpdatedListener(SoulseekClientEventListener<BrowseProgressUpdatedEventArgs> listener);

    void removeBrowseProgressUpdatedListener(SoulseekClientEventListener<BrowseProgressUpdatedEventArgs> listener);

    void addConnectedListener(SoulseekClientEventListener<Void> listener);

    void removeConnectedListener(SoulseekClientEventListener<Void> listener);

    void addDemotedFromDistributedBranchRootListener(SoulseekClientEventListener<Void> listener);

    void removeDemotedFromDistributedBranchRootListener(SoulseekClientEventListener<Void> listener);

    void addDisconnectedListener(SoulseekClientEventListener<SoulseekClientDisconnectedEventArgs> listener);

    void removeDisconnectedListener(SoulseekClientEventListener<SoulseekClientDisconnectedEventArgs> listener);

    void addDistributedChildAddedListener(SoulseekClientEventListener<DistributedChildEventArgs> listener);

    void removeDistributedChildAddedListener(SoulseekClientEventListener<DistributedChildEventArgs> listener);

    void addDistributedChildDisconnectedListener(SoulseekClientEventListener<DistributedChildEventArgs> listener);

    void removeDistributedChildDisconnectedListener(SoulseekClientEventListener<DistributedChildEventArgs> listener);

    void addDistributedNetworkResetListener(SoulseekClientEventListener<Void> listener);

    void removeDistributedNetworkResetListener(SoulseekClientEventListener<Void> listener);

    void addDistributedNetworkStateChangedListener(SoulseekClientEventListener<DistributedNetworkInfo> listener);

    void removeDistributedNetworkStateChangedListener(SoulseekClientEventListener<DistributedNetworkInfo> listener);

    void addDistributedParentAdoptedListener(SoulseekClientEventListener<DistributedParentEventArgs> listener);

    void removeDistributedParentAdoptedListener(SoulseekClientEventListener<DistributedParentEventArgs> listener);

    void addDistributedParentDisconnectedListener(SoulseekClientEventListener<DistributedParentEventArgs> listener);

    void removeDistributedParentDisconnectedListener(SoulseekClientEventListener<DistributedParentEventArgs> listener);

    void addDownloadDeniedListener(SoulseekClientEventListener<DownloadDeniedEventArgs> listener);

    void removeDownloadDeniedListener(SoulseekClientEventListener<DownloadDeniedEventArgs> listener);

    void addDownloadFailedListener(SoulseekClientEventListener<DownloadFailedEventArgs> listener);

    void removeDownloadFailedListener(SoulseekClientEventListener<DownloadFailedEventArgs> listener);

    void addExcludedSearchPhrasesReceivedListener(SoulseekClientEventListener<List<String>> listener);

    void removeExcludedSearchPhrasesReceivedListener(SoulseekClientEventListener<List<String>> listener);

    void addGlobalMessageReceivedListener(SoulseekClientEventListener<String> listener);

    void removeGlobalMessageReceivedListener(SoulseekClientEventListener<String> listener);

    void addKickedFromServerListener(SoulseekClientEventListener<Void> listener);

    void removeKickedFromServerListener(SoulseekClientEventListener<Void> listener);

    void addLoggedInListener(SoulseekClientEventListener<Void> listener);

    void removeLoggedInListener(SoulseekClientEventListener<Void> listener);

    void addPrivateMessageReceivedListener(SoulseekClientEventListener<PrivateMessageReceivedEventArgs> listener);

    void removePrivateMessageReceivedListener(SoulseekClientEventListener<PrivateMessageReceivedEventArgs> listener);

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
            SoulseekClientEventListener<PrivilegeNotificationReceivedEventArgs> listener);

    void removePrivilegeNotificationReceivedListener(
            SoulseekClientEventListener<PrivilegeNotificationReceivedEventArgs> listener);

    void addPromotedToDistributedBranchRootListener(SoulseekClientEventListener<Void> listener);

    void removePromotedToDistributedBranchRootListener(SoulseekClientEventListener<Void> listener);

    void addPublicChatMessageReceivedListener(SoulseekClientEventListener<PublicChatMessageReceivedEventArgs> listener);

    void removePublicChatMessageReceivedListener(
            SoulseekClientEventListener<PublicChatMessageReceivedEventArgs> listener);

    void addRoomJoinedListener(SoulseekClientEventListener<RoomJoinedEventArgs> listener);

    void removeRoomJoinedListener(SoulseekClientEventListener<RoomJoinedEventArgs> listener);

    void addRoomLeftListener(SoulseekClientEventListener<RoomLeftEventArgs> listener);

    void removeRoomLeftListener(SoulseekClientEventListener<RoomLeftEventArgs> listener);

    void addRoomListReceivedListener(SoulseekClientEventListener<RoomList> listener);

    void removeRoomListReceivedListener(SoulseekClientEventListener<RoomList> listener);

    void addRoomMessageReceivedListener(SoulseekClientEventListener<RoomMessageReceivedEventArgs> listener);

    void removeRoomMessageReceivedListener(SoulseekClientEventListener<RoomMessageReceivedEventArgs> listener);

    void addRoomTickerAddedListener(SoulseekClientEventListener<RoomTickerAddedEventArgs> listener);

    void removeRoomTickerAddedListener(SoulseekClientEventListener<RoomTickerAddedEventArgs> listener);

    void addRoomTickerListReceivedListener(SoulseekClientEventListener<RoomTickerListReceivedEventArgs> listener);

    void removeRoomTickerListReceivedListener(SoulseekClientEventListener<RoomTickerListReceivedEventArgs> listener);

    void addRoomTickerRemovedListener(SoulseekClientEventListener<RoomTickerRemovedEventArgs> listener);

    void removeRoomTickerRemovedListener(SoulseekClientEventListener<RoomTickerRemovedEventArgs> listener);

    void addSearchRequestReceivedListener(SoulseekClientEventListener<SearchRequestEventArgs> listener);

    void removeSearchRequestReceivedListener(SoulseekClientEventListener<SearchRequestEventArgs> listener);

    void addSearchResponseDeliveredListener(SoulseekClientEventListener<SearchRequestResponseEventArgs> listener);

    void removeSearchResponseDeliveredListener(SoulseekClientEventListener<SearchRequestResponseEventArgs> listener);

    void addSearchResponseDeliveryFailedListener(SoulseekClientEventListener<SearchRequestResponseEventArgs> listener);

    void removeSearchResponseDeliveryFailedListener(
            SoulseekClientEventListener<SearchRequestResponseEventArgs> listener);

    void addSearchResponseReceivedListener(SoulseekClientEventListener<SearchResponseReceivedEventArgs> listener);

    void removeSearchResponseReceivedListener(SoulseekClientEventListener<SearchResponseReceivedEventArgs> listener);

    void addSearchStateChangedListener(SoulseekClientEventListener<SearchStateChangedEventArgs> listener);

    void removeSearchStateChangedListener(SoulseekClientEventListener<SearchStateChangedEventArgs> listener);

    void addServerInfoReceivedListener(SoulseekClientEventListener<ServerInfo> listener);

    void removeServerInfoReceivedListener(SoulseekClientEventListener<ServerInfo> listener);

    void addStateChangedListener(SoulseekClientEventListener<SoulseekClientStateChangedEventArgs> listener);

    void removeStateChangedListener(SoulseekClientEventListener<SoulseekClientStateChangedEventArgs> listener);

    void addTransferProgressUpdatedListener(SoulseekClientEventListener<TransferProgressUpdatedEventArgs> listener);

    void removeTransferProgressUpdatedListener(SoulseekClientEventListener<TransferProgressUpdatedEventArgs> listener);

    void addTransferStateChangedListener(SoulseekClientEventListener<TransferStateChangedEventArgs> listener);

    void removeTransferStateChangedListener(SoulseekClientEventListener<TransferStateChangedEventArgs> listener);

    void addUserCannotConnectListener(SoulseekClientEventListener<UserCannotConnectEventArgs> listener);

    void removeUserCannotConnectListener(SoulseekClientEventListener<UserCannotConnectEventArgs> listener);

    void addUserStatisticsChangedListener(SoulseekClientEventListener<UserStatistics> listener);

    void removeUserStatisticsChangedListener(SoulseekClientEventListener<UserStatistics> listener);

    void addUserStatusChangedListener(SoulseekClientEventListener<UserStatus> listener);

    void removeUserStatusChangedListener(SoulseekClientEventListener<UserStatus> listener);

    int getNextToken();

    CompletableFuture<Void> acknowledgePrivateMessageAsync(int privateMessageId);

    CompletableFuture<Void> acknowledgePrivateMessageAsync(int privateMessageId, CancellationToken cancellationToken);

    CompletableFuture<Void> acknowledgePrivilegeNotificationAsync(int privilegeNotificationId);

    CompletableFuture<Void> acknowledgePrivilegeNotificationAsync(
            int privilegeNotificationId, CancellationToken cancellationToken);

    CompletableFuture<Void> addPrivateRoomMemberAsync(String roomName, String username);

    CompletableFuture<Void> addPrivateRoomMemberAsync(
            String roomName, String username, CancellationToken cancellationToken);

    CompletableFuture<Void> addPrivateRoomModeratorAsync(String roomName, String username);

    CompletableFuture<Void> addPrivateRoomModeratorAsync(
            String roomName, String username, CancellationToken cancellationToken);

    CompletableFuture<BrowseResponse> browseAsync(String username);

    CompletableFuture<BrowseResponse> browseAsync(String username, BrowseOptions options);

    CompletableFuture<BrowseResponse> browseAsync(String username, CancellationToken cancellationToken);

    CompletableFuture<BrowseResponse> browseAsync(
            String username, BrowseOptions options, CancellationToken cancellationToken);

    CompletableFuture<Void> changePasswordAsync(String password);

    CompletableFuture<Void> changePasswordAsync(String password, CancellationToken cancellationToken);

    CompletableFuture<Void> connectAsync(String username, String password);

    CompletableFuture<Void> connectAsync(String username, String password, CancellationToken cancellationToken);

    CompletableFuture<Void> connectAsync(String address, int port, String username, String password);

    CompletableFuture<Void> connectAsync(
            String address, int port, String username, String password, CancellationToken cancellationToken);

    CompletableFuture<Void> connectToUserAsync(String username);

    CompletableFuture<Void> connectToUserAsync(String username, boolean invalidateCache);

    CompletableFuture<Void> connectToUserAsync(String username, CancellationToken cancellationToken);

    CompletableFuture<Void> connectToUserAsync(
            String username, boolean invalidateCache, CancellationToken cancellationToken);

    void disconnect();

    void disconnect(String message);

    void disconnect(String message, Exception exception);

    CompletableFuture<Transfer> downloadAsync(String username, String remoteFilename, String localFilename);

    CompletableFuture<Transfer> downloadAsync(String username, String remoteFilename, String localFilename, Long size);

    CompletableFuture<Transfer> downloadAsync(
            String username, String remoteFilename, String localFilename, CancellationToken cancellationToken);

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
            CancellationToken cancellationToken);

    CompletableFuture<Transfer> downloadAsync(
            String username, String remoteFilename, DownloadStreamFactory outputStreamFactory);

    CompletableFuture<Transfer> downloadAsync(
            String username, String remoteFilename, DownloadStreamFactory outputStreamFactory, Long size);

    CompletableFuture<Transfer> downloadAsync(
            String username,
            String remoteFilename,
            DownloadStreamFactory outputStreamFactory,
            CancellationToken cancellationToken);

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
            CancellationToken cancellationToken);

    CompletableFuture<Void> dropPrivateRoomMembershipAsync(String roomName);

    CompletableFuture<Void> dropPrivateRoomMembershipAsync(String roomName, CancellationToken cancellationToken);

    CompletableFuture<Void> dropPrivateRoomOwnershipAsync(String roomName);

    CompletableFuture<Void> dropPrivateRoomOwnershipAsync(String roomName, CancellationToken cancellationToken);

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
            CancellationToken cancellationToken);

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
            CancellationToken cancellationToken);

    CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String username, String remoteFilename, String localFilename);

    CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String username, String remoteFilename, String localFilename, Integer token);

    CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String username, String remoteFilename, String localFilename, CancellationToken cancellationToken);

    CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String username, String remoteFilename, String localFilename, Integer token, TransferOptions options);

    CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String username,
            String remoteFilename,
            String localFilename,
            Integer token,
            TransferOptions options,
            CancellationToken cancellationToken);

    CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String username, String remoteFilename, long size, UploadStreamFactory inputStreamFactory);

    CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String username, String remoteFilename, long size, UploadStreamFactory inputStreamFactory, Integer token);

    CompletableFuture<CompletableFuture<Transfer>> enqueueUploadAsync(
            String username,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            CancellationToken cancellationToken);

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
            CancellationToken cancellationToken);

    CompletableFuture<List<Directory>> getDirectoryContentsAsync(String username, String directoryName);

    CompletableFuture<List<Directory>> getDirectoryContentsAsync(String username, String directoryName, int token);

    CompletableFuture<List<Directory>> getDirectoryContentsAsync(
            String username, String directoryName, CancellationToken cancellationToken);

    CompletableFuture<List<Directory>> getDirectoryContentsAsync(
            String username, String directoryName, Integer token, CancellationToken cancellationToken);

    CompletableFuture<Integer> getDownloadPlaceInQueueAsync(String username, String filename);

    CompletableFuture<Integer> getDownloadPlaceInQueueAsync(
            String username, String filename, CancellationToken cancellationToken);

    CompletableFuture<Integer> getPrivilegesAsync();

    CompletableFuture<Integer> getPrivilegesAsync(CancellationToken cancellationToken);

    CompletableFuture<RoomList> getRoomListAsync();

    CompletableFuture<RoomList> getRoomListAsync(CancellationToken cancellationToken);

    CompletableFuture<InetSocketAddress> getUserEndPointAsync(String username);

    CompletableFuture<InetSocketAddress> getUserEndPointAsync(String username, CancellationToken cancellationToken);

    CompletableFuture<UserInfo> getUserInfoAsync(String username);

    CompletableFuture<UserInfo> getUserInfoAsync(String username, CancellationToken cancellationToken);

    CompletableFuture<Boolean> getUserPrivilegedAsync(String username);

    CompletableFuture<Boolean> getUserPrivilegedAsync(String username, CancellationToken cancellationToken);

    CompletableFuture<UserStatistics> getUserStatisticsAsync(String username);

    CompletableFuture<UserStatistics> getUserStatisticsAsync(String username, CancellationToken cancellationToken);

    CompletableFuture<UserStatus> getUserStatusAsync(String username);

    CompletableFuture<UserStatus> getUserStatusAsync(String username, CancellationToken cancellationToken);

    CompletableFuture<Void> grantUserPrivilegesAsync(String username, int days);

    CompletableFuture<Void> grantUserPrivilegesAsync(String username, int days, CancellationToken cancellationToken);

    CompletableFuture<RoomData> joinRoomAsync(String roomName);

    CompletableFuture<RoomData> joinRoomAsync(String roomName, boolean isPrivate);

    CompletableFuture<RoomData> joinRoomAsync(String roomName, CancellationToken cancellationToken);

    CompletableFuture<RoomData> joinRoomAsync(String roomName, boolean isPrivate, CancellationToken cancellationToken);

    CompletableFuture<Void> leaveRoomAsync(String roomName);

    CompletableFuture<Void> leaveRoomAsync(String roomName, CancellationToken cancellationToken);

    CompletableFuture<Long> pingServerAsync();

    CompletableFuture<Long> pingServerAsync(CancellationToken cancellationToken);

    CompletableFuture<Boolean> reconfigureOptionsAsync(SoulseekClientOptionsPatch patch);

    CompletableFuture<Boolean> reconfigureOptionsAsync(
            SoulseekClientOptionsPatch patch, CancellationToken cancellationToken);

    CompletableFuture<Void> removePrivateRoomMemberAsync(String roomName, String username);

    CompletableFuture<Void> removePrivateRoomMemberAsync(
            String roomName, String username, CancellationToken cancellationToken);

    CompletableFuture<Void> removePrivateRoomModeratorAsync(String roomName, String username);

    CompletableFuture<Void> removePrivateRoomModeratorAsync(
            String roomName, String username, CancellationToken cancellationToken);

    CompletableFuture<SearchResult> searchAsync(SearchQuery query);

    CompletableFuture<SearchResult> searchAsync(SearchQuery query, CancellationToken cancellationToken);

    CompletableFuture<SearchResult> searchAsync(SearchQuery query, SearchScope scope);

    CompletableFuture<SearchResult> searchAsync(SearchQuery query, SearchScope scope, Integer token);

    CompletableFuture<SearchResult> searchAsync(
            SearchQuery query, SearchScope scope, Integer token, SearchOptions options);

    CompletableFuture<SearchResult> searchAsync(
            SearchQuery query,
            SearchScope scope,
            Integer token,
            SearchOptions options,
            CancellationToken cancellationToken);

    CompletableFuture<Search> searchAsync(SearchQuery query, Consumer<SearchResponse> responseHandler);

    CompletableFuture<Search> searchAsync(
            SearchQuery query, Consumer<SearchResponse> responseHandler, CancellationToken cancellationToken);

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
            CancellationToken cancellationToken);

    CompletableFuture<Void> sendPrivateMessageAsync(String username, String message);

    CompletableFuture<Void> sendPrivateMessageAsync(
            String username, String message, CancellationToken cancellationToken);

    CompletableFuture<Void> sendRoomMessageAsync(String roomName, String message);

    CompletableFuture<Void> sendRoomMessageAsync(String roomName, String message, CancellationToken cancellationToken);

    CompletableFuture<Void> sendUploadSpeedAsync(int speed);

    CompletableFuture<Void> sendUploadSpeedAsync(int speed, CancellationToken cancellationToken);

    CompletableFuture<Void> setRoomTickerAsync(String roomName, String message);

    CompletableFuture<Void> setRoomTickerAsync(String roomName, String message, CancellationToken cancellationToken);

    CompletableFuture<Void> setSharedCountsAsync(int directories, int files);

    CompletableFuture<Void> setSharedCountsAsync(int directories, int files, CancellationToken cancellationToken);

    CompletableFuture<Void> setStatusAsync(UserPresence status);

    CompletableFuture<Void> setStatusAsync(UserPresence status, CancellationToken cancellationToken);

    CompletableFuture<Void> startPublicChatAsync();

    CompletableFuture<Void> startPublicChatAsync(CancellationToken cancellationToken);

    CompletableFuture<Void> stopPublicChatAsync();

    CompletableFuture<Void> stopPublicChatAsync(CancellationToken cancellationToken);

    CompletableFuture<Void> unwatchUserAsync(String username);

    CompletableFuture<Void> unwatchUserAsync(String username, CancellationToken cancellationToken);

    CompletableFuture<Transfer> uploadAsync(String username, String remoteFilename, String localFilename);

    CompletableFuture<Transfer> uploadAsync(
            String username, String remoteFilename, String localFilename, Integer token);

    CompletableFuture<Transfer> uploadAsync(
            String username, String remoteFilename, String localFilename, CancellationToken cancellationToken);

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
            CancellationToken cancellationToken);

    CompletableFuture<Transfer> uploadAsync(
            String username, String remoteFilename, long size, UploadStreamFactory inputStreamFactory);

    CompletableFuture<Transfer> uploadAsync(
            String username, String remoteFilename, long size, UploadStreamFactory inputStreamFactory, Integer token);

    CompletableFuture<Transfer> uploadAsync(
            String username,
            String remoteFilename,
            long size,
            UploadStreamFactory inputStreamFactory,
            CancellationToken cancellationToken);

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
            CancellationToken cancellationToken);

    CompletableFuture<UserData> watchUserAsync(String username);

    CompletableFuture<UserData> watchUserAsync(String username, CancellationToken cancellationToken);

    @Override
    void close();
}
