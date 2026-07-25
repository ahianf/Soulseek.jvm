// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.diagnostics.DiagnosticEventListener;
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
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The client's event listener registry: ninety-four add/remove pairs, the
 * per-event listener lists, and the dispatch that raises them.
 *
 * <p>Pure plumbing, and almost four hundred lines of it. It is a base class
 * rather than a collaborator because the add/remove methods are the public API
 * — they have to be members of the client, not delegations to something it
 * holds — while none of the machinery behind them is anything the client's own
 * logic should have to read past.
 */
abstract class ClientEventSupport {

    private final Map<Event, CopyOnWriteArrayList<SoulseekClientEventListener<?>>> listeners =
            new EnumMap<>(Event.class);

    ClientEventSupport() {
        for (Event event : Event.values()) {
            listeners.put(event, new CopyOnWriteArrayList<>());
        }
    }

    public final void addBrowseProgressUpdatedListener(SoulseekClientEventListener<BrowseProgressUpdatedEvent> value) {
        add(Event.BROWSE_PROGRESS_UPDATED, value);
    }

    public final void removeBrowseProgressUpdatedListener(
            SoulseekClientEventListener<BrowseProgressUpdatedEvent> value) {
        remove(Event.BROWSE_PROGRESS_UPDATED, value);
    }

    public final void addConnectedListener(SoulseekClientEventListener<Void> value) {
        add(Event.CONNECTED, value);
    }

    public final void removeConnectedListener(SoulseekClientEventListener<Void> value) {
        remove(Event.CONNECTED, value);
    }

    public final void addDemotedFromDistributedBranchRootListener(SoulseekClientEventListener<Void> value) {
        add(Event.DEMOTED_FROM_DISTRIBUTED_BRANCH_ROOT, value);
    }

    public final void removeDemotedFromDistributedBranchRootListener(SoulseekClientEventListener<Void> value) {
        remove(Event.DEMOTED_FROM_DISTRIBUTED_BRANCH_ROOT, value);
    }

    public final void addDiagnosticGeneratedListener(DiagnosticEventListener value) {
        add(Event.DIAGNOSTIC_GENERATED, value);
    }

    public final void removeDiagnosticGeneratedListener(DiagnosticEventListener value) {
        remove(Event.DIAGNOSTIC_GENERATED, value);
    }

    public final void addDisconnectedListener(SoulseekClientEventListener<SoulseekClientDisconnectedEvent> value) {
        add(Event.DISCONNECTED, value);
    }

    public final void removeDisconnectedListener(SoulseekClientEventListener<SoulseekClientDisconnectedEvent> value) {
        remove(Event.DISCONNECTED, value);
    }

    public final void addDistributedChildAddedListener(SoulseekClientEventListener<DistributedChildEvent> value) {
        add(Event.DISTRIBUTED_CHILD_ADDED, value);
    }

    public final void removeDistributedChildAddedListener(SoulseekClientEventListener<DistributedChildEvent> value) {
        remove(Event.DISTRIBUTED_CHILD_ADDED, value);
    }

    public final void addDistributedChildDisconnectedListener(
            SoulseekClientEventListener<DistributedChildEvent> value) {
        add(Event.DISTRIBUTED_CHILD_DISCONNECTED, value);
    }

    public final void removeDistributedChildDisconnectedListener(
            SoulseekClientEventListener<DistributedChildEvent> value) {
        remove(Event.DISTRIBUTED_CHILD_DISCONNECTED, value);
    }

    public final void addDistributedNetworkResetListener(SoulseekClientEventListener<Void> value) {
        add(Event.DISTRIBUTED_NETWORK_RESET, value);
    }

    public final void removeDistributedNetworkResetListener(SoulseekClientEventListener<Void> value) {
        remove(Event.DISTRIBUTED_NETWORK_RESET, value);
    }

    public final void addDistributedNetworkStateChangedListener(
            SoulseekClientEventListener<DistributedNetworkInfo> value) {
        add(Event.DISTRIBUTED_NETWORK_STATE_CHANGED, value);
    }

    public final void removeDistributedNetworkStateChangedListener(
            SoulseekClientEventListener<DistributedNetworkInfo> value) {
        remove(Event.DISTRIBUTED_NETWORK_STATE_CHANGED, value);
    }

    public final void addDistributedParentAdoptedListener(SoulseekClientEventListener<DistributedParentEvent> value) {
        add(Event.DISTRIBUTED_PARENT_ADOPTED, value);
    }

    public final void removeDistributedParentAdoptedListener(
            SoulseekClientEventListener<DistributedParentEvent> value) {
        remove(Event.DISTRIBUTED_PARENT_ADOPTED, value);
    }

    public final void addDistributedParentDisconnectedListener(
            SoulseekClientEventListener<DistributedParentEvent> value) {
        add(Event.DISTRIBUTED_PARENT_DISCONNECTED, value);
    }

    public final void removeDistributedParentDisconnectedListener(
            SoulseekClientEventListener<DistributedParentEvent> value) {
        remove(Event.DISTRIBUTED_PARENT_DISCONNECTED, value);
    }

    public final void addDownloadDeniedListener(SoulseekClientEventListener<DownloadDeniedEvent> value) {
        add(Event.DOWNLOAD_DENIED, value);
    }

    public final void removeDownloadDeniedListener(SoulseekClientEventListener<DownloadDeniedEvent> value) {
        remove(Event.DOWNLOAD_DENIED, value);
    }

    public final void addDownloadFailedListener(SoulseekClientEventListener<DownloadFailedEvent> value) {
        add(Event.DOWNLOAD_FAILED, value);
    }

    public final void removeDownloadFailedListener(SoulseekClientEventListener<DownloadFailedEvent> value) {
        remove(Event.DOWNLOAD_FAILED, value);
    }

    public final void addExcludedSearchPhrasesReceivedListener(SoulseekClientEventListener<List<String>> value) {
        add(Event.EXCLUDED_SEARCH_PHRASES_RECEIVED, value);
    }

    public final void removeExcludedSearchPhrasesReceivedListener(SoulseekClientEventListener<List<String>> value) {
        remove(Event.EXCLUDED_SEARCH_PHRASES_RECEIVED, value);
    }

    public final void addGlobalMessageReceivedListener(SoulseekClientEventListener<String> value) {
        add(Event.GLOBAL_MESSAGE_RECEIVED, value);
    }

    public final void removeGlobalMessageReceivedListener(SoulseekClientEventListener<String> value) {
        remove(Event.GLOBAL_MESSAGE_RECEIVED, value);
    }

    public final void addKickedFromServerListener(SoulseekClientEventListener<Void> value) {
        add(Event.KICKED_FROM_SERVER, value);
    }

    public final void removeKickedFromServerListener(SoulseekClientEventListener<Void> value) {
        remove(Event.KICKED_FROM_SERVER, value);
    }

    public final void addLoggedInListener(SoulseekClientEventListener<Void> value) {
        add(Event.LOGGED_IN, value);
    }

    public final void removeLoggedInListener(SoulseekClientEventListener<Void> value) {
        remove(Event.LOGGED_IN, value);
    }

    public final void addPrivateMessageReceivedListener(
            SoulseekClientEventListener<PrivateMessageReceivedEvent> value) {
        add(Event.PRIVATE_MESSAGE_RECEIVED, value);
    }

    public final void removePrivateMessageReceivedListener(
            SoulseekClientEventListener<PrivateMessageReceivedEvent> value) {
        remove(Event.PRIVATE_MESSAGE_RECEIVED, value);
    }

    public final void addPrivateRoomMembershipAddedListener(SoulseekClientEventListener<String> value) {
        add(Event.PRIVATE_ROOM_MEMBERSHIP_ADDED, value);
    }

    public final void removePrivateRoomMembershipAddedListener(SoulseekClientEventListener<String> value) {
        remove(Event.PRIVATE_ROOM_MEMBERSHIP_ADDED, value);
    }

    public final void addPrivateRoomMembershipRemovedListener(SoulseekClientEventListener<String> value) {
        add(Event.PRIVATE_ROOM_MEMBERSHIP_REMOVED, value);
    }

    public final void removePrivateRoomMembershipRemovedListener(SoulseekClientEventListener<String> value) {
        remove(Event.PRIVATE_ROOM_MEMBERSHIP_REMOVED, value);
    }

    public final void addPrivateRoomModeratedUserListReceivedListener(SoulseekClientEventListener<RoomInfo> value) {
        add(Event.PRIVATE_ROOM_MODERATED_USER_LIST_RECEIVED, value);
    }

    public final void removePrivateRoomModeratedUserListReceivedListener(SoulseekClientEventListener<RoomInfo> value) {
        remove(Event.PRIVATE_ROOM_MODERATED_USER_LIST_RECEIVED, value);
    }

    public final void addPrivateRoomModerationAddedListener(SoulseekClientEventListener<String> value) {
        add(Event.PRIVATE_ROOM_MODERATION_ADDED, value);
    }

    public final void removePrivateRoomModerationAddedListener(SoulseekClientEventListener<String> value) {
        remove(Event.PRIVATE_ROOM_MODERATION_ADDED, value);
    }

    public final void addPrivateRoomModerationRemovedListener(SoulseekClientEventListener<String> value) {
        add(Event.PRIVATE_ROOM_MODERATION_REMOVED, value);
    }

    public final void removePrivateRoomModerationRemovedListener(SoulseekClientEventListener<String> value) {
        remove(Event.PRIVATE_ROOM_MODERATION_REMOVED, value);
    }

    public final void addPrivateRoomUserListReceivedListener(SoulseekClientEventListener<RoomInfo> value) {
        add(Event.PRIVATE_ROOM_USER_LIST_RECEIVED, value);
    }

    public final void removePrivateRoomUserListReceivedListener(SoulseekClientEventListener<RoomInfo> value) {
        remove(Event.PRIVATE_ROOM_USER_LIST_RECEIVED, value);
    }

    public final void addPrivilegedUserListReceivedListener(SoulseekClientEventListener<List<String>> value) {
        add(Event.PRIVILEGED_USER_LIST_RECEIVED, value);
    }

    public final void removePrivilegedUserListReceivedListener(SoulseekClientEventListener<List<String>> value) {
        remove(Event.PRIVILEGED_USER_LIST_RECEIVED, value);
    }

    public final void addPrivilegeNotificationReceivedListener(
            SoulseekClientEventListener<PrivilegeNotificationReceivedEvent> value) {
        add(Event.PRIVILEGE_NOTIFICATION_RECEIVED, value);
    }

    public final void removePrivilegeNotificationReceivedListener(
            SoulseekClientEventListener<PrivilegeNotificationReceivedEvent> value) {
        remove(Event.PRIVILEGE_NOTIFICATION_RECEIVED, value);
    }

    public final void addPromotedToDistributedBranchRootListener(SoulseekClientEventListener<Void> value) {
        add(Event.PROMOTED_TO_DISTRIBUTED_BRANCH_ROOT, value);
    }

    public final void removePromotedToDistributedBranchRootListener(SoulseekClientEventListener<Void> value) {
        remove(Event.PROMOTED_TO_DISTRIBUTED_BRANCH_ROOT, value);
    }

    public final void addPublicChatMessageReceivedListener(
            SoulseekClientEventListener<PublicChatMessageReceivedEvent> value) {
        add(Event.PUBLIC_CHAT_MESSAGE_RECEIVED, value);
    }

    public final void removePublicChatMessageReceivedListener(
            SoulseekClientEventListener<PublicChatMessageReceivedEvent> value) {
        remove(Event.PUBLIC_CHAT_MESSAGE_RECEIVED, value);
    }

    public final void addRoomJoinedListener(SoulseekClientEventListener<RoomJoinedEvent> value) {
        add(Event.ROOM_JOINED, value);
    }

    public final void removeRoomJoinedListener(SoulseekClientEventListener<RoomJoinedEvent> value) {
        remove(Event.ROOM_JOINED, value);
    }

    public final void addRoomLeftListener(SoulseekClientEventListener<RoomLeftEvent> value) {
        add(Event.ROOM_LEFT, value);
    }

    public final void removeRoomLeftListener(SoulseekClientEventListener<RoomLeftEvent> value) {
        remove(Event.ROOM_LEFT, value);
    }

    public final void addRoomListReceivedListener(SoulseekClientEventListener<RoomList> value) {
        add(Event.ROOM_LIST_RECEIVED, value);
    }

    public final void removeRoomListReceivedListener(SoulseekClientEventListener<RoomList> value) {
        remove(Event.ROOM_LIST_RECEIVED, value);
    }

    public final void addRoomMessageReceivedListener(SoulseekClientEventListener<RoomMessageReceivedEvent> value) {
        add(Event.ROOM_MESSAGE_RECEIVED, value);
    }

    public final void removeRoomMessageReceivedListener(SoulseekClientEventListener<RoomMessageReceivedEvent> value) {
        remove(Event.ROOM_MESSAGE_RECEIVED, value);
    }

    public final void addRoomTickerAddedListener(SoulseekClientEventListener<RoomTickerAddedEvent> value) {
        add(Event.ROOM_TICKER_ADDED, value);
    }

    public final void removeRoomTickerAddedListener(SoulseekClientEventListener<RoomTickerAddedEvent> value) {
        remove(Event.ROOM_TICKER_ADDED, value);
    }

    public final void addRoomTickerListReceivedListener(
            SoulseekClientEventListener<RoomTickerListReceivedEvent> value) {
        add(Event.ROOM_TICKER_LIST_RECEIVED, value);
    }

    public final void removeRoomTickerListReceivedListener(
            SoulseekClientEventListener<RoomTickerListReceivedEvent> value) {
        remove(Event.ROOM_TICKER_LIST_RECEIVED, value);
    }

    public final void addRoomTickerRemovedListener(SoulseekClientEventListener<RoomTickerRemovedEvent> value) {
        add(Event.ROOM_TICKER_REMOVED, value);
    }

    public final void removeRoomTickerRemovedListener(SoulseekClientEventListener<RoomTickerRemovedEvent> value) {
        remove(Event.ROOM_TICKER_REMOVED, value);
    }

    public final void addSearchRequestReceivedListener(SoulseekClientEventListener<SearchRequestEvent> value) {
        add(Event.SEARCH_REQUEST_RECEIVED, value);
    }

    public final void removeSearchRequestReceivedListener(SoulseekClientEventListener<SearchRequestEvent> value) {
        remove(Event.SEARCH_REQUEST_RECEIVED, value);
    }

    public final void addSearchResponseDeliveredListener(
            SoulseekClientEventListener<SearchRequestResponseEvent> value) {
        add(Event.SEARCH_RESPONSE_DELIVERED, value);
    }

    public final void removeSearchResponseDeliveredListener(
            SoulseekClientEventListener<SearchRequestResponseEvent> value) {
        remove(Event.SEARCH_RESPONSE_DELIVERED, value);
    }

    public final void addSearchResponseDeliveryFailedListener(
            SoulseekClientEventListener<SearchRequestResponseEvent> value) {
        add(Event.SEARCH_RESPONSE_DELIVERY_FAILED, value);
    }

    public final void removeSearchResponseDeliveryFailedListener(
            SoulseekClientEventListener<SearchRequestResponseEvent> value) {
        remove(Event.SEARCH_RESPONSE_DELIVERY_FAILED, value);
    }

    public final void addSearchResponseReceivedListener(
            SoulseekClientEventListener<SearchResponseReceivedEvent> value) {
        add(Event.SEARCH_RESPONSE_RECEIVED, value);
    }

    public final void removeSearchResponseReceivedListener(
            SoulseekClientEventListener<SearchResponseReceivedEvent> value) {
        remove(Event.SEARCH_RESPONSE_RECEIVED, value);
    }

    public final void addSearchStateChangedListener(SoulseekClientEventListener<SearchStateChangedEvent> value) {
        add(Event.SEARCH_STATE_CHANGED, value);
    }

    public final void removeSearchStateChangedListener(SoulseekClientEventListener<SearchStateChangedEvent> value) {
        remove(Event.SEARCH_STATE_CHANGED, value);
    }

    public final void addServerInfoReceivedListener(SoulseekClientEventListener<ServerInfo> value) {
        add(Event.SERVER_INFO_RECEIVED, value);
    }

    public final void removeServerInfoReceivedListener(SoulseekClientEventListener<ServerInfo> value) {
        remove(Event.SERVER_INFO_RECEIVED, value);
    }

    public final void addStateChangedListener(SoulseekClientEventListener<SoulseekClientStateChangedEvent> value) {
        add(Event.STATE_CHANGED, value);
    }

    public final void removeStateChangedListener(SoulseekClientEventListener<SoulseekClientStateChangedEvent> value) {
        remove(Event.STATE_CHANGED, value);
    }

    public final void addTransferProgressUpdatedListener(
            SoulseekClientEventListener<TransferProgressUpdatedEvent> value) {
        add(Event.TRANSFER_PROGRESS_UPDATED, value);
    }

    public final void removeTransferProgressUpdatedListener(
            SoulseekClientEventListener<TransferProgressUpdatedEvent> value) {
        remove(Event.TRANSFER_PROGRESS_UPDATED, value);
    }

    public final void addTransferStateChangedListener(SoulseekClientEventListener<TransferStateChangedEvent> value) {
        add(Event.TRANSFER_STATE_CHANGED, value);
    }

    public final void removeTransferStateChangedListener(SoulseekClientEventListener<TransferStateChangedEvent> value) {
        remove(Event.TRANSFER_STATE_CHANGED, value);
    }

    public final void addUserCannotConnectListener(SoulseekClientEventListener<UserCannotConnectEvent> value) {
        add(Event.USER_CANNOT_CONNECT, value);
    }

    public final void removeUserCannotConnectListener(SoulseekClientEventListener<UserCannotConnectEvent> value) {
        remove(Event.USER_CANNOT_CONNECT, value);
    }

    public final void addUserStatisticsChangedListener(SoulseekClientEventListener<UserStatistics> value) {
        add(Event.USER_STATISTICS_CHANGED, value);
    }

    public final void removeUserStatisticsChangedListener(SoulseekClientEventListener<UserStatistics> value) {
        remove(Event.USER_STATISTICS_CHANGED, value);
    }

    public final void addUserStatusChangedListener(SoulseekClientEventListener<UserStatus> value) {
        add(Event.USER_STATUS_CHANGED, value);
    }

    public final void removeUserStatusChangedListener(SoulseekClientEventListener<UserStatus> value) {
        remove(Event.USER_STATUS_CHANGED, value);
    }

    protected <T> void add(Event event, SoulseekClientEventListener<T> listener) {
        listeners.get(event).add(Objects.requireNonNull(listener, "listener"));
    }

    protected <T> void remove(Event event, SoulseekClientEventListener<T> listener) {
        listeners.get(event).remove(listener);
    }

    protected <T> void raise(Event event, T eventData) {
        raiseFrom(this, event, eventData);
    }

    enum Event {
        BROWSE_PROGRESS_UPDATED,
        CONNECTED,
        DEMOTED_FROM_DISTRIBUTED_BRANCH_ROOT,
        DIAGNOSTIC_GENERATED,
        DISCONNECTED,
        DISTRIBUTED_CHILD_ADDED,
        DISTRIBUTED_CHILD_DISCONNECTED,
        DISTRIBUTED_NETWORK_RESET,
        DISTRIBUTED_NETWORK_STATE_CHANGED,
        DISTRIBUTED_PARENT_ADOPTED,
        DISTRIBUTED_PARENT_DISCONNECTED,
        DOWNLOAD_DENIED,
        DOWNLOAD_FAILED,
        EXCLUDED_SEARCH_PHRASES_RECEIVED,
        GLOBAL_MESSAGE_RECEIVED,
        KICKED_FROM_SERVER,
        LOGGED_IN,
        PRIVATE_MESSAGE_RECEIVED,
        PRIVATE_ROOM_MEMBERSHIP_ADDED,
        PRIVATE_ROOM_MEMBERSHIP_REMOVED,
        PRIVATE_ROOM_MODERATED_USER_LIST_RECEIVED,
        PRIVATE_ROOM_MODERATION_ADDED,
        PRIVATE_ROOM_MODERATION_REMOVED,
        PRIVATE_ROOM_USER_LIST_RECEIVED,
        PRIVILEGED_USER_LIST_RECEIVED,
        PRIVILEGE_NOTIFICATION_RECEIVED,
        PROMOTED_TO_DISTRIBUTED_BRANCH_ROOT,
        PUBLIC_CHAT_MESSAGE_RECEIVED,
        ROOM_JOINED,
        ROOM_LEFT,
        ROOM_LIST_RECEIVED,
        ROOM_MESSAGE_RECEIVED,
        ROOM_TICKER_ADDED,
        ROOM_TICKER_LIST_RECEIVED,
        ROOM_TICKER_REMOVED,
        SEARCH_REQUEST_RECEIVED,
        SEARCH_RESPONSE_DELIVERED,
        SEARCH_RESPONSE_DELIVERY_FAILED,
        SEARCH_RESPONSE_RECEIVED,
        SEARCH_STATE_CHANGED,
        SERVER_INFO_RECEIVED,
        STATE_CHANGED,
        TRANSFER_PROGRESS_UPDATED,
        TRANSFER_STATE_CHANGED,
        USER_CANNOT_CONNECT,
        USER_STATISTICS_CHANGED,
        USER_STATUS_CHANGED
    }

    @SuppressWarnings("unchecked")
    protected <T> void raiseFrom(Object sender, Event event, T eventData) {
        for (SoulseekClientEventListener<?> listener : listeners.get(event)) {
            ((SoulseekClientEventListener<T>) listener).handle(sender, eventData);
        }
    }
}
