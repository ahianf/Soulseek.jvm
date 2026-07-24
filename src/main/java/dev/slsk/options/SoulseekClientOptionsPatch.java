// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.options;

import dev.slsk.SearchResponseCache;
import dev.slsk.UserEndpointCache;
import java.net.InetAddress;

/** A patch for {@link SoulseekClientOptions}. */
public class SoulseekClientOptionsPatch {
    private final Boolean acceptDistributedChildren;
    private final Boolean acceptPrivateRoomInvitations;
    private final Boolean autoAcknowledgePrivateMessages;
    private final Boolean autoAcknowledgePrivilegeNotifications;
    private final BrowseResponseResolver browseResponseResolver;
    private final Boolean deduplicateSearchRequests;
    private final DirectoryContentsResolver directoryContentsResolver;
    private final Integer distributedChildLimit;
    private final ConnectionOptions distributedConnectionOptions;
    private final Boolean enableDistributedNetwork;
    private final Boolean enableListener;
    private final EnqueueDownloadCallback enqueueDownload;
    private final ConnectionOptions incomingConnectionOptions;
    private final InetAddress listenIpAddress;
    private final Integer listenPort;
    private final Integer maximumDownloadSpeed;
    private final Integer maximumUploadSpeed;
    private final ConnectionOptions peerConnectionOptions;
    private final PlaceInQueueResolver placeInQueueResolver;
    private final SearchResponseCache searchResponseCache;
    private final SearchResponseResolver searchResponseResolver;
    private final ConnectionOptions serverConnectionOptions;
    private final ConnectionOptions transferConnectionOptions;
    private final UserEndpointCache userEndpointCache;
    private final UserInfoResolver userInfoResolver;

    /** Creates an empty patch. */
    public SoulseekClientOptionsPatch() {
        this(
                null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
                null, null, null, null, null, null, null, null);
    }

    /** Creates a patch through its listener switch. */
    public SoulseekClientOptionsPatch(Boolean enableListener) {
        this(
                enableListener,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    /** Creates a patch through its listener address. */
    public SoulseekClientOptionsPatch(Boolean enableListener, InetAddress listenIpAddress) {
        this(
                enableListener,
                listenIpAddress,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    /** Creates a patch through its listener port. */
    public SoulseekClientOptionsPatch(Boolean enableListener, InetAddress listenIpAddress, Integer listenPort) {
        this(
                enableListener,
                listenIpAddress,
                listenPort,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    /**
     * Creates a complete options patch.
     *
     * <p>Boxed primitives preserve the nullable C# option values. A
     * {@code null} reference means that the property is not replaced.
     */
    public SoulseekClientOptionsPatch(
            Boolean enableListener,
            InetAddress listenIpAddress,
            Integer listenPort,
            Boolean enableDistributedNetwork,
            Boolean acceptDistributedChildren,
            Integer distributedChildLimit,
            Integer maximumUploadSpeed,
            Integer maximumDownloadSpeed,
            Boolean deduplicateSearchRequests,
            Boolean autoAcknowledgePrivateMessages,
            Boolean autoAcknowledgePrivilegeNotifications,
            Boolean acceptPrivateRoomInvitations,
            ConnectionOptions serverConnectionOptions,
            ConnectionOptions peerConnectionOptions,
            ConnectionOptions transferConnectionOptions,
            ConnectionOptions incomingConnectionOptions,
            ConnectionOptions distributedConnectionOptions,
            UserEndpointCache userEndpointCache,
            SearchResponseResolver searchResponseResolver,
            SearchResponseCache searchResponseCache,
            BrowseResponseResolver browseResponseResolver,
            DirectoryContentsResolver directoryContentsResolver,
            UserInfoResolver userInfoResolver,
            EnqueueDownloadCallback enqueueDownload,
            PlaceInQueueResolver placeInQueueResolver) {
        this.enableListener = enableListener;
        this.listenIpAddress = listenIpAddress;
        this.listenPort = listenPort;

        if (listenPort != null && (listenPort < 1024 || listenPort > 65_535)) {
            throw new IllegalArgumentException("listenPort must be between 1024 and 65535");
        }

        this.enableDistributedNetwork = enableDistributedNetwork;
        this.acceptDistributedChildren = acceptDistributedChildren;
        this.distributedChildLimit = distributedChildLimit;

        if (distributedChildLimit != null && distributedChildLimit < 0) {
            throw new IllegalArgumentException("distributedChildLimit must be greater than or equal to zero");
        }

        this.maximumUploadSpeed = maximumUploadSpeed;
        this.maximumDownloadSpeed = maximumDownloadSpeed;
        this.deduplicateSearchRequests = deduplicateSearchRequests;
        this.autoAcknowledgePrivateMessages = autoAcknowledgePrivateMessages;
        this.autoAcknowledgePrivilegeNotifications = autoAcknowledgePrivilegeNotifications;
        this.acceptPrivateRoomInvitations = acceptPrivateRoomInvitations;

        this.serverConnectionOptions =
                serverConnectionOptions == null ? null : serverConnectionOptions.withoutInactivityTimeout();
        this.peerConnectionOptions = peerConnectionOptions;
        this.transferConnectionOptions = transferConnectionOptions;
        this.incomingConnectionOptions = incomingConnectionOptions;
        this.distributedConnectionOptions = distributedConnectionOptions;
        this.userEndpointCache = userEndpointCache;
        this.searchResponseResolver = searchResponseResolver;
        this.searchResponseCache = searchResponseCache;
        this.browseResponseResolver = browseResponseResolver;
        this.directoryContentsResolver = directoryContentsResolver;
        this.userInfoResolver = userInfoResolver;
        this.enqueueDownload = enqueueDownload;
        this.placeInQueueResolver = placeInQueueResolver;
    }

    /** Returns the distributed-child setting, or {@code null}. */
    public final Boolean getAcceptDistributedChildren() {
        return acceptDistributedChildren;
    }

    /** Returns the private-room invitation setting, or {@code null}. */
    public final Boolean getAcceptPrivateRoomInvitations() {
        return acceptPrivateRoomInvitations;
    }

    /** Returns the private-message acknowledgement setting. */
    public final Boolean getAutoAcknowledgePrivateMessages() {
        return autoAcknowledgePrivateMessages;
    }

    /** Returns the privilege-notification acknowledgement setting. */
    public final Boolean getAutoAcknowledgePrivilegeNotifications() {
        return autoAcknowledgePrivilegeNotifications;
    }

    /** Returns the browse response resolver, or {@code null}. */
    public final BrowseResponseResolver getBrowseResponseResolver() {
        return browseResponseResolver;
    }

    /** Returns the search-request deduplication setting. */
    public final Boolean getDeduplicateSearchRequests() {
        return deduplicateSearchRequests;
    }

    /** Returns the directory contents resolver, or {@code null}. */
    public final DirectoryContentsResolver getDirectoryContentsResolver() {
        return directoryContentsResolver;
    }

    /** Returns the distributed child limit, or {@code null}. */
    public final Integer getDistributedChildLimit() {
        return distributedChildLimit;
    }

    /** Returns the distributed connection options, or {@code null}. */
    public final ConnectionOptions getDistributedConnectionOptions() {
        return distributedConnectionOptions;
    }

    /** Returns the distributed-network setting, or {@code null}. */
    public final Boolean getEnableDistributedNetwork() {
        return enableDistributedNetwork;
    }

    /** Returns the listener setting, or {@code null}. */
    public final Boolean getEnableListener() {
        return enableListener;
    }

    /** Returns the enqueue-download callback, or {@code null}. */
    public final EnqueueDownloadCallback getEnqueueDownload() {
        return enqueueDownload;
    }

    /** Returns the incoming connection options, or {@code null}. */
    public final ConnectionOptions getIncomingConnectionOptions() {
        return incomingConnectionOptions;
    }

    /** Returns the listener address, or {@code null}. */
    public final InetAddress getListenIpAddress() {
        return listenIpAddress;
    }

    /** Returns the listener port, or {@code null}. */
    public final Integer getListenPort() {
        return listenPort;
    }

    /** Returns the maximum download speed, or {@code null}. */
    public final Integer getMaximumDownloadSpeed() {
        return maximumDownloadSpeed;
    }

    /** Returns the maximum upload speed, or {@code null}. */
    public final Integer getMaximumUploadSpeed() {
        return maximumUploadSpeed;
    }

    /** Returns the peer connection options, or {@code null}. */
    public final ConnectionOptions getPeerConnectionOptions() {
        return peerConnectionOptions;
    }

    /** Returns the place-in-queue resolver, or {@code null}. */
    public final PlaceInQueueResolver getPlaceInQueueResolver() {
        return placeInQueueResolver;
    }

    /** Returns the search response cache, or {@code null}. */
    public final SearchResponseCache getSearchResponseCache() {
        return searchResponseCache;
    }

    /** Returns the search response resolver, or {@code null}. */
    public final SearchResponseResolver getSearchResponseResolver() {
        return searchResponseResolver;
    }

    /** Returns the server connection options, or {@code null}. */
    public final ConnectionOptions getServerConnectionOptions() {
        return serverConnectionOptions;
    }

    /** Returns the transfer connection options, or {@code null}. */
    public final ConnectionOptions getTransferConnectionOptions() {
        return transferConnectionOptions;
    }

    /** Returns the user endpoint cache, or {@code null}. */
    public final UserEndpointCache getUserEndpointCache() {
        return userEndpointCache;
    }

    /** Returns the user information resolver, or {@code null}. */
    public final UserInfoResolver getUserInfoResolver() {
        return userInfoResolver;
    }
}
