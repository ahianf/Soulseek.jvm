// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import dev.slsk.internal.SearchResponseCache;
import dev.slsk.internal.diagnostics.DiagnosticLevel;
import dev.slsk.internal.user.UserEndpointCache;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

/** Options for a Soulseek client. */
public class SoulseekClientOptions {
    /** Default listener port. */
    public static final int DEFAULT_LISTEN_PORT = 30_000;
    /** Default distributed-child limit. */
    public static final int DEFAULT_DISTRIBUTED_CHILD_LIMIT = 25;
    /** Default maximum concurrent searches. */
    public static final int DEFAULT_MAXIMUM_CONCURRENT_SEARCHES = 2;
    /** Default maximum concurrent uploads. */
    public static final int DEFAULT_MAXIMUM_CONCURRENT_UPLOADS = 10;
    /** Default server/peer message timeout in milliseconds. */
    public static final int DEFAULT_MESSAGE_TIMEOUT = 5_000;

    private final boolean acceptDistributedChildren;
    private final boolean acceptPrivateRoomInvitations;
    private final boolean autoAcknowledgePrivateMessages;
    private final boolean autoAcknowledgePrivilegeNotifications;
    private final boolean deduplicateSearchRequests;
    private final int distributedChildLimit;
    private final ConnectionOptions distributedConnectionOptions;
    private final boolean enableDistributedNetwork;
    private final boolean enableListener;
    private final ConnectionOptions incomingConnectionOptions;
    private final InetAddress listenIpAddress;
    private final int listenPort;
    private final int maximumConcurrentDownloads;
    private final int maximumConcurrentSearches;
    private final int maximumConcurrentUploads;
    private final int maximumConcurrentUploadsPerUser = 1;
    private final int maximumDownloadSpeed;
    private final int maximumUploadSpeed;
    private final int messageTimeout;
    private final DiagnosticLevel minimumDiagnosticLevel;
    private final ConnectionOptions peerConnectionOptions;
    private final SearchResponseCache searchResponseCache;
    private final ConnectionOptions serverConnectionOptions;
    private final int startingToken;
    private final ConnectionOptions transferConnectionOptions;
    private final UserEndpointCache userEndpointCache;

    /** Creates options with source defaults. */
    public SoulseekClientOptions() {
        this(
                true,
                null,
                DEFAULT_LISTEN_PORT,
                true,
                true,
                DEFAULT_DISTRIBUTED_CHILD_LIMIT,
                DEFAULT_MAXIMUM_CONCURRENT_SEARCHES,
                DEFAULT_MAXIMUM_CONCURRENT_UPLOADS,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                true,
                DEFAULT_MESSAGE_TIMEOUT,
                true,
                true,
                false,
                DiagnosticLevel.INFO,
                0,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    /** Creates options through the listener switch. */
    public SoulseekClientOptions(boolean enableListener) {
        this(
                enableListener,
                null,
                DEFAULT_LISTEN_PORT,
                true,
                true,
                DEFAULT_DISTRIBUTED_CHILD_LIMIT,
                DEFAULT_MAXIMUM_CONCURRENT_SEARCHES,
                DEFAULT_MAXIMUM_CONCURRENT_UPLOADS,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                true,
                DEFAULT_MESSAGE_TIMEOUT,
                true,
                true,
                false,
                DiagnosticLevel.INFO,
                0,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    /** Creates options through the listener address. */
    public SoulseekClientOptions(boolean enableListener, InetAddress listenIpAddress) {
        this(
                enableListener,
                listenIpAddress,
                DEFAULT_LISTEN_PORT,
                true,
                true,
                DEFAULT_DISTRIBUTED_CHILD_LIMIT,
                DEFAULT_MAXIMUM_CONCURRENT_SEARCHES,
                DEFAULT_MAXIMUM_CONCURRENT_UPLOADS,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                true,
                DEFAULT_MESSAGE_TIMEOUT,
                true,
                true,
                false,
                DiagnosticLevel.INFO,
                0,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    /** Creates options through the listener port. */
    public SoulseekClientOptions(boolean enableListener, InetAddress listenIpAddress, int listenPort) {
        this(enableListener, listenIpAddress, listenPort, DEFAULT_MESSAGE_TIMEOUT);
    }

    /**
     * Creates options through the listener port and message timeout.
     *
     * @param enableListener whether to accept inbound peer connections
     * @param listenIpAddress the local listener address, or {@code null} for all addresses
     * @param listenPort the local listener port advertised to the network
     * @param messageTimeout the timeout in milliseconds for correlated server messages
     */
    public SoulseekClientOptions(
            boolean enableListener, InetAddress listenIpAddress, int listenPort, int messageTimeout) {
        this(enableListener, listenIpAddress, listenPort, messageTimeout, DiagnosticLevel.INFO);
    }

    /**
     * Creates options through listener, message-timeout, and diagnostic settings.
     *
     * @param enableListener whether to accept inbound peer connections
     * @param listenIpAddress the local listener address, or {@code null} for all addresses
     * @param listenPort the local listener port advertised to the network
     * @param messageTimeout the timeout in milliseconds for correlated server messages
     * @param minimumDiagnosticLevel the minimum diagnostic event level to emit
     */
    public SoulseekClientOptions(
            boolean enableListener,
            InetAddress listenIpAddress,
            int listenPort,
            int messageTimeout,
            DiagnosticLevel minimumDiagnosticLevel) {
        this(
                enableListener,
                listenIpAddress,
                listenPort,
                true,
                true,
                DEFAULT_DISTRIBUTED_CHILD_LIMIT,
                DEFAULT_MAXIMUM_CONCURRENT_SEARCHES,
                DEFAULT_MAXIMUM_CONCURRENT_UPLOADS,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                true,
                messageTimeout,
                true,
                true,
                false,
                minimumDiagnosticLevel,
                0,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    /** Creates complete client options. */
    public SoulseekClientOptions(
            boolean enableListener,
            InetAddress listenIpAddress,
            int listenPort,
            boolean enableDistributedNetwork,
            boolean acceptDistributedChildren,
            int distributedChildLimit,
            int maximumConcurrentSearches,
            int maximumConcurrentUploads,
            int maximumUploadSpeed,
            int maximumConcurrentDownloads,
            int maximumDownloadSpeed,
            boolean deduplicateSearchRequests,
            int messageTimeout,
            boolean autoAcknowledgePrivateMessages,
            boolean autoAcknowledgePrivilegeNotifications,
            boolean acceptPrivateRoomInvitations,
            DiagnosticLevel minimumDiagnosticLevel,
            int startingToken,
            ConnectionOptions serverConnectionOptions,
            ConnectionOptions peerConnectionOptions,
            ConnectionOptions transferConnectionOptions,
            ConnectionOptions incomingConnectionOptions,
            ConnectionOptions distributedConnectionOptions,
            UserEndpointCache userEndpointCache,
            SearchResponseCache searchResponseCache) {
        this.enableListener = enableListener;
        this.listenIpAddress = listenIpAddress == null ? wildcardAddress() : listenIpAddress;
        this.listenPort = listenPort;

        if (listenPort < 1024 || listenPort > 65_535) {
            throw new IllegalArgumentException("listenPort must be between 1024 and 65535");
        }

        this.enableDistributedNetwork = enableDistributedNetwork;
        this.acceptDistributedChildren = acceptDistributedChildren;
        this.distributedChildLimit = distributedChildLimit;

        if (distributedChildLimit < 0) {
            throw new IllegalArgumentException("distributedChildLimit must be greater than or equal to zero");
        }

        this.maximumConcurrentSearches = maximumConcurrentSearches;
        if (maximumConcurrentSearches < 1) {
            throw new IllegalArgumentException("maximumConcurrentSearches must be greater than or equal to one");
        }

        this.maximumConcurrentUploads = maximumConcurrentUploads;
        if (maximumConcurrentUploads < 1) {
            throw new IllegalArgumentException("maximumConcurrentUploads must be greater than or equal to one");
        }

        this.maximumUploadSpeed = maximumUploadSpeed;
        this.maximumConcurrentDownloads = maximumConcurrentDownloads;
        if (maximumConcurrentDownloads < 1) {
            throw new IllegalArgumentException("maximumConcurrentDownloads must be greater than or equal to one");
        }

        this.maximumDownloadSpeed = maximumDownloadSpeed;
        this.deduplicateSearchRequests = deduplicateSearchRequests;
        if (messageTimeout <= 0) {
            throw new IllegalArgumentException("messageTimeout must be greater than zero");
        }
        this.messageTimeout = messageTimeout;
        this.autoAcknowledgePrivateMessages = autoAcknowledgePrivateMessages;
        this.autoAcknowledgePrivilegeNotifications = autoAcknowledgePrivilegeNotifications;
        this.acceptPrivateRoomInvitations = acceptPrivateRoomInvitations;
        this.minimumDiagnosticLevel = Objects.requireNonNull(minimumDiagnosticLevel, "minimumDiagnosticLevel");
        this.startingToken = startingToken;

        this.serverConnectionOptions = (serverConnectionOptions == null
                        ? new ConnectionOptions()
                        : serverConnectionOptions)
                .withoutInactivityTimeout();
        this.peerConnectionOptions = peerConnectionOptions == null ? new ConnectionOptions() : peerConnectionOptions;
        this.transferConnectionOptions =
                transferConnectionOptions == null ? new ConnectionOptions() : transferConnectionOptions;
        this.incomingConnectionOptions =
                incomingConnectionOptions == null ? new ConnectionOptions() : incomingConnectionOptions;
        this.distributedConnectionOptions =
                distributedConnectionOptions == null ? new ConnectionOptions() : distributedConnectionOptions;

        // Defaulted rather than left null: a peer's address is something only
        // the server can tell us, peers search us repeatedly, and without a
        // cache every answer costs a lookup the server has already answered.
        this.userEndpointCache =
                userEndpointCache == null ? new dev.slsk.internal.user.BoundedUserEndpointCache() : userEndpointCache;
        // Defaulted rather than left null: without a cache we silently answer
        // fewer searches than we think we do, and which searches is decided by
        // whether the peer is behind NAT.
        this.searchResponseCache =
                searchResponseCache == null ? new dev.slsk.internal.BoundedSearchResponseCache() : searchResponseCache;
    }

    /** Returns a clone with the supplied patch applied. */
    public final SoulseekClientOptions with(SoulseekClientOptionsPatch patch) {
        Objects.requireNonNull(patch, "patch");

        return new SoulseekClientOptions(
                patch.getEnableListener() == null ? enableListener : patch.getEnableListener(),
                patch.getListenIpAddress() == null ? listenIpAddress : patch.getListenIpAddress(),
                patch.getListenPort() == null ? listenPort : patch.getListenPort(),
                patch.getEnableDistributedNetwork() == null
                        ? enableDistributedNetwork
                        : patch.getEnableDistributedNetwork(),
                patch.getAcceptDistributedChildren() == null
                        ? acceptDistributedChildren
                        : patch.getAcceptDistributedChildren(),
                patch.getDistributedChildLimit() == null ? distributedChildLimit : patch.getDistributedChildLimit(),
                DEFAULT_MAXIMUM_CONCURRENT_SEARCHES,
                maximumConcurrentUploads,
                patch.getMaximumUploadSpeed() == null ? maximumUploadSpeed : patch.getMaximumUploadSpeed(),
                maximumConcurrentDownloads,
                patch.getMaximumDownloadSpeed() == null ? maximumDownloadSpeed : patch.getMaximumDownloadSpeed(),
                patch.getDeduplicateSearchRequests() == null
                        ? deduplicateSearchRequests
                        : patch.getDeduplicateSearchRequests(),
                messageTimeout,
                patch.getAutoAcknowledgePrivateMessages() == null
                        ? autoAcknowledgePrivateMessages
                        : patch.getAutoAcknowledgePrivateMessages(),
                patch.getAutoAcknowledgePrivilegeNotifications() == null
                        ? autoAcknowledgePrivilegeNotifications
                        : patch.getAutoAcknowledgePrivilegeNotifications(),
                patch.getAcceptPrivateRoomInvitations() == null
                        ? acceptPrivateRoomInvitations
                        : patch.getAcceptPrivateRoomInvitations(),
                minimumDiagnosticLevel,
                startingToken,
                patch.getServerConnectionOptions() == null
                        ? serverConnectionOptions
                        : patch.getServerConnectionOptions(),
                patch.getPeerConnectionOptions() == null ? peerConnectionOptions : patch.getPeerConnectionOptions(),
                patch.getTransferConnectionOptions() == null
                        ? transferConnectionOptions
                        : patch.getTransferConnectionOptions(),
                patch.getIncomingConnectionOptions() == null
                        ? incomingConnectionOptions
                        : patch.getIncomingConnectionOptions(),
                patch.getDistributedConnectionOptions() == null
                        ? distributedConnectionOptions
                        : patch.getDistributedConnectionOptions(),
                patch.getUserEndpointCache() == null ? userEndpointCache : patch.getUserEndpointCache(),
                patch.getSearchResponseCache() == null ? searchResponseCache : patch.getSearchResponseCache());
    }

    /** Returns whether distributed child connections are accepted. */
    public final boolean isAcceptDistributedChildren() {
        return acceptDistributedChildren;
    }

    /** Returns whether private-room invitations are accepted. */
    public final boolean isAcceptPrivateRoomInvitations() {
        return acceptPrivateRoomInvitations;
    }

    /** Returns whether private messages are acknowledged automatically. */
    public final boolean isAutoAcknowledgePrivateMessages() {
        return autoAcknowledgePrivateMessages;
    }

    /** Returns whether privilege notifications are acknowledged automatically. */
    public final boolean isAutoAcknowledgePrivilegeNotifications() {
        return autoAcknowledgePrivilegeNotifications;
    }

    /** Returns whether duplicate search requests are discarded. */
    public final boolean isDeduplicateSearchRequests() {
        return deduplicateSearchRequests;
    }

    /** Returns the distributed child limit. */
    public final int getDistributedChildLimit() {
        return distributedChildLimit;
    }

    /** Returns the distributed connection options. */
    public final ConnectionOptions getDistributedConnectionOptions() {
        return distributedConnectionOptions;
    }

    /** Returns whether the distributed network is enabled. */
    public final boolean isEnableDistributedNetwork() {
        return enableDistributedNetwork;
    }

    /** Returns whether the listener is enabled. */
    public final boolean isEnableListener() {
        return enableListener;
    }

    /** Returns the incoming connection options. */
    public final ConnectionOptions getIncomingConnectionOptions() {
        return incomingConnectionOptions;
    }

    /** Returns the listener IP address. */
    public final InetAddress getListenIpAddress() {
        return listenIpAddress;
    }

    /** Returns the listener port. */
    public final int getListenPort() {
        return listenPort;
    }

    /** Returns the maximum concurrent downloads. */
    public final int getMaximumConcurrentDownloads() {
        return maximumConcurrentDownloads;
    }

    /** Returns the maximum concurrent searches. */
    public final int getMaximumConcurrentSearches() {
        return maximumConcurrentSearches;
    }

    /** Returns the maximum concurrent uploads. */
    public final int getMaximumConcurrentUploads() {
        return maximumConcurrentUploads;
    }

    /** Returns the per-user upload-slot limit. */
    public final int getMaximumConcurrentUploadsPerUser() {
        return maximumConcurrentUploadsPerUser;
    }

    /** Returns the maximum total download speed in KiB/s. */
    public final int getMaximumDownloadSpeed() {
        return maximumDownloadSpeed;
    }

    /** Returns the maximum total upload speed in KiB/s. */
    public final int getMaximumUploadSpeed() {
        return maximumUploadSpeed;
    }

    /** Returns the server/peer message timeout in milliseconds. */
    public final int getMessageTimeout() {
        return messageTimeout;
    }

    /** Returns the minimum diagnostic level. */
    public final DiagnosticLevel getMinimumDiagnosticLevel() {
        return minimumDiagnosticLevel;
    }

    /** Returns the peer connection options. */
    public final ConnectionOptions getPeerConnectionOptions() {
        return peerConnectionOptions;
    }

    /** Returns the search response cache, or {@code null}. */
    public final SearchResponseCache getSearchResponseCache() {
        return searchResponseCache;
    }

    /** Returns the server connection options. */
    public final ConnectionOptions getServerConnectionOptions() {
        return serverConnectionOptions;
    }

    /** Returns the starting token. */
    public final int getStartingToken() {
        return startingToken;
    }

    /** Returns the transfer connection options. */
    public final ConnectionOptions getTransferConnectionOptions() {
        return transferConnectionOptions;
    }

    /** Returns the user endpoint cache, or {@code null}. */
    public final UserEndpointCache getUserEndpointCache() {
        return userEndpointCache;
    }

    private static InetAddress wildcardAddress() {
        try {
            return InetAddress.getByAddress(new byte[4]);
        } catch (UnknownHostException impossible) {
            throw new AssertionError(impossible);
        }
    }
}
