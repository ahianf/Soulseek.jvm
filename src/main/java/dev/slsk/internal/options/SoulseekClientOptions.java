// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import dev.slsk.internal.diagnostics.DiagnosticSeverity;
import dev.slsk.internal.search.BoundedSearchResponseCache;
import dev.slsk.internal.search.SearchResponseCache;
import dev.slsk.internal.user.BoundedUserEndpointCache;
import dev.slsk.internal.user.UserEndpointCache;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Objects;

/** Options for a Soulseek client. */
public record SoulseekClientOptions(
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
        Duration messageTimeout,
        boolean autoAcknowledgePrivateMessages,
        boolean autoAcknowledgePrivilegeNotifications,
        boolean acceptPrivateRoomInvitations,
        DiagnosticSeverity minimumDiagnosticLevel,
        int startingToken,
        ConnectionOptions serverConnectionOptions,
        ConnectionOptions peerConnectionOptions,
        ConnectionOptions transferConnectionOptions,
        ConnectionOptions incomingConnectionOptions,
        ConnectionOptions distributedConnectionOptions,
        UserEndpointCache userEndpointCache,
        SearchResponseCache searchResponseCache) {
    /** Default listener port. */
    public static final int DEFAULT_LISTEN_PORT = 30_000;
    /** Default distributed-child limit. */
    public static final int DEFAULT_DISTRIBUTED_CHILD_LIMIT = 25;
    /** Default maximum concurrent searches. */
    public static final int DEFAULT_MAXIMUM_CONCURRENT_SEARCHES = 2;
    /** Default maximum concurrent uploads. */
    public static final int DEFAULT_MAXIMUM_CONCURRENT_UPLOADS = 10;
    /** Default server/peer message timeout. */
    public static final Duration DEFAULT_MESSAGE_TIMEOUT = Duration.ofSeconds(5);

    /** Normalizes optional collaborators and validates scalar limits. */
    public SoulseekClientOptions {
        listenIpAddress = listenIpAddress == null ? wildcardAddress() : listenIpAddress;
        if (listenPort < 1024 || listenPort > 65_535) {
            throw new IllegalArgumentException("listenPort must be between 1024 and 65535");
        }
        if (distributedChildLimit < 0) {
            throw new IllegalArgumentException("distributedChildLimit must be greater than or equal to zero");
        }
        if (maximumConcurrentSearches < 1) {
            throw new IllegalArgumentException("maximumConcurrentSearches must be greater than or equal to one");
        }
        if (maximumConcurrentUploads < 1) {
            throw new IllegalArgumentException("maximumConcurrentUploads must be greater than or equal to one");
        }
        if (maximumConcurrentDownloads < 1) {
            throw new IllegalArgumentException("maximumConcurrentDownloads must be greater than or equal to one");
        }
        messageTimeout = Objects.requireNonNull(messageTimeout, "messageTimeout");
        if (!messageTimeout.isPositive()) {
            throw new IllegalArgumentException("messageTimeout must be greater than zero");
        }
        minimumDiagnosticLevel = Objects.requireNonNull(minimumDiagnosticLevel, "minimumDiagnosticLevel");

        serverConnectionOptions = (serverConnectionOptions == null ? new ConnectionOptions() : serverConnectionOptions)
                .withoutInactivityTimeout();
        peerConnectionOptions = peerConnectionOptions == null ? new ConnectionOptions() : peerConnectionOptions;
        transferConnectionOptions =
                transferConnectionOptions == null ? new ConnectionOptions() : transferConnectionOptions;
        incomingConnectionOptions =
                incomingConnectionOptions == null ? new ConnectionOptions() : incomingConnectionOptions;
        distributedConnectionOptions =
                distributedConnectionOptions == null ? new ConnectionOptions() : distributedConnectionOptions;

        // Defaulted rather than left null: a peer's address is something only
        // the server can tell us, peers search us repeatedly, and without a
        // cache every answer costs a lookup the server has already answered.
        userEndpointCache = userEndpointCache == null ? new BoundedUserEndpointCache() : userEndpointCache;
        // Defaulted rather than left null: without a cache we silently answer
        // fewer searches than we think we do, and which searches is decided by
        // whether the peer is behind NAT.
        searchResponseCache = searchResponseCache == null ? new BoundedSearchResponseCache() : searchResponseCache;
    }

    /** Creates options with defaults. */
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
                DiagnosticSeverity.INFO,
                0,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    /** Starts a field-named client-options builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Starts a builder initialized from existing options. */
    public static Builder builder(SoulseekClientOptions source) {
        return new Builder(source);
    }

    /** Returns a copy with the supplied patch applied. */
    public SoulseekClientOptions with(SoulseekClientOptionsPatch patch) {
        Objects.requireNonNull(patch, "patch");
        Builder builder = builder(this);
        patch.enableListener().ifPresent(builder::enableListener);
        patch.listenIpAddress().ifPresent(builder::listenIpAddress);
        patch.listenPort().ifPresent(builder::listenPort);
        patch.enableDistributedNetwork().ifPresent(builder::enableDistributedNetwork);
        patch.acceptDistributedChildren().ifPresent(builder::acceptDistributedChildren);
        patch.distributedChildLimit().ifPresent(builder::distributedChildLimit);
        patch.maximumUploadSpeed().ifPresent(builder::maximumUploadSpeed);
        patch.maximumDownloadSpeed().ifPresent(builder::maximumDownloadSpeed);
        patch.deduplicateSearchRequests().ifPresent(builder::deduplicateSearchRequests);
        patch.autoAcknowledgePrivateMessages().ifPresent(builder::autoAcknowledgePrivateMessages);
        patch.autoAcknowledgePrivilegeNotifications().ifPresent(builder::autoAcknowledgePrivilegeNotifications);
        patch.acceptPrivateRoomInvitations().ifPresent(builder::acceptPrivateRoomInvitations);
        patch.serverConnectionOptions().ifPresent(builder::serverConnectionOptions);
        patch.peerConnectionOptions().ifPresent(builder::peerConnectionOptions);
        patch.transferConnectionOptions().ifPresent(builder::transferConnectionOptions);
        patch.incomingConnectionOptions().ifPresent(builder::incomingConnectionOptions);
        patch.distributedConnectionOptions().ifPresent(builder::distributedConnectionOptions);
        patch.userEndpointCache().ifPresent(builder::userEndpointCache);
        patch.searchResponseCache().ifPresent(builder::searchResponseCache);
        return builder.build();
    }

    /** Returns the fixed per-user upload-slot limit. */
    public int maximumConcurrentUploadsPerUser() {
        return 1;
    }

    /** Builder for client options. */
    public static final class Builder {
        private boolean acceptDistributedChildren = true;
        private boolean acceptPrivateRoomInvitations;
        private boolean autoAcknowledgePrivateMessages = true;
        private boolean autoAcknowledgePrivilegeNotifications = true;
        private boolean deduplicateSearchRequests = true;
        private int distributedChildLimit = DEFAULT_DISTRIBUTED_CHILD_LIMIT;
        private ConnectionOptions distributedConnectionOptions;
        private boolean enableDistributedNetwork = true;
        private boolean enableListener = true;
        private ConnectionOptions incomingConnectionOptions;
        private InetAddress listenIpAddress;
        private int listenPort = DEFAULT_LISTEN_PORT;
        private int maximumConcurrentDownloads = Integer.MAX_VALUE;
        private int maximumConcurrentSearches = DEFAULT_MAXIMUM_CONCURRENT_SEARCHES;
        private int maximumConcurrentUploads = DEFAULT_MAXIMUM_CONCURRENT_UPLOADS;
        private int maximumDownloadSpeed = Integer.MAX_VALUE;
        private int maximumUploadSpeed = Integer.MAX_VALUE;
        private Duration messageTimeout = DEFAULT_MESSAGE_TIMEOUT;
        private DiagnosticSeverity minimumDiagnosticLevel = DiagnosticSeverity.INFO;
        private ConnectionOptions peerConnectionOptions;
        private SearchResponseCache searchResponseCache;
        private ConnectionOptions serverConnectionOptions;
        private int startingToken;
        private ConnectionOptions transferConnectionOptions;
        private UserEndpointCache userEndpointCache;

        private Builder() {}

        private Builder(SoulseekClientOptions source) {
            Objects.requireNonNull(source, "source");
            enableListener = source.enableListener;
            listenIpAddress = source.listenIpAddress;
            listenPort = source.listenPort;
            enableDistributedNetwork = source.enableDistributedNetwork;
            acceptDistributedChildren = source.acceptDistributedChildren;
            distributedChildLimit = source.distributedChildLimit;
            maximumConcurrentSearches = source.maximumConcurrentSearches;
            maximumConcurrentUploads = source.maximumConcurrentUploads;
            maximumUploadSpeed = source.maximumUploadSpeed;
            maximumConcurrentDownloads = source.maximumConcurrentDownloads;
            maximumDownloadSpeed = source.maximumDownloadSpeed;
            deduplicateSearchRequests = source.deduplicateSearchRequests;
            messageTimeout = source.messageTimeout;
            autoAcknowledgePrivateMessages = source.autoAcknowledgePrivateMessages;
            autoAcknowledgePrivilegeNotifications = source.autoAcknowledgePrivilegeNotifications;
            acceptPrivateRoomInvitations = source.acceptPrivateRoomInvitations;
            minimumDiagnosticLevel = source.minimumDiagnosticLevel;
            startingToken = source.startingToken;
            serverConnectionOptions = source.serverConnectionOptions;
            peerConnectionOptions = source.peerConnectionOptions;
            transferConnectionOptions = source.transferConnectionOptions;
            incomingConnectionOptions = source.incomingConnectionOptions;
            distributedConnectionOptions = source.distributedConnectionOptions;
            userEndpointCache = source.userEndpointCache;
            searchResponseCache = source.searchResponseCache;
        }

        public Builder enableListener(boolean value) {
            enableListener = value;
            return this;
        }

        public Builder listenIpAddress(InetAddress value) {
            listenIpAddress = value;
            return this;
        }

        public Builder listenPort(int value) {
            listenPort = value;
            return this;
        }

        public Builder enableDistributedNetwork(boolean value) {
            enableDistributedNetwork = value;
            return this;
        }

        public Builder acceptDistributedChildren(boolean value) {
            acceptDistributedChildren = value;
            return this;
        }

        public Builder distributedChildLimit(int value) {
            distributedChildLimit = value;
            return this;
        }

        public Builder maximumConcurrentSearches(int value) {
            maximumConcurrentSearches = value;
            return this;
        }

        public Builder maximumConcurrentUploads(int value) {
            maximumConcurrentUploads = value;
            return this;
        }

        public Builder maximumUploadSpeed(int value) {
            maximumUploadSpeed = value;
            return this;
        }

        public Builder maximumConcurrentDownloads(int value) {
            maximumConcurrentDownloads = value;
            return this;
        }

        public Builder maximumDownloadSpeed(int value) {
            maximumDownloadSpeed = value;
            return this;
        }

        public Builder deduplicateSearchRequests(boolean value) {
            deduplicateSearchRequests = value;
            return this;
        }

        public Builder messageTimeout(Duration value) {
            messageTimeout = value;
            return this;
        }

        public Builder autoAcknowledgePrivateMessages(boolean value) {
            autoAcknowledgePrivateMessages = value;
            return this;
        }

        public Builder autoAcknowledgePrivilegeNotifications(boolean value) {
            autoAcknowledgePrivilegeNotifications = value;
            return this;
        }

        public Builder acceptPrivateRoomInvitations(boolean value) {
            acceptPrivateRoomInvitations = value;
            return this;
        }

        public Builder minimumDiagnosticLevel(DiagnosticSeverity value) {
            minimumDiagnosticLevel = value;
            return this;
        }

        public Builder startingToken(int value) {
            startingToken = value;
            return this;
        }

        public Builder serverConnectionOptions(ConnectionOptions value) {
            serverConnectionOptions = value;
            return this;
        }

        public Builder peerConnectionOptions(ConnectionOptions value) {
            peerConnectionOptions = value;
            return this;
        }

        public Builder transferConnectionOptions(ConnectionOptions value) {
            transferConnectionOptions = value;
            return this;
        }

        public Builder incomingConnectionOptions(ConnectionOptions value) {
            incomingConnectionOptions = value;
            return this;
        }

        public Builder distributedConnectionOptions(ConnectionOptions value) {
            distributedConnectionOptions = value;
            return this;
        }

        public Builder userEndpointCache(UserEndpointCache value) {
            userEndpointCache = value;
            return this;
        }

        public Builder searchResponseCache(SearchResponseCache value) {
            searchResponseCache = value;
            return this;
        }

        public SoulseekClientOptions build() {
            return new SoulseekClientOptions(
                    enableListener,
                    listenIpAddress,
                    listenPort,
                    enableDistributedNetwork,
                    acceptDistributedChildren,
                    distributedChildLimit,
                    maximumConcurrentSearches,
                    maximumConcurrentUploads,
                    maximumUploadSpeed,
                    maximumConcurrentDownloads,
                    maximumDownloadSpeed,
                    deduplicateSearchRequests,
                    messageTimeout,
                    autoAcknowledgePrivateMessages,
                    autoAcknowledgePrivilegeNotifications,
                    acceptPrivateRoomInvitations,
                    minimumDiagnosticLevel,
                    startingToken,
                    serverConnectionOptions,
                    peerConnectionOptions,
                    transferConnectionOptions,
                    incomingConnectionOptions,
                    distributedConnectionOptions,
                    userEndpointCache,
                    searchResponseCache);
        }
    }

    private static InetAddress wildcardAddress() {
        try {
            return InetAddress.getByAddress(new byte[4]);
        } catch (UnknownHostException impossible) {
            throw new AssertionError(impossible);
        }
    }
}
