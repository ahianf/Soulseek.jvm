// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import dev.slsk.internal.search.SearchResponseCache;
import dev.slsk.internal.user.UserEndpointCache;
import java.net.InetAddress;
import java.util.Objects;
import java.util.Optional;

/** A field-named patch for {@link SoulseekClientOptions}. */
public record SoulseekClientOptionsPatch(
        Optional<Boolean> enableListener,
        Optional<InetAddress> listenIpAddress,
        Optional<Integer> listenPort,
        Optional<Boolean> enableDistributedNetwork,
        Optional<Boolean> acceptDistributedChildren,
        Optional<Integer> distributedChildLimit,
        Optional<Integer> maximumUploadSpeed,
        Optional<Integer> maximumDownloadSpeed,
        Optional<Boolean> deduplicateSearchRequests,
        Optional<Boolean> autoAcknowledgePrivateMessages,
        Optional<Boolean> autoAcknowledgePrivilegeNotifications,
        Optional<Boolean> acceptPrivateRoomInvitations,
        Optional<ConnectionOptions> serverConnectionOptions,
        Optional<ConnectionOptions> peerConnectionOptions,
        Optional<ConnectionOptions> transferConnectionOptions,
        Optional<ConnectionOptions> incomingConnectionOptions,
        Optional<ConnectionOptions> distributedConnectionOptions,
        Optional<UserEndpointCache> userEndpointCache,
        Optional<SearchResponseCache> searchResponseCache) {
    /** Validates present values and normalizes server options. */
    public SoulseekClientOptionsPatch {
        enableListener = required(enableListener, "enableListener");
        listenIpAddress = required(listenIpAddress, "listenIpAddress");
        listenPort = required(listenPort, "listenPort");
        enableDistributedNetwork = required(enableDistributedNetwork, "enableDistributedNetwork");
        acceptDistributedChildren = required(acceptDistributedChildren, "acceptDistributedChildren");
        distributedChildLimit = required(distributedChildLimit, "distributedChildLimit");
        maximumUploadSpeed = required(maximumUploadSpeed, "maximumUploadSpeed");
        maximumDownloadSpeed = required(maximumDownloadSpeed, "maximumDownloadSpeed");
        deduplicateSearchRequests = required(deduplicateSearchRequests, "deduplicateSearchRequests");
        autoAcknowledgePrivateMessages = required(autoAcknowledgePrivateMessages, "autoAcknowledgePrivateMessages");
        autoAcknowledgePrivilegeNotifications =
                required(autoAcknowledgePrivilegeNotifications, "autoAcknowledgePrivilegeNotifications");
        acceptPrivateRoomInvitations = required(acceptPrivateRoomInvitations, "acceptPrivateRoomInvitations");
        serverConnectionOptions = required(serverConnectionOptions, "serverConnectionOptions")
                .map(ConnectionOptions::withoutInactivityTimeout);
        peerConnectionOptions = required(peerConnectionOptions, "peerConnectionOptions");
        transferConnectionOptions = required(transferConnectionOptions, "transferConnectionOptions");
        incomingConnectionOptions = required(incomingConnectionOptions, "incomingConnectionOptions");
        distributedConnectionOptions = required(distributedConnectionOptions, "distributedConnectionOptions");
        userEndpointCache = required(userEndpointCache, "userEndpointCache");
        searchResponseCache = required(searchResponseCache, "searchResponseCache");

        listenPort.ifPresent(value -> {
            if (value < 1024 || value > 65_535) {
                throw new IllegalArgumentException("listenPort must be between 1024 and 65535");
            }
        });
        distributedChildLimit.ifPresent(value -> {
            if (value < 0) {
                throw new IllegalArgumentException("distributedChildLimit must be greater than or equal to zero");
            }
        });
    }

    /** Creates an empty patch. */
    public SoulseekClientOptionsPatch() {
        this(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    /** Starts a field-named patch builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder for client-option patches. */
    public static final class Builder {
        private Optional<Boolean> acceptDistributedChildren = Optional.empty();
        private Optional<Boolean> acceptPrivateRoomInvitations = Optional.empty();
        private Optional<Boolean> autoAcknowledgePrivateMessages = Optional.empty();
        private Optional<Boolean> autoAcknowledgePrivilegeNotifications = Optional.empty();
        private Optional<Boolean> deduplicateSearchRequests = Optional.empty();
        private Optional<Integer> distributedChildLimit = Optional.empty();
        private Optional<ConnectionOptions> distributedConnectionOptions = Optional.empty();
        private Optional<Boolean> enableDistributedNetwork = Optional.empty();
        private Optional<Boolean> enableListener = Optional.empty();
        private Optional<ConnectionOptions> incomingConnectionOptions = Optional.empty();
        private Optional<InetAddress> listenIpAddress = Optional.empty();
        private Optional<Integer> listenPort = Optional.empty();
        private Optional<Integer> maximumDownloadSpeed = Optional.empty();
        private Optional<Integer> maximumUploadSpeed = Optional.empty();
        private Optional<ConnectionOptions> peerConnectionOptions = Optional.empty();
        private Optional<SearchResponseCache> searchResponseCache = Optional.empty();
        private Optional<ConnectionOptions> serverConnectionOptions = Optional.empty();
        private Optional<ConnectionOptions> transferConnectionOptions = Optional.empty();
        private Optional<UserEndpointCache> userEndpointCache = Optional.empty();

        private Builder() {}

        public Builder enableListener(boolean value) {
            enableListener = Optional.of(value);
            return this;
        }

        public Builder listenIpAddress(InetAddress value) {
            listenIpAddress = Optional.of(value);
            return this;
        }

        public Builder listenPort(int value) {
            listenPort = Optional.of(value);
            return this;
        }

        public Builder enableDistributedNetwork(boolean value) {
            enableDistributedNetwork = Optional.of(value);
            return this;
        }

        public Builder acceptDistributedChildren(boolean value) {
            acceptDistributedChildren = Optional.of(value);
            return this;
        }

        public Builder distributedChildLimit(int value) {
            distributedChildLimit = Optional.of(value);
            return this;
        }

        public Builder maximumUploadSpeed(int value) {
            maximumUploadSpeed = Optional.of(value);
            return this;
        }

        public Builder maximumDownloadSpeed(int value) {
            maximumDownloadSpeed = Optional.of(value);
            return this;
        }

        public Builder deduplicateSearchRequests(boolean value) {
            deduplicateSearchRequests = Optional.of(value);
            return this;
        }

        public Builder autoAcknowledgePrivateMessages(boolean value) {
            autoAcknowledgePrivateMessages = Optional.of(value);
            return this;
        }

        public Builder autoAcknowledgePrivilegeNotifications(boolean value) {
            autoAcknowledgePrivilegeNotifications = Optional.of(value);
            return this;
        }

        public Builder acceptPrivateRoomInvitations(boolean value) {
            acceptPrivateRoomInvitations = Optional.of(value);
            return this;
        }

        public Builder serverConnectionOptions(ConnectionOptions value) {
            serverConnectionOptions = Optional.of(value);
            return this;
        }

        public Builder peerConnectionOptions(ConnectionOptions value) {
            peerConnectionOptions = Optional.of(value);
            return this;
        }

        public Builder transferConnectionOptions(ConnectionOptions value) {
            transferConnectionOptions = Optional.of(value);
            return this;
        }

        public Builder incomingConnectionOptions(ConnectionOptions value) {
            incomingConnectionOptions = Optional.of(value);
            return this;
        }

        public Builder distributedConnectionOptions(ConnectionOptions value) {
            distributedConnectionOptions = Optional.of(value);
            return this;
        }

        public Builder userEndpointCache(UserEndpointCache value) {
            userEndpointCache = Optional.of(value);
            return this;
        }

        public Builder searchResponseCache(SearchResponseCache value) {
            searchResponseCache = Optional.of(value);
            return this;
        }

        public SoulseekClientOptionsPatch build() {
            return new SoulseekClientOptionsPatch(
                    enableListener,
                    listenIpAddress,
                    listenPort,
                    enableDistributedNetwork,
                    acceptDistributedChildren,
                    distributedChildLimit,
                    maximumUploadSpeed,
                    maximumDownloadSpeed,
                    deduplicateSearchRequests,
                    autoAcknowledgePrivateMessages,
                    autoAcknowledgePrivilegeNotifications,
                    acceptPrivateRoomInvitations,
                    serverConnectionOptions,
                    peerConnectionOptions,
                    transferConnectionOptions,
                    incomingConnectionOptions,
                    distributedConnectionOptions,
                    userEndpointCache,
                    searchResponseCache);
        }
    }

    private static <T> Optional<T> required(Optional<T> value, String name) {
        return Objects.requireNonNull(value, name);
    }
}
