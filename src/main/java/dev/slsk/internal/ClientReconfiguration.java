// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import static dev.slsk.internal.ClientSupport.acquirePermit;
import static dev.slsk.internal.ClientSupport.failureMessage;
import static dev.slsk.internal.ClientSupport.unwrap;

import dev.slsk.CancellationSignal;
import dev.slsk.exceptions.SoulseekClientException;
import dev.slsk.internal.options.SoulseekClientOptionsPatch;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

/**
 * Applies an options patch to a running client: swapping the listener, resizing
 * the rate-limit buckets, and deciding whether the change needs a reconnect.
 *
 * <p>Unlike the other components this one takes the client directly rather than
 * going through {@link ClientContext}. Reconfiguration by its nature reaches
 * into almost everything the client holds; routing that through the seam would
 * have meant widening it with a dozen accessors that exist for one caller,
 * which is the opposite of what the seam is for.
 */
final class ClientReconfiguration {

    private final DefaultSoulseekClient client;

    ClientReconfiguration(DefaultSoulseekClient client) {
        this.client = java.util.Objects.requireNonNull(client, "client");
    }

    CompletableFuture<Boolean> performReconfigureOptionsAsync(
            SoulseekClientOptionsPatch patch, CancellationSignal cancellationSignal) {
        boolean connected = client.isConnectedAndLoggedIn();
        boolean enableDistributedNetworkChanged = patch.getEnableDistributedNetwork() != null
                && patch.getEnableDistributedNetwork() != client.options.isEnableDistributedNetwork();
        boolean acceptDistributedChildrenChanged = patch.getAcceptDistributedChildren() != null
                && patch.getAcceptDistributedChildren() != client.options.isAcceptDistributedChildren();
        boolean distributedConnectionOptionsChanged = patch.getDistributedConnectionOptions() != null
                && patch.getDistributedConnectionOptions() != client.options.getDistributedConnectionOptions();
        boolean distributedNetworkWasDisabled = enableDistributedNetworkChanged && !patch.getEnableDistributedNetwork();
        boolean distributedChildrenWereDisabled =
                acceptDistributedChildrenChanged && !patch.getAcceptDistributedChildren();
        boolean reconnectRequired = connected
                && (distributedNetworkWasDisabled
                        || distributedChildrenWereDisabled
                        || distributedConnectionOptionsChanged);
        boolean serverConnectionOptionsChanged = patch.getServerConnectionOptions() != null
                && patch.getServerConnectionOptions() != client.options.getServerConnectionOptions();
        if (connected && serverConnectionOptionsChanged) {
            reconnectRequired = true;
        }

        boolean enableListenerChanged =
                patch.getEnableListener() != null && patch.getEnableListener() != client.options.isEnableListener();
        boolean listenAddressChanged = patch.getListenIpAddress() != null
                && !patch.getListenIpAddress().equals(client.options.getListenIpAddress());
        boolean listenPortChanged =
                patch.getListenPort() != null && patch.getListenPort() != client.options.getListenPort();
        boolean incomingConnectionOptionsChanged = patch.getIncomingConnectionOptions() != null
                && patch.getIncomingConnectionOptions() != client.options.getIncomingConnectionOptions();

        if (enableListenerChanged || listenAddressChanged || listenPortChanged || incomingConnectionOptionsChanged) {
            boolean wasListening = client.listener != null && client.listener.isListening();
            if (client.listener != null) {
                client.listener.stop();
            }
            client.listener = null;
            client.options = client.options.with(listenerPatch(patch));
            if (wasListening && client.options.isEnableListener()) {
                client.listener = client.clientListenerFactory.create(
                        client.options.getListenIpAddress(),
                        client.options.getListenPort(),
                        client.options.getIncomingConnectionOptions());
                client.listener.addAcceptedListener(client.listenerHandler::handleConnection);
                client.listener.start();
            }
        }

        boolean maximumUploadSpeedChanged = patch.getMaximumUploadSpeed() != null
                && patch.getMaximumUploadSpeed() != client.options.getMaximumUploadSpeed();
        boolean maximumDownloadSpeedChanged = patch.getMaximumDownloadSpeed() != null
                && patch.getMaximumDownloadSpeed() != client.options.getMaximumDownloadSpeed();
        client.options = client.options.with(patch);

        if (maximumUploadSpeedChanged) {
            client.uploadTokenBucket.setCapacity((client.options.getMaximumUploadSpeed() * 1024L) / 10);
        }
        if (maximumDownloadSpeedChanged) {
            client.downloadTokenBucket.setCapacity((client.options.getMaximumDownloadSpeed() * 1024L) / 10);
        }

        client.diagnostic.info("Options reconfigured successfully");
        if (!client.isConnectedAndLoggedIn()) {
            return CompletableFuture.completedFuture(false);
        }
        client.diagnostic.debug("Updating server with latest configuration");
        boolean requiresReconnect = reconnectRequired;
        return client.sendConfigurationMessagesAsync(cancellationSignal).thenApply(ignored -> {
            if (requiresReconnect) {
                client.diagnostic.warning("Server reconnect required for client.options " + "to fully take effect");
            }
            return requiresReconnect;
        });
    }

    CompletableFuture<Boolean> reconfigureOptionsInternalAsync(
            SoulseekClientOptionsPatch patch, CancellationSignal cancellationSignal) {
        CompletableFuture<Boolean> serialized = acquirePermit(client.stateSemaphore, cancellationSignal)
                .thenCompose(ignored -> {
                    CompletableFuture<Boolean> operation;
                    try {
                        operation = performReconfigureOptionsAsync(patch, cancellationSignal);
                    } catch (Throwable failure) {
                        operation = CompletableFuture.failedFuture(failure);
                    }
                    return operation.whenComplete((result, failure) -> client.stateSemaphore.release());
                });
        return serialized.handle((result, failure) -> {
            if (failure == null) {
                return result;
            }
            Throwable cause = unwrap(failure);
            if (cause instanceof CancellationException || cause instanceof TimeoutException) {
                throw new CompletionException(cause);
            }
            throw new CompletionException(new SoulseekClientException(
                    "Failed to reconfigure client.options: "
                            + failureMessage(cause)
                            + ".  Any successful reconfiguration has not "
                            + "been rolled back; retry with the same patch "
                            + "until successful or consider this as a "
                            + "fatal Exception",
                    cause));
        });
    }

    static SoulseekClientOptionsPatch listenerPatch(SoulseekClientOptionsPatch patch) {
        return new SoulseekClientOptionsPatch(
                patch.getEnableListener(),
                patch.getListenIpAddress(),
                patch.getListenPort(),
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
                patch.getIncomingConnectionOptions(),
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
}
