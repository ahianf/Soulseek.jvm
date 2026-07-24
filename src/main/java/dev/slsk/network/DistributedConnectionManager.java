// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network;

import dev.slsk.CancellationToken;
import dev.slsk.DistributedNetworkInfo;
import dev.slsk.diagnostics.DiagnosticSource;
import dev.slsk.eventargs.DistributedChildEventArgs;
import dev.slsk.eventargs.DistributedParentEventArgs;
import dev.slsk.messaging.messages.ConnectToPeerResponse;
import dev.slsk.network.tcp.Connection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Manages distributed-network parent and child connections. */
public interface DistributedConnectionManager extends AutoCloseable, DiagnosticSource {
    void addChildAddedListener(DistributedManagerEventListener<DistributedChildEventArgs> listener);

    void removeChildAddedListener(DistributedManagerEventListener<DistributedChildEventArgs> listener);

    void addChildDisconnectedListener(DistributedManagerEventListener<DistributedChildEventArgs> listener);

    void removeChildDisconnectedListener(DistributedManagerEventListener<DistributedChildEventArgs> listener);

    void addDemotedFromBranchRootListener(DistributedManagerEventListener<Void> listener);

    void removeDemotedFromBranchRootListener(DistributedManagerEventListener<Void> listener);

    void addParentAdoptedListener(DistributedManagerEventListener<DistributedParentEventArgs> listener);

    void removeParentAdoptedListener(DistributedManagerEventListener<DistributedParentEventArgs> listener);

    void addParentDisconnectedListener(DistributedManagerEventListener<DistributedParentEventArgs> listener);

    void removeParentDisconnectedListener(DistributedManagerEventListener<DistributedParentEventArgs> listener);

    void addPromotedToBranchRootListener(DistributedManagerEventListener<Void> listener);

    void removePromotedToBranchRootListener(DistributedManagerEventListener<Void> listener);

    void addStateChangedListener(DistributedManagerEventListener<DistributedNetworkInfo> listener);

    void removeStateChangedListener(DistributedManagerEventListener<DistributedNetworkInfo> listener);

    Double getAverageBroadcastLatency();

    int getBranchLevel();

    String getBranchRoot();

    boolean canAcceptChildren();

    int getChildLimit();

    List<PeerEndpoint> getChildren();

    boolean hasParent();

    boolean isBranchRoot();

    PeerEndpoint getParent();

    Map<Integer, String> getPendingSolicitations();

    CompletableFuture<Void> addOrUpdateChildConnectionAsync(String username, Connection incomingConnection);

    CompletableFuture<Void> addParentConnectionAsync(Iterable<PeerEndpoint> parentCandidates);

    CompletableFuture<Void> broadcastMessageAsync(byte[] bytes, CancellationToken cancellationToken);

    default CompletableFuture<Void> broadcastMessageAsync(byte[] bytes) {
        return broadcastMessageAsync(bytes, CancellationToken.none());
    }

    void demoteFromBranchRoot();

    CompletableFuture<Void> getOrAddChildConnectionAsync(ConnectToPeerResponse connectToPeerResponse);

    void promoteToBranchRoot();

    void removeAndDisposeAll();

    void resetStatus();

    void setParentBranchLevel(int branchLevel);

    void setParentBranchRoot(String branchRoot);

    CompletableFuture<Void> updateStatusAsync(CancellationToken cancellationToken);

    default CompletableFuture<Void> updateStatusAsync() {
        return updateStatusAsync(CancellationToken.none());
    }

    @Override
    void close();
}
