// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network;

import dev.slsk.CancellationSignal;
import dev.slsk.DistributedNetworkInfo;
import dev.slsk.diagnostics.DiagnosticSource;
import dev.slsk.events.DistributedChildEvent;
import dev.slsk.events.DistributedParentEvent;
import dev.slsk.messaging.messages.ConnectToPeerResponse;
import dev.slsk.network.tcp.Connection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Manages distributed-network parent and child connections. */
public interface DistributedConnectionManager extends AutoCloseable, DiagnosticSource {
    void addChildAddedListener(DistributedManagerEventListener<DistributedChildEvent> listener);

    void removeChildAddedListener(DistributedManagerEventListener<DistributedChildEvent> listener);

    void addChildDisconnectedListener(DistributedManagerEventListener<DistributedChildEvent> listener);

    void removeChildDisconnectedListener(DistributedManagerEventListener<DistributedChildEvent> listener);

    void addDemotedFromBranchRootListener(DistributedManagerEventListener<Void> listener);

    void removeDemotedFromBranchRootListener(DistributedManagerEventListener<Void> listener);

    void addParentAdoptedListener(DistributedManagerEventListener<DistributedParentEvent> listener);

    void removeParentAdoptedListener(DistributedManagerEventListener<DistributedParentEvent> listener);

    void addParentDisconnectedListener(DistributedManagerEventListener<DistributedParentEvent> listener);

    void removeParentDisconnectedListener(DistributedManagerEventListener<DistributedParentEvent> listener);

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

    CompletableFuture<Void> broadcastMessageAsync(byte[] bytes, CancellationSignal cancellationSignal);

    default CompletableFuture<Void> broadcastMessageAsync(byte[] bytes) {
        return broadcastMessageAsync(bytes, CancellationSignal.none());
    }

    void demoteFromBranchRoot();

    CompletableFuture<Void> getOrAddChildConnectionAsync(ConnectToPeerResponse connectToPeerResponse);

    void promoteToBranchRoot();

    void removeAndDisposeAll();

    void resetStatus();

    void setParentBranchLevel(int branchLevel);

    void setParentBranchRoot(String branchRoot);

    CompletableFuture<Void> updateStatusAsync(CancellationSignal cancellationSignal);

    default CompletableFuture<Void> updateStatusAsync() {
        return updateStatusAsync(CancellationSignal.none());
    }

    @Override
    void close();
}
