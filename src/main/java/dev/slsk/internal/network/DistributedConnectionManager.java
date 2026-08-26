// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import dev.slsk.Subscription;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.messaging.messages.ConnectToPeerResponse;
import dev.slsk.internal.network.tcp.TransportConnection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/** Manages distributed-network parent and child connections. */
public interface DistributedConnectionManager extends AutoCloseable {
    enum Kind {
        CHILD_ADDED,
        CHILD_DISCONNECTED,
        DEMOTED_FROM_BRANCH_ROOT,
        PARENT_ADOPTED,
        PARENT_DISCONNECTED,
        PROMOTED_TO_BRANCH_ROOT,
        STATE_CHANGED
    }

    <T> Subscription subscribe(Kind kind, Consumer<? super T> listener);

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

    /**
     * Adopts an inbound direct child connection, superseding any existing one.
     *
     * <p>Blocking, and it throws what stopped it. A caller on a read loop
     * dispatches it rather than waiting.
     *
     * @param username the child's username
     * @param incomingConnection the accepted connection to hand off
     */
    void addOrUpdateChildConnection(String username, TransportConnection incomingConnection);

    /**
     * Attempts every candidate at once and adopts the one with the lowest branch
     * level.
     *
     * <p>Blocking: it returns when every candidate has settled and the best of
     * them is the parent. A caller on a read loop dispatches it.
     *
     * @param parentCandidates the candidates to attempt
     */
    void addParentConnection(Iterable<PeerEndpoint> parentCandidates);

    /**
     * Writes a message to every child, blocking until all of them have settled.
     *
     * @param bytes the message
     * @param cancellationSignal the cancellation signal
     */
    void broadcastMessage(byte[] bytes, CancellationSignal cancellationSignal);

    default void broadcastMessage(byte[] bytes) {
        broadcastMessage(bytes, CancellationSignal.none());
    }

    void demoteFromBranchRoot();

    /**
     * Establishes an inbound indirect child connection, or leaves the cached one
     * in place.
     *
     * <p>Blocking, and it throws what stopped it.
     *
     * @param connectToPeerResponse the server's solicitation response
     */
    void getOrAddChildConnection(ConnectToPeerResponse connectToPeerResponse);

    void promoteToBranchRoot();

    void removeAndCloseAll();

    void resetStatus();

    void setParentBranchLevel(int branchLevel);

    void setParentBranchRoot(String branchRoot);

    /**
     * Writes this node's distributed status to the server if it has changed.
     *
     * <p>Blocking, and it reports its own failures; a status update is nobody's
     * to fail.
     *
     * @param cancellationSignal the cancellation signal
     */
    void updateStatus(CancellationSignal cancellationSignal);

    default void updateStatus() {
        updateStatus(CancellationSignal.none());
    }

    @Override
    void close();
}
