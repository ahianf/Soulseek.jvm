// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.messaging.messages.ConnectToPeerResponse;
import dev.slsk.internal.network.tcp.TransportConnection;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/** Manages peer message and transfer connections. */
public interface PeerConnectionManager extends AutoCloseable {
    /** Returns a snapshot of established peer message connections. */
    List<PeerEndpoint> getMessageConnections();

    /** Returns a snapshot of pending connection solicitations. */
    Map<Integer, String> getPendingSolicitations();

    /**
     * Adopts an inbound message connection, superseding any cached one.
     *
     * <p>Blocking, like everything else here. The dedupe-and-broadcast cache
     * behind these is still a future per user — that is the one thing in this
     * class a future genuinely earns, and D11 replaces it with a connection
     * cell — but it is nobody else's business, so it stops at this boundary.
     */
    void addOrUpdateMessageConnection(String username, TransportConnection incomingConnection);

    TransportConnection awaitTransferConnection(
            String username, String filename, int remoteToken, CancellationSignal cancellationSignal);

    MessageConnection getCachedMessageConnection(String username);

    MessageConnection getOrAddMessageConnection(ConnectToPeerResponse connectToPeerResponse);

    MessageConnection getOrAddMessageConnection(
            String username, InetSocketAddress ipEndpoint, CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException;

    MessageConnection getOrAddMessageConnection(
            String username, InetSocketAddress ipEndpoint, int solicitationToken, CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException;

    TransferConnectionResult getTransferConnection(String username, int token, TransportConnection incomingConnection);

    TransferConnectionResult getTransferConnection(ConnectToPeerResponse connectToPeerResponse);

    TransportConnection getTransferConnection(
            String username, InetSocketAddress ipEndpoint, int token, CancellationSignal cancellationSignal);

    void removeAndCloseAll();

    boolean tryInvalidateMessageConnectionCache(String username);

    @Override
    void close();
}
