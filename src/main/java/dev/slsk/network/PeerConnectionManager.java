// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network;

import dev.slsk.CancellationToken;
import dev.slsk.diagnostics.DiagnosticSource;
import dev.slsk.messaging.messages.ConnectToPeerResponse;
import dev.slsk.network.tcp.Connection;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Manages peer message and transfer connections. */
public interface PeerConnectionManager extends AutoCloseable, DiagnosticSource {
    /** Returns a snapshot of established peer message connections. */
    List<PeerEndpoint> getMessageConnections();

    /** Returns a snapshot of pending connection solicitations. */
    Map<Integer, String> getPendingSolicitations();

    CompletableFuture<Void> addOrUpdateMessageConnectionAsync(String username, Connection incomingConnection);

    CompletableFuture<Connection> awaitTransferConnectionAsync(
            String username, String filename, int remoteToken, CancellationToken cancellationToken);

    CompletableFuture<MessageConnection> getCachedMessageConnectionAsync(String username);

    CompletableFuture<MessageConnection> getOrAddMessageConnectionAsync(ConnectToPeerResponse connectToPeerResponse);

    CompletableFuture<MessageConnection> getOrAddMessageConnectionAsync(
            String username, InetSocketAddress ipEndpoint, CancellationToken cancellationToken);

    CompletableFuture<MessageConnection> getOrAddMessageConnectionAsync(
            String username, InetSocketAddress ipEndpoint, int solicitationToken, CancellationToken cancellationToken);

    CompletableFuture<TransferConnectionResult> getTransferConnectionAsync(
            String username, int token, Connection incomingConnection);

    CompletableFuture<TransferConnectionResult> getTransferConnectionAsync(ConnectToPeerResponse connectToPeerResponse);

    CompletableFuture<Connection> getTransferConnectionAsync(
            String username, InetSocketAddress ipEndpoint, int token, CancellationToken cancellationToken);

    void removeAndDisposeAll();

    boolean tryInvalidateMessageConnectionCache(String username);

    @Override
    void close();
}
