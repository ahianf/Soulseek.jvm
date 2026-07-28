// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network;

import dev.slsk.CancellationSignal;
import dev.slsk.internal.diagnostics.DiagnosticSource;
import dev.slsk.internal.messaging.messages.ConnectToPeerResponse;
import dev.slsk.internal.network.tcp.Connection;
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
            String username, String filename, int remoteToken, CancellationSignal cancellationSignal);

    CompletableFuture<MessageConnection> getCachedMessageConnectionAsync(String username);

    CompletableFuture<MessageConnection> getOrAddMessageConnectionAsync(ConnectToPeerResponse connectToPeerResponse);

    CompletableFuture<MessageConnection> getOrAddMessageConnectionAsync(
            String username, InetSocketAddress ipEndpoint, CancellationSignal cancellationSignal);

    CompletableFuture<MessageConnection> getOrAddMessageConnectionAsync(
            String username,
            InetSocketAddress ipEndpoint,
            int solicitationToken,
            CancellationSignal cancellationSignal);

    CompletableFuture<TransferConnectionResult> getTransferConnectionAsync(
            String username, int token, Connection incomingConnection);

    CompletableFuture<TransferConnectionResult> getTransferConnectionAsync(ConnectToPeerResponse connectToPeerResponse);

    CompletableFuture<Connection> getTransferConnectionAsync(
            String username, InetSocketAddress ipEndpoint, int token, CancellationSignal cancellationSignal);

    void removeAndDisposeAll();

    boolean tryInvalidateMessageConnectionCache(String username);

    @Override
    void close();
}
