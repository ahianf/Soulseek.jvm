// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network.tcp;

import dev.slsk.CancellationToken;
import dev.slsk.options.ConnectionOptions;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** Provides client connections for TCP network services. */
public interface Connection extends AutoCloseable {
    /** Adds a connected-event listener. */
    void addConnectedListener(ConnectionEventListener<Void> listener);

    /** Removes a connected-event listener. */
    void removeConnectedListener(ConnectionEventListener<Void> listener);

    /** Adds a data-read listener. */
    void addDataReadListener(ConnectionEventListener<ConnectionDataEventArgs> listener);

    /** Removes a data-read listener. */
    void removeDataReadListener(ConnectionEventListener<ConnectionDataEventArgs> listener);

    /** Adds a data-written listener. */
    void addDataWrittenListener(ConnectionEventListener<ConnectionDataEventArgs> listener);

    /** Removes a data-written listener. */
    void removeDataWrittenListener(ConnectionEventListener<ConnectionDataEventArgs> listener);

    /** Adds a disconnected listener. */
    void addDisconnectedListener(ConnectionEventListener<ConnectionDisconnectedEventArgs> listener);

    /** Removes a disconnected listener. */
    void removeDisconnectedListener(ConnectionEventListener<ConnectionDisconnectedEventArgs> listener);

    /** Adds a state-change listener. */
    void addStateChangedListener(ConnectionEventListener<ConnectionStateChangedEventArgs> listener);

    /** Removes a state-change listener. */
    void removeStateChangedListener(ConnectionEventListener<ConnectionStateChangedEventArgs> listener);

    /** Returns the connection identifier. */
    UUID getId();

    /** Returns the elapsed time since the last activity. */
    Duration getInactiveTime();

    /** Returns the remote endpoint. */
    InetSocketAddress getIpEndpoint();

    /** Returns the connection key. */
    ConnectionKey getKey();

    /** Returns the connection options. */
    ConnectionOptions getOptions();

    /** Returns the current state. */
    ConnectionState getState();

    /** Returns the connection traits. */
    ConnectionTypes getType();

    /** Sets the connection traits. */
    void setType(ConnectionTypes type);

    /** Returns the current depth of the double-buffered write queue. */
    int getWriteQueueDepth();

    /** Connects to the configured endpoint. */
    CompletableFuture<Void> connectAsync(CancellationToken cancellationToken);

    /** Connects without a cancellable token. */
    default CompletableFuture<Void> connectAsync() {
        return connectAsync(CancellationToken.none());
    }

    /** Disconnects without optional details. */
    default void disconnect() {
        disconnect(null, null);
    }

    /** Disconnects with a reason. */
    default void disconnect(String message) {
        disconnect(message, null);
    }

    /** Disconnects with optional details. */
    void disconnect(String message, Exception exception);

    /** Decouples and returns the underlying TCP client. */
    TcpClient handoffTcpClient();

    /** Reads an exact byte count into a new array. */
    CompletableFuture<byte[]> readAsync(long length, CancellationToken cancellationToken);

    /** Reads an exact byte count without a cancellable token. */
    default CompletableFuture<byte[]> readAsync(long length) {
        return readAsync(length, CancellationToken.none());
    }

    /** Reads an exact byte count into a stream. */
    CompletableFuture<Void> readAsync(
            long length,
            OutputStream outputStream,
            ConnectionGovernor governor,
            ConnectionReporter reporter,
            CancellationToken cancellationToken);

    /** Reads to a stream using source defaults. */
    default CompletableFuture<Void> readAsync(long length, OutputStream outputStream, ConnectionGovernor governor) {
        return readAsync(length, outputStream, governor, null, CancellationToken.none());
    }

    /** Waits for this connection to disconnect. */
    CompletableFuture<String> waitForDisconnect(CancellationToken cancellationToken);

    /** Waits without a cancellable token. */
    default CompletableFuture<String> waitForDisconnect() {
        return waitForDisconnect(CancellationToken.none());
    }

    /** Writes an array. */
    CompletableFuture<Void> writeAsync(byte[] bytes, CancellationToken cancellationToken);

    /** Writes an array without a cancellable token. */
    default CompletableFuture<Void> writeAsync(byte[] bytes) {
        return writeAsync(bytes, CancellationToken.none());
    }

    /** Writes an exact byte count from a stream. */
    CompletableFuture<Void> writeAsync(
            long length,
            InputStream inputStream,
            ConnectionGovernor governor,
            ConnectionReporter reporter,
            CancellationToken cancellationToken);

    /** Writes from a stream using source defaults. */
    default CompletableFuture<Void> writeAsync(long length, InputStream inputStream) {
        return writeAsync(length, inputStream, null, null, CancellationToken.none());
    }

    /** Writes from a stream using a governor. */
    default CompletableFuture<Void> writeAsync(long length, InputStream inputStream, ConnectionGovernor governor) {
        return writeAsync(length, inputStream, governor, null, CancellationToken.none());
    }

    /** Disposes the connection. */
    @Override
    void close();
}
