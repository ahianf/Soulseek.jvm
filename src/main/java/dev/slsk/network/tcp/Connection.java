// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.network.tcp;

import dev.slsk.CancellationSignal;
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
    void addDataReadListener(ConnectionEventListener<ConnectionDataEvent> listener);

    /** Removes a data-read listener. */
    void removeDataReadListener(ConnectionEventListener<ConnectionDataEvent> listener);

    /** Adds a data-written listener. */
    void addDataWrittenListener(ConnectionEventListener<ConnectionDataEvent> listener);

    /** Removes a data-written listener. */
    void removeDataWrittenListener(ConnectionEventListener<ConnectionDataEvent> listener);

    /** Adds a disconnected listener. */
    void addDisconnectedListener(ConnectionEventListener<ConnectionDisconnectedEvent> listener);

    /** Removes a disconnected listener. */
    void removeDisconnectedListener(ConnectionEventListener<ConnectionDisconnectedEvent> listener);

    /** Adds a state-change listener. */
    void addStateChangedListener(ConnectionEventListener<ConnectionStateChangedEvent> listener);

    /** Removes a state-change listener. */
    void removeStateChangedListener(ConnectionEventListener<ConnectionStateChangedEvent> listener);

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
    CompletableFuture<Void> connectAsync(CancellationSignal cancellationSignal);

    /** Connects without a cancellable token. */
    default CompletableFuture<Void> connectAsync() {
        return connectAsync(CancellationSignal.none());
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
    CompletableFuture<byte[]> readAsync(long length, CancellationSignal cancellationSignal);

    /** Reads an exact byte count without a cancellable token. */
    default CompletableFuture<byte[]> readAsync(long length) {
        return readAsync(length, CancellationSignal.none());
    }

    /** Reads an exact byte count into a stream. */
    CompletableFuture<Void> readAsync(
            long length,
            OutputStream outputStream,
            ConnectionGovernor governor,
            ConnectionReporter reporter,
            CancellationSignal cancellationSignal);

    /** Reads to a stream using source defaults. */
    default CompletableFuture<Void> readAsync(long length, OutputStream outputStream, ConnectionGovernor governor) {
        return readAsync(length, outputStream, governor, null, CancellationSignal.none());
    }

    /** Waits for this connection to disconnect. */
    CompletableFuture<String> waitForDisconnect(CancellationSignal cancellationSignal);

    /** Waits without a cancellable token. */
    default CompletableFuture<String> waitForDisconnect() {
        return waitForDisconnect(CancellationSignal.none());
    }

    /** Writes an array. */
    CompletableFuture<Void> writeAsync(byte[] bytes, CancellationSignal cancellationSignal);

    /** Writes an array without a cancellable token. */
    default CompletableFuture<Void> writeAsync(byte[] bytes) {
        return writeAsync(bytes, CancellationSignal.none());
    }

    /**
     * Writes an array on the calling thread.
     *
     * <p>The blocking sibling of {@link #writeAsync(byte[], CancellationSignal)},
     * mirroring the blocking read the framed read loop uses. It is for callers
     * already running on their own virtual thread that would otherwise dispatch
     * a write and immediately block on the result: that pattern costs a second
     * thread whose only job is to be waited on. The distributed broadcast did
     * it once per child, per message.
     *
     * @param bytes the bytes to write
     * @param cancellationSignal the cancellation signal
     * @throws Exception if the write fails
     */
    default void write(byte[] bytes, CancellationSignal cancellationSignal) throws Exception {
        writeAsync(bytes, cancellationSignal).join();
    }

    /** Writes an exact byte count from a stream. */
    CompletableFuture<Void> writeAsync(
            long length,
            InputStream inputStream,
            ConnectionGovernor governor,
            ConnectionReporter reporter,
            CancellationSignal cancellationSignal);

    /** Writes from a stream using source defaults. */
    default CompletableFuture<Void> writeAsync(long length, InputStream inputStream) {
        return writeAsync(length, inputStream, null, null, CancellationSignal.none());
    }

    /** Writes from a stream using a governor. */
    default CompletableFuture<Void> writeAsync(long length, InputStream inputStream, ConnectionGovernor governor) {
        return writeAsync(length, inputStream, governor, null, CancellationSignal.none());
    }

    /** Disposes the connection. */
    @Override
    void close();
}
