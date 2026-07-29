// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

import dev.slsk.CancellationSignal;
import dev.slsk.internal.options.ConnectionOptions;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletionException;

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

    /**
     * Connects to the configured endpoint, blocking until it lands.
     *
     * <p>Failures arrive the way {@link java.util.concurrent.CompletableFuture#join()}
     * presented them: a {@link java.util.concurrent.CancellationException} raw,
     * everything else wrapped in a {@link CompletionException}. Every caller in
     * this library already unwraps, so the shape of a failure did not have to
     * change when the thread hop that produced it went away.
     *
     * @param cancellationSignal the cancellation signal
     */
    void connect(CancellationSignal cancellationSignal);

    /** Connects without a cancellable token. */
    default void connect() {
        connect(CancellationSignal.none());
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
    byte[] read(long length, CancellationSignal cancellationSignal);

    /** Reads an exact byte count without a cancellable token. */
    default byte[] read(long length) {
        return read(length, CancellationSignal.none());
    }

    /** Reads an exact byte count into a stream. */
    void read(
            long length,
            OutputStream outputStream,
            ConnectionGovernor governor,
            ConnectionReporter reporter,
            CancellationSignal cancellationSignal);

    /** Reads to a stream using source defaults. */
    default void read(long length, OutputStream outputStream, ConnectionGovernor governor) {
        read(length, outputStream, governor, null, CancellationSignal.none());
    }

    /**
     * Waits for this connection to disconnect and returns the reason.
     *
     * @param cancellationSignal disconnects the connection when signalled
     * @return the disconnect message
     */
    String awaitDisconnect(CancellationSignal cancellationSignal);

    /** Waits without a cancellable token. */
    default String awaitDisconnect() {
        return awaitDisconnect(CancellationSignal.none());
    }

    /**
     * Writes an array, blocking until it lands.
     *
     * <p>Every caller here already owns a virtual thread, so dispatching the
     * write onto a second one and immediately blocking on the result bought a
     * thread whose only job was to be waited on. The distributed broadcast did
     * it once per child, per message.
     *
     * @param bytes the bytes to write
     * @param cancellationSignal the cancellation signal
     */
    void write(byte[] bytes, CancellationSignal cancellationSignal);

    /** Writes an array without a cancellable token. */
    default void write(byte[] bytes) {
        write(bytes, CancellationSignal.none());
    }

    /** Writes an exact byte count from a stream. */
    void write(
            long length,
            InputStream inputStream,
            ConnectionGovernor governor,
            ConnectionReporter reporter,
            CancellationSignal cancellationSignal);

    /** Writes from a stream using source defaults. */
    default void write(long length, InputStream inputStream) {
        write(length, inputStream, null, null, CancellationSignal.none());
    }

    /** Writes from a stream using a governor. */
    default void write(long length, InputStream inputStream, ConnectionGovernor governor) {
        write(length, inputStream, governor, null, CancellationSignal.none());
    }

    /** Disposes the connection. */
    @Override
    void close();
}
