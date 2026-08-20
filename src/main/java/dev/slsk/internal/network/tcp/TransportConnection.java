// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

import dev.slsk.Subscription;
import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.options.ConnectionOptions;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/** Provides client connections for TCP network services. */
public interface TransportConnection extends AutoCloseable {
    enum Kind {
        CONNECTED,
        DATA_READ,
        DATA_WRITTEN,
        DISCONNECTED,
        STATE_CHANGED
    }

    /** Subscribes to a connection event. Data-written callbacks run on the I/O worker. */
    <T> Subscription subscribe(Kind kind, Consumer<? super T> listener);

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
    TransportState getState();

    /** Returns the connection traits. */
    ConnectionType getType();

    /** Sets the connection traits. */
    void setType(ConnectionType type);

    /** Returns the current depth of the double-buffered write queue. */
    int getWriteQueueDepth();

    /**
     * Connects to the configured endpoint, blocking until it lands.
     *
     * <p>A failure is raised as itself: a lapsed deadline is the checked
     * {@link TimeoutException}, a caller's cancellation is a raw
     * {@link java.util.concurrent.CancellationException}, and everything else
     * is a domain exception. Nothing arrives wrapped.
     *
     * @param cancellationSignal the cancellation signal
     * @throws TimeoutException if the connect deadline lapsed
     */
    void connect(CancellationSignal cancellationSignal) throws InterruptedException, TimeoutException;

    /** Connects without a cancellable token. */
    default void connect() throws InterruptedException, TimeoutException {
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

    /** Decouples and returns the underlying socket connector. */
    SocketConnector handoffConnector();

    /** Reads an exact byte count into a new array. */
    byte[] read(long length, CancellationSignal cancellationSignal) throws InterruptedException, TimeoutException;

    /** Reads an exact byte count without a cancellable token. */
    default byte[] read(long length) throws InterruptedException, TimeoutException {
        return read(length, CancellationSignal.none());
    }

    /** Reads an exact byte count into a stream. */
    void read(
            long length,
            OutputStream outputStream,
            ConnectionGovernor governor,
            ConnectionReporter reporter,
            CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException;

    /** Reads to a stream using source defaults. */
    default void read(long length, OutputStream outputStream, ConnectionGovernor governor)
            throws InterruptedException, TimeoutException {
        read(length, outputStream, governor, null, CancellationSignal.none());
    }

    /**
     * Waits for this connection to disconnect and returns the reason.
     *
     * @param cancellationSignal disconnects the connection when signalled
     * @return the disconnect message
     */
    String awaitDisconnect(CancellationSignal cancellationSignal) throws InterruptedException, TimeoutException;

    /** Waits without a cancellable token. */
    default String awaitDisconnect() throws InterruptedException, TimeoutException {
        return awaitDisconnect(CancellationSignal.none());
    }

    /**
     * Writes an array, blocking until it lands.
     *
     * <p>The caller waits only on the connection-owned writer's completion;
     * socket I/O never runs on the caller. Cancellation removes a queued frame,
     * but once writing starts the writer finishes the complete frame so a
     * shared protocol stream is never corrupted by caller cancellation.
     *
     * <p>The array is adopted: the connection keeps it until the frame is
     * written, so the caller must not mutate it after this is invoked.
     *
     * @param bytes the bytes to write
     * @param cancellationSignal the cancellation signal
     */
    void write(byte[] bytes, CancellationSignal cancellationSignal) throws InterruptedException, TimeoutException;

    /** Writes an array without a cancellable token. */
    default void write(byte[] bytes) throws InterruptedException, TimeoutException {
        write(bytes, CancellationSignal.none());
    }

    /** The settled half of a two-phase framed write; see {@link #beginWrite}. */
    @FunctionalInterface
    interface PendingWrite {
        /**
         * Waits for the frame's outcome.
         *
         * <p>Failures arrive with the same shapes {@link #write(byte[],
         * CancellationSignal)} raises them in.
         */
        void await() throws InterruptedException, TimeoutException;
    }

    /**
     * Enqueues a framed write and returns the wait for its outcome.
     *
     * <p>The two halves of {@link #write(byte[], CancellationSignal)}, split so
     * one caller can put a frame on several connections before waiting on any
     * of them — the broadcast fan-out — without a thread per connection whose
     * only purpose is to be waited on. The array is adopted exactly as it is
     * by {@code write}.
     *
     * <p>This default writes synchronously and returns an already-settled
     * wait, which preserves the blocking contract for implementations that
     * have no queue to split against.
     *
     * @param bytes the bytes to write
     * @param cancellationSignal the cancellation signal
     * @return the wait for the frame's outcome
     */
    default PendingWrite beginWrite(byte[] bytes, CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException {
        write(bytes, cancellationSignal);
        return () -> {};
    }

    /** Writes an exact byte count from a stream. */
    void write(
            long length,
            InputStream inputStream,
            ConnectionGovernor governor,
            ConnectionReporter reporter,
            CancellationSignal cancellationSignal)
            throws InterruptedException, TimeoutException;

    /** Writes from a stream using source defaults. */
    default void write(long length, InputStream inputStream) throws InterruptedException, TimeoutException {
        write(length, inputStream, null, null, CancellationSignal.none());
    }

    /** Writes from a stream using a governor. */
    default void write(long length, InputStream inputStream, ConnectionGovernor governor)
            throws InterruptedException, TimeoutException {
        write(length, inputStream, governor, null, CancellationSignal.none());
    }

    /** Closes the connection. */
    @Override
    void close();
}
