// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import java.net.Socket;

/**
 * Options for TCP connections.
 */
public class ConnectionOptions {
    /** Default TCP read buffer size. */
    public static final int DEFAULT_READ_BUFFER_SIZE = 16_384;
    /** Default TCP write buffer size. */
    public static final int DEFAULT_WRITE_BUFFER_SIZE = 16_384;
    /** Default double-buffered write queue size. */
    public static final int DEFAULT_WRITE_QUEUE_SIZE = 250;
    /** Default connection timeout in milliseconds. */
    public static final int DEFAULT_CONNECT_TIMEOUT = 10_000;
    /**
     * Floor for the wait on an answer to a solicited indirect connection.
     *
     * <p>Not a connect timeout, which is what that wait used to be given. A
     * direct connect is one round trip to a host that is either listening or
     * not; a solicitation goes to the server, is relayed, is noticed by a peer
     * that may be busy, and only then comes back as a PierceFirewall. Charging
     * the second to the first's budget writes off peers that were answering.
     *
     * <p>Twenty seconds is Nicotine+'s {@code INDIRECT_REQUEST_TIMEOUT}. One
     * recorded peer on a mobile link answered between 0.6 and 7.3 seconds when
     * it answered at all, against a ten-second budget.
     */
    public static final int DEFAULT_INDIRECT_SOLICITATION_TIMEOUT = 20_000;
    /** Default inactivity timeout in milliseconds. */
    public static final int DEFAULT_INACTIVITY_TIMEOUT = 15_000;
    /**
     * Default time a producer waits for room in a full write queue.
     *
     * <p>Zero would reproduce the source's drop-and-disconnect. This gives a
     * burst time to drain before the connection is written off.
     */
    public static final int DEFAULT_WRITE_QUEUE_TIMEOUT = 10_000;

    private final SocketConfigurator configureSocket;
    private final int connectTimeout;
    private final int inactivityTimeout;
    private final ProxyOptions proxyOptions;
    private final int readBufferSize;
    private final int writeBufferSize;
    private final int writeQueueSize;

    /** How long a producer waits for room in a full write queue. */
    private final int writeQueueTimeout;

    /**
     * Creates connection options with source defaults.
     */
    public ConnectionOptions() {
        this(
                DEFAULT_READ_BUFFER_SIZE,
                DEFAULT_WRITE_BUFFER_SIZE,
                DEFAULT_WRITE_QUEUE_SIZE,
                DEFAULT_CONNECT_TIMEOUT,
                DEFAULT_INACTIVITY_TIMEOUT,
                null,
                null);
    }

    /**
     * Creates connection options overriding the read buffer size.
     *
     * @param readBufferSize the read buffer size
     */
    public ConnectionOptions(int readBufferSize) {
        this(
                readBufferSize,
                DEFAULT_WRITE_BUFFER_SIZE,
                DEFAULT_WRITE_QUEUE_SIZE,
                DEFAULT_CONNECT_TIMEOUT,
                DEFAULT_INACTIVITY_TIMEOUT,
                null,
                null);
    }

    /**
     * Creates connection options overriding buffer sizes.
     *
     * @param readBufferSize the read buffer size
     * @param writeBufferSize the write buffer size
     */
    public ConnectionOptions(int readBufferSize, int writeBufferSize) {
        this(
                readBufferSize,
                writeBufferSize,
                DEFAULT_WRITE_QUEUE_SIZE,
                DEFAULT_CONNECT_TIMEOUT,
                DEFAULT_INACTIVITY_TIMEOUT,
                null,
                null);
    }

    /**
     * Creates connection options overriding buffer and queue sizes.
     *
     * @param readBufferSize the read buffer size
     * @param writeBufferSize the write buffer size
     * @param writeQueueSize the write queue size
     */
    public ConnectionOptions(int readBufferSize, int writeBufferSize, int writeQueueSize) {
        this(
                readBufferSize,
                writeBufferSize,
                writeQueueSize,
                DEFAULT_CONNECT_TIMEOUT,
                DEFAULT_INACTIVITY_TIMEOUT,
                null,
                null);
    }

    /**
     * Creates connection options through the connect timeout.
     *
     * @param readBufferSize the read buffer size
     * @param writeBufferSize the write buffer size
     * @param writeQueueSize the write queue size
     * @param connectTimeout the connect timeout
     */
    public ConnectionOptions(int readBufferSize, int writeBufferSize, int writeQueueSize, int connectTimeout) {
        this(readBufferSize, writeBufferSize, writeQueueSize, connectTimeout, DEFAULT_INACTIVITY_TIMEOUT, null, null);
    }

    /**
     * Creates connection options through the inactivity timeout.
     *
     * @param readBufferSize the read buffer size
     * @param writeBufferSize the write buffer size
     * @param writeQueueSize the write queue size
     * @param connectTimeout the connect timeout
     * @param inactivityTimeout the inactivity timeout
     */
    public ConnectionOptions(
            int readBufferSize, int writeBufferSize, int writeQueueSize, int connectTimeout, int inactivityTimeout) {
        this(readBufferSize, writeBufferSize, writeQueueSize, connectTimeout, inactivityTimeout, null, null);
    }

    /**
     * Creates connection options with proxy settings.
     *
     * @param readBufferSize the read buffer size
     * @param writeBufferSize the write buffer size
     * @param writeQueueSize the write queue size
     * @param connectTimeout the connect timeout
     * @param inactivityTimeout the inactivity timeout
     * @param proxyOptions the proxy settings
     */
    public ConnectionOptions(
            int readBufferSize,
            int writeBufferSize,
            int writeQueueSize,
            int connectTimeout,
            int inactivityTimeout,
            ProxyOptions proxyOptions) {
        this(readBufferSize, writeBufferSize, writeQueueSize, connectTimeout, inactivityTimeout, proxyOptions, null);
    }

    /**
     * Creates connection options.
     *
     * @param readBufferSize the read buffer size
     * @param writeBufferSize the write buffer size
     * @param writeQueueSize the write queue size
     * @param connectTimeout the connect timeout in milliseconds
     * @param inactivityTimeout the inactivity timeout in milliseconds
     * @param proxyOptions the SOCKS5 proxy settings
     * @param configureSocket the socket configuration callback
     */
    public ConnectionOptions(
            int readBufferSize,
            int writeBufferSize,
            int writeQueueSize,
            int connectTimeout,
            int inactivityTimeout,
            ProxyOptions proxyOptions,
            SocketConfigurator configureSocket) {
        this.readBufferSize = readBufferSize;
        this.writeBufferSize = writeBufferSize;
        this.writeQueueSize = writeQueueSize;
        this.writeQueueTimeout = DEFAULT_WRITE_QUEUE_TIMEOUT;
        this.connectTimeout = connectTimeout;
        this.inactivityTimeout = inactivityTimeout;
        this.proxyOptions = proxyOptions;
        this.configureSocket = configureSocket == null ? this::configureSocketDefault : configureSocket;
    }

    /**
     * Returns the socket configuration callback.
     *
     * @return the callback
     */
    public final SocketConfigurator getConfigureSocket() {
        return configureSocket;
    }

    /**
     * Returns the connection timeout.
     *
     * @return the timeout in milliseconds
     */
    public final int getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Returns how long to wait for an answer to a solicited indirect connection.
     *
     * <p>The configured connect timeout or {@link
     * #DEFAULT_INDIRECT_SOLICITATION_TIMEOUT}, whichever is longer: a consumer
     * that raises the connect timeout means connections here are slow, which is
     * a reason to wait longer for the relayed path too, never less.
     *
     * @return the timeout in milliseconds
     */
    public final int getIndirectSolicitationTimeout() {
        return Math.max(connectTimeout, DEFAULT_INDIRECT_SOLICITATION_TIMEOUT);
    }

    /**
     * Returns the inactivity timeout.
     *
     * @return the timeout in milliseconds
     */
    public final int getInactivityTimeout() {
        return inactivityTimeout;
    }

    /**
     * Returns the proxy settings.
     *
     * @return the proxy settings, or {@code null}
     */
    public final ProxyOptions getProxyOptions() {
        return proxyOptions;
    }

    /**
     * Returns the read buffer size.
     *
     * @return the read buffer size
     */
    public final int getReadBufferSize() {
        return readBufferSize;
    }

    /**
     * Returns the write buffer size.
     *
     * @return the write buffer size
     */
    public final int getWriteBufferSize() {
        return writeBufferSize;
    }

    /**
     * Returns the write queue size.
     *
     * @return the write queue size
     */
    private ConnectionOptions(ConnectionOptions source, int writeQueueTimeout) {
        this.configureSocket = source.configureSocket;
        this.connectTimeout = source.connectTimeout;
        this.inactivityTimeout = source.inactivityTimeout;
        this.proxyOptions = source.proxyOptions;
        this.readBufferSize = source.readBufferSize;
        this.writeBufferSize = source.writeBufferSize;
        this.writeQueueSize = source.writeQueueSize;
        this.writeQueueTimeout = writeQueueTimeout;
    }

    /**
     * Returns a copy that waits a different length of time for write-queue room.
     *
     * @param value the timeout in milliseconds; {@code 0} restores the source's
     *     drop-immediately behaviour
     * @return a copy carrying the timeout
     */
    public final ConnectionOptions withWriteQueueTimeout(int value) {
        return value == writeQueueTimeout ? this : new ConnectionOptions(this, value);
    }

    /**
     * Returns how long a producer waits for room in a full write queue.
     *
     * @return the timeout in milliseconds
     */
    public final int getWriteQueueTimeout() {
        return writeQueueTimeout;
    }

    public final int getWriteQueueSize() {
        return writeQueueSize;
    }

    /**
     * Returns a copy with its inactivity timeout fixed to {@code -1}.
     *
     * @return the copied options
     */
    public final ConnectionOptions withoutInactivityTimeout() {
        return new ConnectionOptions(
                readBufferSize, writeBufferSize, writeQueueSize, connectTimeout, -1, proxyOptions, configureSocket);
    }

    private void configureSocketDefault(Socket socket) {
        // Source default is intentionally a no-op.
    }
}
