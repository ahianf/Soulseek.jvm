// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import java.time.Duration;

/** Options for TCP connections. */
public record ConnectionOptions(
        int readBufferSize,
        int writeBufferSize,
        int writeQueueSize,
        Duration writeQueueTimeout,
        Duration connectTimeout,
        Duration inactivityTimeout,
        ProxyOptions proxyOptions,
        SocketConfigurator configureSocket) {
    /** Default TCP read buffer size. */
    public static final int DEFAULT_READ_BUFFER_SIZE = 16_384;
    /** Default TCP write buffer size. */
    public static final int DEFAULT_WRITE_BUFFER_SIZE = 16_384;
    /** Default double-buffered write queue size. */
    public static final int DEFAULT_WRITE_QUEUE_SIZE = 250;
    /** Default connection timeout. */
    public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
    /** Minimum wait for an answer to a solicited indirect connection. */
    public static final Duration DEFAULT_INDIRECT_SOLICITATION_TIMEOUT = Duration.ofSeconds(20);
    /** Default inactivity timeout. */
    public static final Duration DEFAULT_INACTIVITY_TIMEOUT = Duration.ofSeconds(15);
    /** Default time a producer waits for room in a full write queue. */
    public static final Duration DEFAULT_WRITE_QUEUE_TIMEOUT = Duration.ofSeconds(10);

    private static final SocketConfigurator DEFAULT_SOCKET_CONFIGURATOR = socket -> {};

    /** Normalizes an omitted socket configurator to the no-op default. */
    public ConnectionOptions {
        configureSocket = configureSocket == null ? DEFAULT_SOCKET_CONFIGURATOR : configureSocket;
    }

    /** Creates connection options with defaults. */
    public ConnectionOptions() {
        this(
                DEFAULT_READ_BUFFER_SIZE,
                DEFAULT_WRITE_BUFFER_SIZE,
                DEFAULT_WRITE_QUEUE_SIZE,
                DEFAULT_WRITE_QUEUE_TIMEOUT,
                DEFAULT_CONNECT_TIMEOUT,
                DEFAULT_INACTIVITY_TIMEOUT,
                null,
                null);
    }

    /** Starts a connection-options builder with defaults. */
    public static Builder builder() {
        return new Builder();
    }

    /** Returns the relayed-connection budget, never shorter than a direct connect. */
    public Duration indirectSolicitationTimeout() {
        if (connectTimeout == null || connectTimeout.compareTo(DEFAULT_INDIRECT_SOLICITATION_TIMEOUT) < 0) {
            return DEFAULT_INDIRECT_SOLICITATION_TIMEOUT;
        }
        return connectTimeout;
    }

    /** Returns a copy with another write-queue backpressure budget. */
    public ConnectionOptions withWriteQueueTimeout(Duration value) {
        return value == writeQueueTimeout
                ? this
                : new ConnectionOptions(
                        readBufferSize,
                        writeBufferSize,
                        writeQueueSize,
                        value,
                        connectTimeout,
                        inactivityTimeout,
                        proxyOptions,
                        configureSocket);
    }

    /** Returns a copy with no inactivity deadline. */
    public ConnectionOptions withoutInactivityTimeout() {
        return inactivityTimeout == null
                ? this
                : new ConnectionOptions(
                        readBufferSize,
                        writeBufferSize,
                        writeQueueSize,
                        writeQueueTimeout,
                        connectTimeout,
                        null,
                        proxyOptions,
                        configureSocket);
    }

    /** Builder for field-named connection configuration. */
    public static final class Builder {
        private SocketConfigurator configureSocket;
        private Duration connectTimeout = DEFAULT_CONNECT_TIMEOUT;
        private Duration inactivityTimeout = DEFAULT_INACTIVITY_TIMEOUT;
        private ProxyOptions proxyOptions;
        private int readBufferSize = DEFAULT_READ_BUFFER_SIZE;
        private int writeBufferSize = DEFAULT_WRITE_BUFFER_SIZE;
        private int writeQueueSize = DEFAULT_WRITE_QUEUE_SIZE;
        private Duration writeQueueTimeout = DEFAULT_WRITE_QUEUE_TIMEOUT;

        public Builder readBufferSize(int value) {
            readBufferSize = value;
            return this;
        }

        public Builder writeBufferSize(int value) {
            writeBufferSize = value;
            return this;
        }

        public Builder writeQueueSize(int value) {
            writeQueueSize = value;
            return this;
        }

        public Builder writeQueueTimeout(Duration value) {
            writeQueueTimeout = value;
            return this;
        }

        public Builder connectTimeout(Duration value) {
            connectTimeout = value;
            return this;
        }

        public Builder inactivityTimeout(Duration value) {
            inactivityTimeout = value;
            return this;
        }

        public Builder proxyOptions(ProxyOptions value) {
            proxyOptions = value;
            return this;
        }

        public Builder configureSocket(SocketConfigurator value) {
            configureSocket = value;
            return this;
        }

        public ConnectionOptions build() {
            return new ConnectionOptions(
                    readBufferSize,
                    writeBufferSize,
                    writeQueueSize,
                    writeQueueTimeout,
                    connectTimeout,
                    inactivityTimeout,
                    proxyOptions,
                    configureSocket);
        }
    }
}
