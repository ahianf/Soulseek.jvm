// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import dev.slsk.spi.TransferSink;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A file to fetch from a peer.
 *
 * @param user who to fetch it from
 * @param path the file on the peer, in Soulseek's backslash-joined form
 * @param sink where the bytes go, and what makes the result visible
 * @param expectedSize the size the peer advertised, or {@code 0} if unknown
 * @param priority its place in our own queue
 * @param tags whatever the application wants handed back on every event
 */
public record DownloadRequest(
        Username user, String path, TransferSink sink, long expectedSize, Priority priority, Map<String, String> tags) {

    /** Validates and returns the request. */
    public DownloadRequest {
        Objects.requireNonNull(user, "user");
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(sink, "sink");
        Objects.requireNonNull(priority, "priority");
        tags = Map.copyOf(Objects.requireNonNull(tags, "tags"));
        if (path.isBlank()) {
            throw new IllegalArgumentException("path must not be blank");
        }
        if (expectedSize < 0) {
            throw new IllegalArgumentException("expectedSize must not be negative: " + expectedSize);
        }
    }

    /**
     * Returns a request with default priority and no tags.
     *
     * @param user who to fetch from
     * @param path the file on the peer
     * @param destination where to write it
     * @return the request
     */
    public static DownloadRequest of(Username user, String path, Path destination) {
        return of(user, path, TransferSink.file(destination));
    }

    /**
     * Returns a request writing through a sink of your own.
     *
     * @param user who to fetch from
     * @param path the file on the peer
     * @param sink where the bytes go
     * @return the request
     */
    public static DownloadRequest of(Username user, String path, TransferSink sink) {
        return new DownloadRequest(user, path, sink, 0, Priority.NORMAL, Map.of());
    }

    /**
     * Returns a request for a file found by a search.
     *
     * @param user who offered it
     * @param file the file they offered
     * @param destination where to write it
     * @return the request, carrying the size the peer advertised
     */
    public static DownloadRequest of(Username user, SearchFile file, Path destination) {
        return of(user, file, TransferSink.file(destination));
    }

    /**
     * Returns a request for a file found by a search, writing through a sink of
     * your own.
     *
     * @param user who offered it
     * @param file the file they offered
     * @param sink where the bytes go
     * @return the request, carrying the size the peer advertised
     */
    public static DownloadRequest of(Username user, SearchFile file, TransferSink sink) {
        Objects.requireNonNull(file, "file");
        return new DownloadRequest(user, file.path(), sink, file.size(), Priority.NORMAL, Map.of());
    }

    /**
     * Returns a builder.
     *
     * @param user who to fetch from
     * @param path the file on the peer
     * @param destination where to write it
     * @return a builder
     */
    public static Builder builder(Username user, String path, Path destination) {
        return new Builder(user, path, TransferSink.file(destination));
    }

    /**
     * Returns a builder writing through a sink of your own.
     *
     * @param user who to fetch from
     * @param path the file on the peer
     * @param sink where the bytes go
     * @return a builder
     */
    public static Builder builder(Username user, String path, TransferSink sink) {
        return new Builder(user, path, sink);
    }

    /** Builds a {@link DownloadRequest}. */
    public static final class Builder {
        private final Username user;
        private final String path;
        private final TransferSink sink;
        private long expectedSize;
        private Priority priority = Priority.NORMAL;
        private final Map<String, String> tags = new LinkedHashMap<>();

        private Builder(Username user, String path, TransferSink sink) {
            this.user = Objects.requireNonNull(user, "user");
            this.path = Objects.requireNonNull(path, "path");
            this.sink = Objects.requireNonNull(sink, "sink");
        }

        /**
         * Sets the size the peer advertised.
         *
         * @param value the size in bytes
         * @return this builder
         */
        public Builder expectedSize(long value) {
            this.expectedSize = value;
            return this;
        }

        /**
         * Sets the queue priority.
         *
         * @param value the priority
         * @return this builder
         */
        public Builder priority(Priority value) {
            this.priority = Objects.requireNonNull(value, "priority");
            return this;
        }

        /**
         * Attaches application data, handed back on every event for this
         * transfer.
         *
         * @param key the key
         * @param value the value
         * @return this builder
         */
        public Builder tag(String key, String value) {
            tags.put(Objects.requireNonNull(key, "key"), Objects.requireNonNull(value, "value"));
            return this;
        }

        /**
         * Builds the request.
         *
         * @return the request
         */
        public DownloadRequest build() {
            return new DownloadRequest(user, path, sink, expectedSize, priority, Map.copyOf(tags));
        }
    }
}
