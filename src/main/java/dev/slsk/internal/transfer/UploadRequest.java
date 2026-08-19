// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.transfer;

import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.options.TransferOptions;
import java.io.InputStream;
import java.util.Objects;
import java.util.function.LongFunction;

/**
 * Everything one upload needs, in one value.
 *
 * <p>The counterpart to {@link DownloadRequest}; see that class for why the
 * overload cross product was collapsed.
 *
 * {@snippet :
 * Transfer sent = client.upload(
 *         UploadRequest.fromFile("bob", "Music\\track.flac", "/srv/share/track.flac")
 *                 .token(9_001)
 *                 .build());
 * }
 */
public final class UploadRequest {

    private final String username;
    private final String remoteFilename;
    private final String localFilename;
    private final LongFunction<InputStream> inputStreamFactory;
    private final long size;
    private final Integer token;
    private final TransferOptions options;
    private final CancellationSignal cancellationSignal;
    private final boolean fromStream;

    private UploadRequest(Builder builder) {
        this.username = builder.username;
        this.remoteFilename = builder.remoteFilename;
        this.localFilename = builder.localFilename;
        this.inputStreamFactory = builder.inputStreamFactory;
        this.size = builder.size;
        this.token = builder.token;
        this.options = builder.options;
        this.cancellationSignal = builder.cancellationSignal;
        this.fromStream = builder.fromStream;
    }

    /**
     * Starts a request that reads from a local path.
     *
     * @param username the peer to upload to
     * @param remoteFilename the name to present to the peer
     * @param localFilename the local path to read
     * @return a builder
     */
    public static Builder fromFile(String username, String remoteFilename, String localFilename) {
        Builder builder = new Builder(username, remoteFilename);
        builder.localFilename = localFilename;
        return builder;
    }

    /**
     * Starts a request that reads from a caller-supplied stream.
     *
     * @param username the peer to upload to
     * @param remoteFilename the name to present to the peer
     * @param size the number of bytes to send
     * @param inputStreamFactory supplies the source stream
     * @return a builder
     */
    public static Builder fromStream(
            String username, String remoteFilename, long size, LongFunction<InputStream> inputStreamFactory) {
        Builder builder = new Builder(username, remoteFilename);
        builder.inputStreamFactory = Objects.requireNonNull(inputStreamFactory, "inputStreamFactory");
        builder.size = size;
        builder.fromStream = true;
        return builder;
    }

    /** Returns the peer to upload to. */
    public String getUsername() {
        return username;
    }

    /** Returns the name presented to the peer. */
    public String getRemoteFilename() {
        return remoteFilename;
    }

    /** Returns the local source path, or {@code null} when streaming. */
    public String getLocalFilename() {
        return localFilename;
    }

    /** Returns the source stream factory, or {@code null} when reading a file. */
    public LongFunction<InputStream> getInputStreamFactory() {
        return inputStreamFactory;
    }

    /** Returns the number of bytes to send. */
    public long getSize() {
        return size;
    }

    /** Returns the caller-chosen token, or {@code null} to allocate one. */
    public Integer getToken() {
        return token;
    }

    /** Returns the transfer options, or {@code null} for defaults. */
    public TransferOptions getOptions() {
        return options;
    }

    /** Returns whether this request reads from a stream rather than a file. */
    public boolean isFromStream() {
        return fromStream;
    }

    /** Returns the cancellation signal; never {@code null}. */
    public CancellationSignal getCancellationSignal() {
        return cancellationSignal;
    }

    /** Builds an {@link UploadRequest}. */
    public static final class Builder {
        private final String username;
        private final String remoteFilename;
        private String localFilename;
        private LongFunction<InputStream> inputStreamFactory;
        private long size;
        private Integer token;
        private TransferOptions options;
        private CancellationSignal cancellationSignal = CancellationSignal.none();
        private boolean fromStream;

        // Names are not validated here: the client rejects null, empty and
        // whitespace-only values with IllegalArgumentException, and the builder
        // must not change that contract by throwing NPE first.
        private Builder(String username, String remoteFilename) {
            this.username = username;
            this.remoteFilename = remoteFilename;
        }

        /** Sets the number of bytes to send. */
        public Builder size(long size) {
            this.size = size;
            return this;
        }

        /** Sets the transfer token instead of allocating one. */
        public Builder token(Integer token) {
            this.token = token;
            return this;
        }

        /** Sets the transfer options. */
        public Builder options(TransferOptions options) {
            this.options = options;
            return this;
        }

        /** Sets the cancellation signal. */
        public Builder cancellation(CancellationSignal cancellationSignal) {
            this.cancellationSignal = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
            return this;
        }

        /** Builds the request. */
        public UploadRequest build() {
            return new UploadRequest(this);
        }
    }
}
