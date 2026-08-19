// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.transfer;

import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.options.TransferOptions;
import java.io.OutputStream;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Everything one download needs, in one value.
 *
 * <p>The ported API expressed the optional parameters as fourteen overloads —
 * the cross product of two destination kinds and five optional arguments. That
 * shape comes from C# optional parameters, which Java does not have; in Java it
 * reads as fourteen unrelated methods and forces callers to pass {@code null}
 * for the ones they do not care about.
 *
 * <p>Build one of these instead:
 *
 * {@snippet :
 * Transfer file = client.download(
 *         DownloadRequest.toFile("alice", "Music\\track.flac", "/tmp/track.flac")
 *                 .size(4_812_003L)
 *                 .startOffset(0)
 *                 .build());
 * }
 *
 * <p>The builder is sugar over this value, not a parallel API: every path
 * through it produces a {@code DownloadRequest} and calls the same method.
 */
public final class DownloadRequest {

    private final String username;
    private final String remoteFilename;
    private final String localFilename;
    private final Supplier<OutputStream> outputStreamFactory;
    private final Long size;
    private final long startOffset;
    private final Integer token;
    private final TransferOptions options;
    private final CancellationSignal cancellationSignal;
    private final boolean toStream;
    private final dev.slsk.internal.messaging.messages.TransferRequest offer;

    private DownloadRequest(Builder builder) {
        this.username = builder.username;
        this.remoteFilename = builder.remoteFilename;
        this.localFilename = builder.localFilename;
        this.outputStreamFactory = builder.outputStreamFactory;
        this.size = builder.size;
        this.startOffset = builder.startOffset;
        this.token = builder.token;
        this.options = builder.options;
        this.cancellationSignal = builder.cancellationSignal;
        this.toStream = builder.toStream;
        this.offer = builder.offer;
    }

    /**
     * Starts a request that writes to a local path.
     *
     * @param username the peer to download from
     * @param remoteFilename the peer's path to the file
     * @param localFilename the local path to write
     * @return a builder
     */
    public static Builder toFile(String username, String remoteFilename, String localFilename) {
        Builder builder = new Builder(username, remoteFilename);
        builder.localFilename = localFilename;
        return builder;
    }

    /**
     * Starts a request that writes to a caller-supplied stream.
     *
     * @param username the peer to download from
     * @param remoteFilename the peer's path to the file
     * @param outputStreamFactory supplies the destination stream
     * @return a builder
     */
    public static Builder toStream(String username, String remoteFilename, Supplier<OutputStream> outputStreamFactory) {
        Builder builder = new Builder(username, remoteFilename);
        builder.outputStreamFactory = Objects.requireNonNull(outputStreamFactory, "outputStreamFactory");
        builder.toStream = true;
        return builder;
    }

    /** Returns the peer to download from. */
    public String getUsername() {
        return username;
    }

    /** Returns the peer's path to the file. */
    public String getRemoteFilename() {
        return remoteFilename;
    }

    /** Returns the local destination path, or {@code null} when streaming. */
    public String getLocalFilename() {
        return localFilename;
    }

    /** Returns the destination stream factory, or {@code null} when writing a file. */
    public Supplier<OutputStream> getOutputStreamFactory() {
        return outputStreamFactory;
    }

    /** Returns the expected size, or {@code null} to take the peer's word for it. */
    public Long getSize() {
        return size;
    }

    /** Returns the offset to resume from. */
    public long getStartOffset() {
        return startOffset;
    }

    /** Returns the caller-chosen token, or {@code null} to allocate one. */
    public Integer getToken() {
        return token;
    }

    /** Returns the transfer options, or {@code null} for defaults. */
    public TransferOptions getOptions() {
        return options;
    }

    /** Returns whether this request writes to a stream rather than a file. */
    public boolean isToStream() {
        return toStream;
    }

    /**
     * Returns a peer's standing offer of this file, or {@code null}.
     *
     * <p>Set when the download exists because the peer said it was ready, in
     * which case its token and size are already known and asking for the file
     * again would only put us back in the queue we just reached the front of.
     *
     * @return the offer, or {@code null} for an ordinary download
     */
    public dev.slsk.internal.messaging.messages.TransferRequest getOffer() {
        return offer;
    }

    /** Returns the cancellation signal; never {@code null}. */
    public CancellationSignal getCancellationSignal() {
        return cancellationSignal;
    }

    /** Builds a {@link DownloadRequest}. */
    public static final class Builder {
        private final String username;
        private final String remoteFilename;
        private String localFilename;
        private Supplier<OutputStream> outputStreamFactory;
        private Long size;
        private long startOffset;
        private Integer token;
        private TransferOptions options;
        private CancellationSignal cancellationSignal = CancellationSignal.none();
        private boolean toStream;
        private dev.slsk.internal.messaging.messages.TransferRequest offer;

        // Names are not validated here: the client rejects null, empty and
        // whitespace-only values with IllegalArgumentException, and the builder
        // must not change that contract by throwing NPE first.
        private Builder(String username, String remoteFilename) {
            this.username = username;
            this.remoteFilename = remoteFilename;
        }

        /** Sets the expected size in bytes. */
        public Builder size(Long size) {
            this.size = size;
            return this;
        }

        /** Sets the offset to resume from. */
        public Builder startOffset(long startOffset) {
            this.startOffset = startOffset;
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

        /** Sets a peer's standing offer of the file, if there is one. */
        public Builder offer(dev.slsk.internal.messaging.messages.TransferRequest offer) {
            this.offer = offer;
            return this;
        }

        /** Sets the cancellation signal. */
        public Builder cancellation(CancellationSignal cancellationSignal) {
            this.cancellationSignal = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
            return this;
        }

        /** Builds the request. */
        public DownloadRequest build() {
            return new DownloadRequest(this);
        }
    }
}
