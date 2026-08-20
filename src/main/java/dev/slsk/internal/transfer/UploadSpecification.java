// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.transfer;

import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.options.TransferOptions;
import java.io.InputStream;
import java.nio.channels.ReadableByteChannel;
import java.util.function.LongFunction;

/** Everything one upload needs, in one value. */
public record UploadSpecification(
        String username,
        String remoteFilename,
        String localFilename,
        TransferChannels.SourceFactory sourceFactory,
        long size,
        Integer token,
        TransferOptions options,
        CancellationSignal cancellationSignal,
        boolean fromChannel) {

    public UploadSpecification {
        cancellationSignal = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
    }

    private UploadSpecification(Builder builder) {
        this(
                builder.username,
                builder.remoteFilename,
                builder.localFilename,
                builder.sourceFactory,
                builder.size,
                builder.token,
                builder.options,
                builder.cancellationSignal,
                builder.fromChannel);
    }

    public static Builder fromFile(String username, String remoteFilename, String localFilename) {
        Builder builder = new Builder(username, remoteFilename);
        builder.localFilename = localFilename;
        return builder;
    }

    public static Builder fromStream(
            String username, String remoteFilename, long size, LongFunction<InputStream> inputStreamFactory) {
        Builder builder = new Builder(username, remoteFilename);
        builder.sourceFactory = TransferChannels.source(inputStreamFactory);
        builder.size = size;
        builder.fromChannel = true;
        return builder;
    }

    public static Builder fromChannel(
            String username,
            String remoteFilename,
            long size,
            LongFunction<? extends ReadableByteChannel> inputChannelFactory) {
        Builder builder = new Builder(username, remoteFilename);
        builder.sourceFactory = TransferChannels.sourceChannel(inputChannelFactory);
        builder.size = size;
        builder.fromChannel = true;
        return builder;
    }

    public static final class Builder {
        private final String username;
        private final String remoteFilename;
        private String localFilename;
        private TransferChannels.SourceFactory sourceFactory;
        private long size;
        private Integer token;
        private TransferOptions options;
        private CancellationSignal cancellationSignal = CancellationSignal.none();
        private boolean fromChannel;

        private Builder(String username, String remoteFilename) {
            this.username = username;
            this.remoteFilename = remoteFilename;
        }

        public Builder size(long size) {
            this.size = size;
            return this;
        }

        public Builder token(Integer token) {
            this.token = token;
            return this;
        }

        public Builder options(TransferOptions options) {
            this.options = options;
            return this;
        }

        public Builder cancellation(CancellationSignal cancellationSignal) {
            this.cancellationSignal = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
            return this;
        }

        public UploadSpecification build() {
            return new UploadSpecification(this);
        }
    }
}
