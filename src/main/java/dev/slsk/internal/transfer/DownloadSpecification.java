// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.transfer;

import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.options.TransferOptions;
import java.io.OutputStream;
import java.nio.channels.WritableByteChannel;
import java.util.function.LongFunction;
import java.util.function.Supplier;

/** Everything one download needs, in one value. */
public record DownloadSpecification(
        String username,
        String remoteFilename,
        String localFilename,
        TransferChannels.DestinationFactory destinationFactory,
        Long size,
        long startOffset,
        Integer token,
        TransferOptions options,
        CancellationSignal cancellationSignal,
        boolean toChannel,
        dev.slsk.internal.messaging.messages.TransferRequest offer) {

    public DownloadSpecification {
        cancellationSignal = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
    }

    private DownloadSpecification(Builder builder) {
        this(
                builder.username,
                builder.remoteFilename,
                builder.localFilename,
                builder.destinationFactory,
                builder.size,
                builder.startOffset,
                builder.token,
                builder.options,
                builder.cancellationSignal,
                builder.toChannel,
                builder.offer);
    }

    public static Builder toFile(String username, String remoteFilename, String localFilename) {
        Builder builder = new Builder(username, remoteFilename);
        builder.localFilename = localFilename;
        return builder;
    }

    public static Builder toStream(String username, String remoteFilename, Supplier<OutputStream> outputStreamFactory) {
        Builder builder = new Builder(username, remoteFilename);
        builder.destinationFactory = TransferChannels.destination(outputStreamFactory);
        builder.toChannel = true;
        return builder;
    }

    public static Builder toChannel(
            String username, String remoteFilename, LongFunction<? extends WritableByteChannel> outputChannelFactory) {
        Builder builder = new Builder(username, remoteFilename);
        builder.destinationFactory = TransferChannels.destination(outputChannelFactory);
        builder.toChannel = true;
        return builder;
    }

    public static final class Builder {
        private final String username;
        private final String remoteFilename;
        private String localFilename;
        private TransferChannels.DestinationFactory destinationFactory;
        private Long size;
        private long startOffset;
        private Integer token;
        private TransferOptions options;
        private CancellationSignal cancellationSignal = CancellationSignal.none();
        private boolean toChannel;
        private dev.slsk.internal.messaging.messages.TransferRequest offer;

        private Builder(String username, String remoteFilename) {
            this.username = username;
            this.remoteFilename = remoteFilename;
        }

        public Builder size(Long size) {
            this.size = size;
            return this;
        }

        public Builder startOffset(long startOffset) {
            this.startOffset = startOffset;
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

        public Builder offer(dev.slsk.internal.messaging.messages.TransferRequest offer) {
            this.offer = offer;
            return this;
        }

        public Builder cancellation(CancellationSignal cancellationSignal) {
            this.cancellationSignal = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
            return this;
        }

        public DownloadSpecification build() {
            return new DownloadSpecification(this);
        }
    }
}
