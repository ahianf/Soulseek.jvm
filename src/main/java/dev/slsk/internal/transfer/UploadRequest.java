// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.transfer;

import dev.slsk.internal.concurrent.CancellationSignal;
import dev.slsk.internal.options.TransferOptions;
import java.io.InputStream;
import java.util.Objects;
import java.util.function.LongFunction;

/** Everything one upload needs, in one value. */
public record UploadRequest(
        String username,
        String remoteFilename,
        String localFilename,
        LongFunction<InputStream> inputStreamFactory,
        long size,
        Integer token,
        TransferOptions options,
        CancellationSignal cancellationSignal,
        boolean fromStream) {

    public UploadRequest {
        cancellationSignal = cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
    }

    private UploadRequest(Builder builder) {
        this(
                builder.username,
                builder.remoteFilename,
                builder.localFilename,
                builder.inputStreamFactory,
                builder.size,
                builder.token,
                builder.options,
                builder.cancellationSignal,
                builder.fromStream);
    }

    public static Builder fromFile(String username, String remoteFilename, String localFilename) {
        Builder builder = new Builder(username, remoteFilename);
        builder.localFilename = localFilename;
        return builder;
    }

    public static Builder fromStream(
            String username, String remoteFilename, long size, LongFunction<InputStream> inputStreamFactory) {
        Builder builder = new Builder(username, remoteFilename);
        builder.inputStreamFactory = Objects.requireNonNull(inputStreamFactory, "inputStreamFactory");
        builder.size = size;
        builder.fromStream = true;
        return builder;
    }

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

        public UploadRequest build() {
            return new UploadRequest(this);
        }
    }
}
