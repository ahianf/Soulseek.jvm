// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

import dev.slsk.internal.transfer.Transfer;
import java.time.Duration;
import java.util.function.Consumer;

/** Options for transfer operations. */
public record TransferOptions(
        Consumer<TransferStateChange> stateChanged,
        Consumer<TransferProgressUpdate> progressUpdated,
        Consumer<Transfer> slotReleased,
        TransferReporter reporter,
        Duration maximumLingerTime,
        boolean seekInputStreamAutomatically,
        boolean seekOutputStreamAutomatically,
        boolean disposeInputStreamOnCompletion,
        boolean disposeOutputStreamOnCompletion) {
    /** Default maximum connection linger time. */
    public static final Duration DEFAULT_MAXIMUM_LINGER_TIME = Duration.ofSeconds(3);

    /** Creates transfer options with defaults. */
    public TransferOptions() {
        this(null, null, null, null, DEFAULT_MAXIMUM_LINGER_TIME, true, true, true, true);
    }

    /** Starts a field-named transfer-options builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Starts a builder initialized from existing options. */
    public static Builder builder(TransferOptions source) {
        return new Builder(source);
    }

    /** Returns a copy whose state callback invokes {@code additionalStateChanged} first. */
    public TransferOptions withAdditionalStateChanged(Consumer<TransferStateChange> additionalStateChanged) {
        return builder(this)
                .stateChanged(change -> {
                    if (additionalStateChanged != null) {
                        additionalStateChanged.accept(change);
                    }
                    if (stateChanged != null) {
                        stateChanged.accept(change);
                    }
                })
                .build();
    }

    /** Returns a copy retaining both stream-disposal options. */
    public TransferOptions withDisposalOptions() {
        return this;
    }

    /** Returns a copy overriding input-stream disposal when non-null. */
    public TransferOptions withDisposalOptions(Boolean disposeInputStreamOnCompletion) {
        return withDisposalOptions(disposeInputStreamOnCompletion, null);
    }

    /** Returns a copy overriding the specified stream-disposal options. */
    public TransferOptions withDisposalOptions(
            Boolean disposeInputStreamOnCompletion, Boolean disposeOutputStreamOnCompletion) {
        return builder(this)
                .disposeInputStreamOnCompletion(
                        disposeInputStreamOnCompletion == null
                                ? this.disposeInputStreamOnCompletion
                                : disposeInputStreamOnCompletion)
                .disposeOutputStreamOnCompletion(
                        disposeOutputStreamOnCompletion == null
                                ? this.disposeOutputStreamOnCompletion
                                : disposeOutputStreamOnCompletion)
                .build();
    }

    /** Builder for transfer options. */
    public static final class Builder {
        private boolean disposeInputStreamOnCompletion = true;
        private boolean disposeOutputStreamOnCompletion = true;
        private Duration maximumLingerTime = DEFAULT_MAXIMUM_LINGER_TIME;
        private Consumer<TransferProgressUpdate> progressUpdated;
        private TransferReporter reporter;
        private boolean seekInputStreamAutomatically = true;
        private boolean seekOutputStreamAutomatically = true;
        private Consumer<Transfer> slotReleased;
        private Consumer<TransferStateChange> stateChanged;

        private Builder() {}

        private Builder(TransferOptions source) {
            stateChanged = source.stateChanged;
            progressUpdated = source.progressUpdated;
            slotReleased = source.slotReleased;
            reporter = source.reporter;
            maximumLingerTime = source.maximumLingerTime;
            seekInputStreamAutomatically = source.seekInputStreamAutomatically;
            seekOutputStreamAutomatically = source.seekOutputStreamAutomatically;
            disposeInputStreamOnCompletion = source.disposeInputStreamOnCompletion;
            disposeOutputStreamOnCompletion = source.disposeOutputStreamOnCompletion;
        }

        public Builder stateChanged(Consumer<TransferStateChange> value) {
            stateChanged = value;
            return this;
        }

        public Builder progressUpdated(Consumer<TransferProgressUpdate> value) {
            progressUpdated = value;
            return this;
        }

        public Builder slotReleased(Consumer<Transfer> value) {
            slotReleased = value;
            return this;
        }

        public Builder reporter(TransferReporter value) {
            reporter = value;
            return this;
        }

        public Builder maximumLingerTime(Duration value) {
            maximumLingerTime = value;
            return this;
        }

        public Builder seekInputStreamAutomatically(boolean value) {
            seekInputStreamAutomatically = value;
            return this;
        }

        public Builder seekOutputStreamAutomatically(boolean value) {
            seekOutputStreamAutomatically = value;
            return this;
        }

        public Builder disposeInputStreamOnCompletion(boolean value) {
            disposeInputStreamOnCompletion = value;
            return this;
        }

        public Builder disposeOutputStreamOnCompletion(boolean value) {
            disposeOutputStreamOnCompletion = value;
            return this;
        }

        public TransferOptions build() {
            return new TransferOptions(
                    stateChanged,
                    progressUpdated,
                    slotReleased,
                    reporter,
                    maximumLingerTime,
                    seekInputStreamAutomatically,
                    seekOutputStreamAutomatically,
                    disposeInputStreamOnCompletion,
                    disposeOutputStreamOnCompletion);
        }
    }
}
