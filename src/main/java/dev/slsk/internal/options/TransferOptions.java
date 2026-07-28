// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.options;

/**
 * Options for transfer operations.
 */
public class TransferOptions {
    /** Default maximum connection linger time in milliseconds. */
    public static final int DEFAULT_MAXIMUM_LINGER_TIME = 3_000;

    private final boolean disposeInputStreamOnCompletion;
    private final boolean disposeOutputStreamOnCompletion;
    private final int maximumLingerTime;
    private final TransferProgressUpdatedCallback progressUpdated;
    private final TransferReporter reporter;
    private final boolean seekInputStreamAutomatically;
    private final boolean seekOutputStreamAutomatically;
    private final TransferSlotReleasedCallback slotReleased;
    private final TransferStateChangedCallback stateChanged;

    /**
     * Creates transfer options with source defaults.
     */
    public TransferOptions() {
        this(null, null, null, null);
    }

    /**
     * Creates transfer options through the state callback.
     */
    public TransferOptions(TransferStateChangedCallback stateChanged) {
        this(stateChanged, null, null, null);
    }

    /**
     * Creates transfer options through the progress callback.
     */
    public TransferOptions(TransferStateChangedCallback stateChanged, TransferProgressUpdatedCallback progressUpdated) {
        this(stateChanged, progressUpdated, null, null);
    }

    /**
     * Creates transfer options through the slot-release callback.
     */
    public TransferOptions(
            TransferStateChangedCallback stateChanged,
            TransferProgressUpdatedCallback progressUpdated,
            TransferSlotReleasedCallback slotReleased) {
        this(stateChanged, progressUpdated, slotReleased, null);
    }

    /**
     * Creates transfer options through the reporter.
     */
    public TransferOptions(
            TransferStateChangedCallback stateChanged,
            TransferProgressUpdatedCallback progressUpdated,
            TransferSlotReleasedCallback slotReleased,
            TransferReporter reporter) {
        this(stateChanged, progressUpdated, slotReleased, reporter, DEFAULT_MAXIMUM_LINGER_TIME);
    }

    /**
     * Creates transfer options through the maximum linger time.
     */
    public TransferOptions(
            TransferStateChangedCallback stateChanged,
            TransferProgressUpdatedCallback progressUpdated,
            TransferSlotReleasedCallback slotReleased,
            TransferReporter reporter,
            int maximumLingerTime) {
        this(stateChanged, progressUpdated, slotReleased, reporter, maximumLingerTime, true);
    }

    /**
     * Creates transfer options through automatic input seeking.
     */
    public TransferOptions(
            TransferStateChangedCallback stateChanged,
            TransferProgressUpdatedCallback progressUpdated,
            TransferSlotReleasedCallback slotReleased,
            TransferReporter reporter,
            int maximumLingerTime,
            boolean seekInputStreamAutomatically) {
        this(
                stateChanged,
                progressUpdated,
                slotReleased,
                reporter,
                maximumLingerTime,
                seekInputStreamAutomatically,
                true);
    }

    /**
     * Creates transfer options through automatic output seeking.
     */
    public TransferOptions(
            TransferStateChangedCallback stateChanged,
            TransferProgressUpdatedCallback progressUpdated,
            TransferSlotReleasedCallback slotReleased,
            TransferReporter reporter,
            int maximumLingerTime,
            boolean seekInputStreamAutomatically,
            boolean seekOutputStreamAutomatically) {
        this(
                stateChanged,
                progressUpdated,
                slotReleased,
                reporter,
                maximumLingerTime,
                seekInputStreamAutomatically,
                seekOutputStreamAutomatically,
                true);
    }

    /**
     * Creates transfer options through input-stream disposal.
     */
    public TransferOptions(
            TransferStateChangedCallback stateChanged,
            TransferProgressUpdatedCallback progressUpdated,
            TransferSlotReleasedCallback slotReleased,
            TransferReporter reporter,
            int maximumLingerTime,
            boolean seekInputStreamAutomatically,
            boolean seekOutputStreamAutomatically,
            boolean disposeInputStreamOnCompletion) {
        this(
                stateChanged,
                progressUpdated,
                slotReleased,
                reporter,
                maximumLingerTime,
                seekInputStreamAutomatically,
                seekOutputStreamAutomatically,
                disposeInputStreamOnCompletion,
                true);
    }

    /**
     * Creates transfer options.
     */
    public TransferOptions(
            TransferStateChangedCallback stateChanged,
            TransferProgressUpdatedCallback progressUpdated,
            TransferSlotReleasedCallback slotReleased,
            TransferReporter reporter,
            int maximumLingerTime,
            boolean seekInputStreamAutomatically,
            boolean seekOutputStreamAutomatically,
            boolean disposeInputStreamOnCompletion,
            boolean disposeOutputStreamOnCompletion) {
        this.stateChanged = stateChanged;
        this.progressUpdated = progressUpdated;
        this.slotReleased = slotReleased;
        this.reporter = reporter;
        this.maximumLingerTime = maximumLingerTime;
        this.seekInputStreamAutomatically = seekInputStreamAutomatically;
        this.seekOutputStreamAutomatically = seekOutputStreamAutomatically;
        this.disposeInputStreamOnCompletion = disposeInputStreamOnCompletion;
        this.disposeOutputStreamOnCompletion = disposeOutputStreamOnCompletion;
    }

    /**
     * Returns whether input streams are closed on completion.
     */
    public final boolean isDisposeInputStreamOnCompletion() {
        return disposeInputStreamOnCompletion;
    }

    /**
     * Returns whether output streams are closed on completion.
     */
    public final boolean isDisposeOutputStreamOnCompletion() {
        return disposeOutputStreamOnCompletion;
    }

    /**
     * Returns the maximum connection linger time in milliseconds.
     */
    public final int getMaximumLingerTime() {
        return maximumLingerTime;
    }

    /**
     * Returns the progress callback, or {@code null}.
     */
    public final TransferProgressUpdatedCallback getProgressUpdated() {
        return progressUpdated;
    }

    /**
     * Returns the statistics reporter, or {@code null}.
     */
    public final TransferReporter getReporter() {
        return reporter;
    }

    /**
     * Returns whether input streams are automatically positioned.
     */
    public final boolean isSeekInputStreamAutomatically() {
        return seekInputStreamAutomatically;
    }

    /**
     * Returns whether output streams are automatically positioned.
     */
    public final boolean isSeekOutputStreamAutomatically() {
        return seekOutputStreamAutomatically;
    }

    /**
     * Returns the slot-release callback, or {@code null}.
     */
    public final TransferSlotReleasedCallback getSlotReleased() {
        return slotReleased;
    }

    /**
     * Returns the state-change callback, or {@code null}.
     */
    public final TransferStateChangedCallback getStateChanged() {
        return stateChanged;
    }

    /**
     * Returns a copy whose state callback invokes {@code stateChanged} first.
     *
     * @param additionalStateChanged the new callback, or {@code null}
     * @return the copied options
     */
    public TransferOptions withAdditionalStateChanged(TransferStateChangedCallback additionalStateChanged) {
        return new TransferOptions(
                change -> {
                    if (additionalStateChanged != null) {
                        additionalStateChanged.onStateChanged(change);
                    }
                    if (stateChanged != null) {
                        stateChanged.onStateChanged(change);
                    }
                },
                progressUpdated,
                slotReleased,
                reporter,
                maximumLingerTime,
                seekInputStreamAutomatically,
                seekOutputStreamAutomatically,
                disposeInputStreamOnCompletion,
                disposeOutputStreamOnCompletion);
    }

    /**
     * Returns a copy retaining both stream-disposal options.
     *
     * @return the copied options
     */
    public TransferOptions withDisposalOptions() {
        return withDisposalOptions(null, null);
    }

    /**
     * Returns a copy overriding input-stream disposal when non-null.
     *
     * @param disposeInputStreamOnCompletion the input disposal override
     * @return the copied options
     */
    public TransferOptions withDisposalOptions(Boolean disposeInputStreamOnCompletion) {
        return withDisposalOptions(disposeInputStreamOnCompletion, null);
    }

    /**
     * Returns a copy overriding the specified stream-disposal options.
     *
     * @param disposeInputStreamOnCompletion the input disposal override
     * @param disposeOutputStreamOnCompletion the output disposal override
     * @return the copied options
     */
    public TransferOptions withDisposalOptions(
            Boolean disposeInputStreamOnCompletion, Boolean disposeOutputStreamOnCompletion) {
        return new TransferOptions(
                stateChanged,
                progressUpdated,
                slotReleased,
                reporter,
                maximumLingerTime,
                seekInputStreamAutomatically,
                seekOutputStreamAutomatically,
                disposeInputStreamOnCompletion == null
                        ? this.disposeInputStreamOnCompletion
                        : disposeInputStreamOnCompletion,
                disposeOutputStreamOnCompletion == null
                        ? this.disposeOutputStreamOnCompletion
                        : disposeOutputStreamOnCompletion);
    }
}
