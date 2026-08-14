// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.transfer;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A bitwise combination of transfer states.
 */
public final class TransferState {
    /** No transfer state. */
    public static final TransferState NONE = new TransferState(0);
    /** The transfer was requested. */
    public static final TransferState REQUESTED = new TransferState(1);
    /** The transfer is queued. */
    public static final TransferState QUEUED = new TransferState(2);
    /** The transfer is initializing. */
    public static final TransferState INITIALIZING = new TransferState(4);
    /** The transfer is in progress. */
    public static final TransferState IN_PROGRESS = new TransferState(8);
    /** The transfer is complete; another flag describes its disposition. */
    public static final TransferState COMPLETED = new TransferState(16);
    /** The transfer completed successfully. */
    public static final TransferState SUCCEEDED = new TransferState(32);
    /** The transfer completed due to cancellation. */
    public static final TransferState CANCELLED = new TransferState(64);
    /** The transfer completed due to a timeout. */
    public static final TransferState TIMED_OUT = new TransferState(128);
    /** The transfer completed due to an error. */
    public static final TransferState ERRORED = new TransferState(256);
    /** The transfer completed because the peer rejected it. */
    public static final TransferState REJECTED = new TransferState(512);
    /** The transfer completed due to unexpected circumstances. */
    public static final TransferState ABORTED = new TransferState(1024);
    /** The transfer is queued locally. */
    public static final TransferState LOCALLY = new TransferState(2048);
    /** The transfer is queued remotely. */
    public static final TransferState REMOTELY = new TransferState(4096);

    private final int value;

    private TransferState(int value) {
        this.value = value;
    }

    /**
     * Creates a state value from its bit mask.
     *
     * @param value the bit mask
     * @return the state value
     */
    public static TransferState fromValue(int value) {
        return switch (value) {
            case 0 -> NONE;
            case 1 -> REQUESTED;
            case 2 -> QUEUED;
            case 4 -> INITIALIZING;
            case 8 -> IN_PROGRESS;
            case 16 -> COMPLETED;
            case 32 -> SUCCEEDED;
            case 64 -> CANCELLED;
            case 128 -> TIMED_OUT;
            case 256 -> ERRORED;
            case 512 -> REJECTED;
            case 1024 -> ABORTED;
            case 2048 -> LOCALLY;
            case 4096 -> REMOTELY;
            default -> new TransferState(value);
        };
    }

    /**
     * Returns the bit mask.
     *
     * @return the bit mask
     */
    public int getValue() {
        return value;
    }

    /**
     * Returns whether all bits in {@code state} are present.
     *
     * @param state the state to test
     * @return {@code true} when all state bits are present
     */
    public boolean contains(TransferState state) {
        Objects.requireNonNull(state, "state");
        return (value & state.value) == state.value;
    }

    /**
     * Combines this state with another state.
     *
     * @param state the state to combine
     * @return the combined state
     */
    public TransferState or(TransferState state) {
        Objects.requireNonNull(state, "state");
        return fromValue(value | state.value);
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof TransferState states && value == states.value;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(value);
    }

    @Override
    public String toString() {
        if (value == 0) {
            return "NONE";
        }
        List<String> names = new ArrayList<>();
        addName(names, REQUESTED, "REQUESTED");
        addName(names, QUEUED, "QUEUED");
        addName(names, INITIALIZING, "INITIALIZING");
        addName(names, IN_PROGRESS, "IN_PROGRESS");
        addName(names, COMPLETED, "COMPLETED");
        addName(names, SUCCEEDED, "SUCCEEDED");
        addName(names, CANCELLED, "CANCELLED");
        addName(names, TIMED_OUT, "TIMED_OUT");
        addName(names, ERRORED, "ERRORED");
        addName(names, REJECTED, "REJECTED");
        addName(names, ABORTED, "ABORTED");
        addName(names, LOCALLY, "LOCALLY");
        addName(names, REMOTELY, "REMOTELY");
        return names.isEmpty() ? Integer.toString(value) : String.join(" | ", names);
    }

    private void addName(List<String> names, TransferState state, String name) {
        if (contains(state)) {
            names.add(name);
        }
    }
}
