// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A bitwise combination of transfer states.
 */
public final class TransferStates {
    /** No transfer state. */
    public static final TransferStates NONE = new TransferStates(0);
    /** The transfer was requested. */
    public static final TransferStates REQUESTED = new TransferStates(1);
    /** The transfer is queued. */
    public static final TransferStates QUEUED = new TransferStates(2);
    /** The transfer is initializing. */
    public static final TransferStates INITIALIZING = new TransferStates(4);
    /** The transfer is in progress. */
    public static final TransferStates IN_PROGRESS = new TransferStates(8);
    /** The transfer is complete; another flag describes its disposition. */
    public static final TransferStates COMPLETED = new TransferStates(16);
    /** The transfer completed successfully. */
    public static final TransferStates SUCCEEDED = new TransferStates(32);
    /** The transfer completed due to cancellation. */
    public static final TransferStates CANCELLED = new TransferStates(64);
    /** The transfer completed due to a timeout. */
    public static final TransferStates TIMED_OUT = new TransferStates(128);
    /** The transfer completed due to an error. */
    public static final TransferStates ERRORED = new TransferStates(256);
    /** The transfer completed because the peer rejected it. */
    public static final TransferStates REJECTED = new TransferStates(512);
    /** The transfer completed due to unexpected circumstances. */
    public static final TransferStates ABORTED = new TransferStates(1024);
    /** The transfer is queued locally. */
    public static final TransferStates LOCALLY = new TransferStates(2048);
    /** The transfer is queued remotely. */
    public static final TransferStates REMOTELY = new TransferStates(4096);

    private final int value;

    private TransferStates(int value) {
        this.value = value;
    }

    /**
     * Creates a state value from its bit mask.
     *
     * @param value the bit mask
     * @return the state value
     */
    public static TransferStates fromValue(int value) {
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
            default -> new TransferStates(value);
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
    public boolean hasFlag(TransferStates state) {
        Objects.requireNonNull(state, "state");
        return (value & state.value) == state.value;
    }

    /**
     * Combines this state with another state.
     *
     * @param state the state to combine
     * @return the combined state
     */
    public TransferStates or(TransferStates state) {
        Objects.requireNonNull(state, "state");
        return fromValue(value | state.value);
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof TransferStates states && value == states.value;
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

    private void addName(List<String> names, TransferStates state, String name) {
        if (hasFlag(state)) {
            names.add(name);
        }
    }
}
