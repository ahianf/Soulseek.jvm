// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A bitwise combination of search states.
 */
public final class SearchStates {
    /** No search state. */
    public static final SearchStates NONE = new SearchStates(0);
    /** The search was requested. */
    public static final SearchStates REQUESTED = new SearchStates(1);
    /** The search is in progress. */
    public static final SearchStates IN_PROGRESS = new SearchStates(2);
    /** The search is complete. */
    public static final SearchStates COMPLETED = new SearchStates(4);
    /** The search completed due to cancellation. */
    public static final SearchStates CANCELLED = new SearchStates(8);
    /** The search completed due to its timeout. */
    public static final SearchStates TIMED_OUT = new SearchStates(16);
    /** The search completed after reaching its response limit. */
    public static final SearchStates RESPONSE_LIMIT_REACHED = new SearchStates(32);
    /** The search completed after reaching its file limit. */
    public static final SearchStates FILE_LIMIT_REACHED = new SearchStates(64);
    /** The search completed due to an error. */
    public static final SearchStates ERRORED = new SearchStates(128);
    /** The search is queued. */
    public static final SearchStates QUEUED = new SearchStates(256);

    private final int value;

    private SearchStates(int value) {
        this.value = value;
    }

    /**
     * Creates a state value from its bit mask.
     *
     * @param value the bit mask
     * @return the state value
     */
    public static SearchStates fromValue(int value) {
        return switch (value) {
            case 0 -> NONE;
            case 1 -> REQUESTED;
            case 2 -> IN_PROGRESS;
            case 4 -> COMPLETED;
            case 8 -> CANCELLED;
            case 16 -> TIMED_OUT;
            case 32 -> RESPONSE_LIMIT_REACHED;
            case 64 -> FILE_LIMIT_REACHED;
            case 128 -> ERRORED;
            case 256 -> QUEUED;
            default -> new SearchStates(value);
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
    public boolean hasFlag(SearchStates state) {
        Objects.requireNonNull(state, "state");
        return (value & state.value) == state.value;
    }

    /**
     * Combines this state with another state.
     *
     * @param state the state to combine
     * @return the combined state
     */
    public SearchStates or(SearchStates state) {
        Objects.requireNonNull(state, "state");
        return fromValue(value | state.value);
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof SearchStates states && value == states.value;
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
        addName(names, IN_PROGRESS, "IN_PROGRESS");
        addName(names, COMPLETED, "COMPLETED");
        addName(names, CANCELLED, "CANCELLED");
        addName(names, TIMED_OUT, "TIMED_OUT");
        addName(names, RESPONSE_LIMIT_REACHED, "RESPONSE_LIMIT_REACHED");
        addName(names, FILE_LIMIT_REACHED, "FILE_LIMIT_REACHED");
        addName(names, ERRORED, "ERRORED");
        addName(names, QUEUED, "QUEUED");
        return names.isEmpty() ? Integer.toString(value) : String.join(" | ", names);
    }

    private void addName(List<String> names, SearchStates state, String name) {
        if (hasFlag(state)) {
            names.add(name);
        }
    }
}
