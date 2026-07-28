// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A bitwise combination of search states.
 */
public final class SearchState {
    /** No search state. */
    public static final SearchState NONE = new SearchState(0);
    /** The search was requested. */
    public static final SearchState REQUESTED = new SearchState(1);
    /** The search is in progress. */
    public static final SearchState IN_PROGRESS = new SearchState(2);
    /** The search is complete. */
    public static final SearchState COMPLETED = new SearchState(4);
    /** The search completed due to cancellation. */
    public static final SearchState CANCELLED = new SearchState(8);
    /** The search completed due to its timeout. */
    public static final SearchState TIMED_OUT = new SearchState(16);
    /** The search completed after reaching its response limit. */
    public static final SearchState RESPONSE_LIMIT_REACHED = new SearchState(32);
    /** The search completed after reaching its file limit. */
    public static final SearchState FILE_LIMIT_REACHED = new SearchState(64);
    /** The search completed due to an error. */
    public static final SearchState ERRORED = new SearchState(128);
    /** The search is queued. */
    public static final SearchState QUEUED = new SearchState(256);

    private final int value;

    private SearchState(int value) {
        this.value = value;
    }

    /**
     * Creates a state value from its bit mask.
     *
     * @param value the bit mask
     * @return the state value
     */
    public static SearchState fromValue(int value) {
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
            default -> new SearchState(value);
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
    public boolean contains(SearchState state) {
        Objects.requireNonNull(state, "state");
        return (value & state.value) == state.value;
    }

    /**
     * Combines this state with another state.
     *
     * @param state the state to combine
     * @return the combined state
     */
    public SearchState or(SearchState state) {
        Objects.requireNonNull(state, "state");
        return fromValue(value | state.value);
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof SearchState states && value == states.value;
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

    private void addName(List<String> names, SearchState state, String name) {
        if (contains(state)) {
            names.add(name);
        }
    }
}
