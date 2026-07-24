// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A bitwise combination of Soulseek client states.
 */
public final class SoulseekClientState {
    /** No client state. */
    public static final SoulseekClientState NONE = new SoulseekClientState(0);
    /** The client is disconnected. */
    public static final SoulseekClientState DISCONNECTED = new SoulseekClientState(1);
    /** The client is connected. */
    public static final SoulseekClientState CONNECTED = new SoulseekClientState(2);
    /** The client is logged in. */
    public static final SoulseekClientState LOGGED_IN = new SoulseekClientState(4);
    /** The client is connecting. */
    public static final SoulseekClientState CONNECTING = new SoulseekClientState(8);
    /** The client is logging in. */
    public static final SoulseekClientState LOGGING_IN = new SoulseekClientState(16);
    /** The client is disconnecting. */
    public static final SoulseekClientState DISCONNECTING = new SoulseekClientState(32);

    private final int value;

    private SoulseekClientState(int value) {
        this.value = value;
    }

    /**
     * Creates a state value from its bit mask.
     *
     * @param value the bit mask
     * @return the state value
     */
    public static SoulseekClientState fromValue(int value) {
        return switch (value) {
            case 0 -> NONE;
            case 1 -> DISCONNECTED;
            case 2 -> CONNECTED;
            case 4 -> LOGGED_IN;
            case 8 -> CONNECTING;
            case 16 -> LOGGING_IN;
            case 32 -> DISCONNECTING;
            default -> new SoulseekClientState(value);
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
    public boolean contains(SoulseekClientState state) {
        Objects.requireNonNull(state, "state");
        return (value & state.value) == state.value;
    }

    /**
     * Combines this state with another state.
     *
     * @param state the state to combine
     * @return the combined state
     */
    public SoulseekClientState or(SoulseekClientState state) {
        Objects.requireNonNull(state, "state");
        return fromValue(value | state.value);
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof SoulseekClientState states && value == states.value;
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
        addName(names, DISCONNECTED, "DISCONNECTED");
        addName(names, CONNECTED, "CONNECTED");
        addName(names, LOGGED_IN, "LOGGED_IN");
        addName(names, CONNECTING, "CONNECTING");
        addName(names, LOGGING_IN, "LOGGING_IN");
        addName(names, DISCONNECTING, "DISCONNECTING");
        return names.isEmpty() ? Integer.toString(value) : String.join(" | ", names);
    }

    private void addName(List<String> names, SoulseekClientState state, String name) {
        if (contains(state)) {
            names.add(name);
        }
    }
}
