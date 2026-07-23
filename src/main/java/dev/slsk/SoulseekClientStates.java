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
public final class SoulseekClientStates {
    /** No client state. */
    public static final SoulseekClientStates NONE = new SoulseekClientStates(0);
    /** The client is disconnected. */
    public static final SoulseekClientStates DISCONNECTED = new SoulseekClientStates(1);
    /** The client is connected. */
    public static final SoulseekClientStates CONNECTED = new SoulseekClientStates(2);
    /** The client is logged in. */
    public static final SoulseekClientStates LOGGED_IN = new SoulseekClientStates(4);
    /** The client is connecting. */
    public static final SoulseekClientStates CONNECTING = new SoulseekClientStates(8);
    /** The client is logging in. */
    public static final SoulseekClientStates LOGGING_IN = new SoulseekClientStates(16);
    /** The client is disconnecting. */
    public static final SoulseekClientStates DISCONNECTING = new SoulseekClientStates(32);

    private final int value;

    private SoulseekClientStates(int value) {
        this.value = value;
    }

    /**
     * Creates a state value from its bit mask.
     *
     * @param value the bit mask
     * @return the state value
     */
    public static SoulseekClientStates fromValue(int value) {
        return switch (value) {
            case 0 -> NONE;
            case 1 -> DISCONNECTED;
            case 2 -> CONNECTED;
            case 4 -> LOGGED_IN;
            case 8 -> CONNECTING;
            case 16 -> LOGGING_IN;
            case 32 -> DISCONNECTING;
            default -> new SoulseekClientStates(value);
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
    public boolean hasFlag(SoulseekClientStates state) {
        Objects.requireNonNull(state, "state");
        return (value & state.value) == state.value;
    }

    /**
     * Combines this state with another state.
     *
     * @param state the state to combine
     * @return the combined state
     */
    public SoulseekClientStates or(SoulseekClientStates state) {
        Objects.requireNonNull(state, "state");
        return fromValue(value | state.value);
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof SoulseekClientStates states && value == states.value;
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

    private void addName(List<String> names, SoulseekClientStates state, String name) {
        if (hasFlag(state)) {
            names.add(name);
        }
    }
}
