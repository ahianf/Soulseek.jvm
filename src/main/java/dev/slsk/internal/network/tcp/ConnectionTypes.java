// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.network.tcp;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** A bitwise combination of TCP connection traits. */
public final class ConnectionTypes {
    /** No connection trait. */
    public static final ConnectionTypes NONE = new ConnectionTypes(0);
    /** An outbound connection. */
    public static final ConnectionTypes OUTBOUND = new ConnectionTypes(1);
    /** An inbound connection. */
    public static final ConnectionTypes INBOUND = new ConnectionTypes(2);
    /** A direct connection. */
    public static final ConnectionTypes DIRECT = new ConnectionTypes(4);
    /** An indirect connection. */
    public static final ConnectionTypes INDIRECT = new ConnectionTypes(8);

    private final int value;

    private ConnectionTypes(int value) {
        this.value = value;
    }

    /** Creates a connection type from a bit mask. */
    public static ConnectionTypes fromValue(int value) {
        return switch (value) {
            case 0 -> NONE;
            case 1 -> OUTBOUND;
            case 2 -> INBOUND;
            case 4 -> DIRECT;
            case 8 -> INDIRECT;
            default -> new ConnectionTypes(value);
        };
    }

    /** Returns the bit mask. */
    public int getValue() {
        return value;
    }

    /** Returns whether all supplied bits are present. */
    public boolean hasFlag(ConnectionTypes type) {
        Objects.requireNonNull(type, "type");
        return (value & type.value) == type.value;
    }

    /** Combines this value with another connection trait. */
    public ConnectionTypes or(ConnectionTypes type) {
        Objects.requireNonNull(type, "type");
        return fromValue(value | type.value);
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof ConnectionTypes types && value == types.value;
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
        addName(names, OUTBOUND, "OUTBOUND");
        addName(names, INBOUND, "INBOUND");
        addName(names, DIRECT, "DIRECT");
        addName(names, INDIRECT, "INDIRECT");
        return names.isEmpty() ? Integer.toString(value) : String.join(" | ", names);
    }

    private void addName(List<String> names, ConnectionTypes type, String name) {
        if (hasFlag(type)) {
            names.add(name);
        }
    }
}
