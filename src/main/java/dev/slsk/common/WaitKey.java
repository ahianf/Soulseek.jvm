// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.common;

import java.util.Objects;
import java.util.StringJoiner;

/**
 * Uniquely identifies a pending correlated wait.
 */
final class WaitKey {
    private final String token;
    private final Object[] tokenParts;

    /**
     * Creates a wait key from its ordered parts.
     *
     * @param tokenParts the parts that make up the key
     */
    WaitKey(Object... tokenParts) {
        this.tokenParts = Objects.requireNonNull(tokenParts, "tokenParts");
        StringJoiner joiner = new StringJoiner(":");
        for (Object part : tokenParts) {
            joiner.add(part == null ? "" : part.toString());
        }
        token = joiner.toString();
    }

    /**
     * Returns the joined wait token.
     *
     * @return the token
     */
    String getToken() {
        return token;
    }

    /**
     * Returns the original parts array.
     *
     * @return the token parts
     */
    Object[] getTokenParts() {
        return tokenParts;
    }

    @Override
    public boolean equals(Object object) {
        if (object == null) {
            throw new NullPointerException("other");
        }
        if (!(object instanceof WaitKey other)) {
            return false;
        }
        return token.equals(other.token);
    }

    @Override
    public int hashCode() {
        return token.isEmpty() ? 0 : token.hashCode();
    }

    @Override
    public String toString() {
        return token;
    }
}
