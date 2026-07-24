// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.common;

/**
 * Generates sequential tokens for network operations.
 */
public final class TokenFactory {
    private int current;

    /**
     * Creates a token factory starting at zero.
     */
    public TokenFactory() {
        this(0);
    }

    /**
     * Creates a token factory.
     *
     * @param start the first token to return
     */
    public TokenFactory(int start) {
        current = start;
    }

    /**
     * Returns the next token.
     *
     * <p>The operation is thread-safe and rolls over from
     * {@link Integer#MAX_VALUE} to zero.
     *
     * @return the next token
     */
    public synchronized int nextToken() {
        int result = current;
        current = current == Integer.MAX_VALUE ? 0 : current + 1;
        return result;
    }
}
