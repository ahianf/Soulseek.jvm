// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.transfer;

import java.util.Objects;

/**
 * Identifies one transfer for as long as the library remembers it.
 *
 * <p>Every command on a transfer is {@code (id, intent)}: {@code pause(id)},
 * {@code cancel(id)}, {@code retry(id)}. The consumer holds the id across an
 * HTTP round trip and hands it back, which a live transfer object could not
 * survive.
 *
 * <p>An id identifies an <em>enqueue</em>, not a file. Asking for the same file
 * from the same peer twice produces two transfers with two ids, and the second
 * does not inherit the first's history. That is why the identity is minted by
 * the library rather than derived from {@code (user, path)}: a derived key
 * silently aliases a re-download onto the record of the download before it, and
 * every side table the consumer keeps goes stale at exactly that moment.
 *
 * <p>Treat the value as opaque. It is not a protocol token, it means nothing to
 * a peer, and its format is not part of the contract.
 *
 * @param value the identifier
 */
public record TransferId(String value) {

    /**
     * Validates and returns the identifier.
     *
     * @throws NullPointerException if {@code value} is {@code null}
     * @throws IllegalArgumentException if {@code value} is blank
     */
    public TransferId {
        Objects.requireNonNull(value, "value");
        if (value.isBlank()) {
            throw new IllegalArgumentException("transfer id must not be blank");
        }
    }

    /**
     * Returns the identifier for {@code value}.
     *
     * @param value the identifier
     * @return the wrapped identifier
     */
    public static TransferId of(String value) {
        return new TransferId(value);
    }

    @Override
    public String toString() {
        return value;
    }
}
