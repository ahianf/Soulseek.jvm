// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal;

/**
 * The result of a C#-style cache {@code TryGet} or {@code TryRemove}
 * operation.
 *
 * <p>The explicit {@code found} flag preserves the distinction between an
 * absent entry and a present entry whose value is {@code null}.
 *
 * @param found whether the cache contained the requested entry
 * @param value the cached value, which may be {@code null}
 * @param <T> the cached value type
 */
public record CacheLookupResult<T>(boolean found, T value) {
    /**
     * Creates an absent result.
     *
     * @param <T> the cached value type
     * @return the absent result
     */
    public static <T> CacheLookupResult<T> notFound() {
        return new CacheLookupResult<>(false, null);
    }

    /**
     * Creates a found result.
     *
     * @param value the cached value
     * @param <T> the cached value type
     * @return the found result
     */
    public static <T> CacheLookupResult<T> found(T value) {
        return new CacheLookupResult<>(true, value);
    }
}
