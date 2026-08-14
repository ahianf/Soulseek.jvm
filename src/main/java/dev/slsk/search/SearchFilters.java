// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.search;

import java.util.Objects;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Which responses to keep.
 *
 * <p>Applied by the library as responses arrive, so a filtered-out file never
 * reaches a listener and never counts toward the response limits. Filtering
 * afterwards in the consumer would work but wastes the limit on files the
 * consumer was always going to discard.
 *
 * @param minBitrate keep only files at or above this bit rate
 * @param minSize keep only files at or above this size in bytes
 * @param maxSize keep only files at or below this size in bytes
 * @param excludeLocked drop files the peer reserves for privileged users
 * @param requiredExtensions keep only these extensions, lowercased and without
 *     the dot; empty means keep every extension
 */
public record SearchFilters(
        OptionalInt minBitrate,
        OptionalLong minSize,
        OptionalLong maxSize,
        boolean excludeLocked,
        Set<String> requiredExtensions) {

    private static final SearchFilters NONE =
            new SearchFilters(OptionalInt.empty(), OptionalLong.empty(), OptionalLong.empty(), false, Set.of());

    /** Validates and returns the filters. */
    public SearchFilters {
        Objects.requireNonNull(minBitrate, "minBitrate");
        Objects.requireNonNull(minSize, "minSize");
        Objects.requireNonNull(maxSize, "maxSize");
        requiredExtensions = Set.copyOf(Objects.requireNonNull(requiredExtensions, "requiredExtensions"));
    }

    /** Returns filters that keep everything. */
    public static SearchFilters none() {
        return NONE;
    }

    /**
     * Returns whether a file passes.
     *
     * @param file the file
     * @param locked whether the peer reserves it
     * @return {@code true} if it should be kept
     */
    public boolean accepts(SearchFile file, boolean locked) {
        Objects.requireNonNull(file, "file");
        if (locked && excludeLocked) {
            return false;
        }
        if (minSize.isPresent() && file.size() < minSize.getAsLong()) {
            return false;
        }
        if (maxSize.isPresent() && file.size() > maxSize.getAsLong()) {
            return false;
        }
        if (minBitrate.isPresent()) {
            OptionalInt bitrate = file.attributes().bitrate();
            if (bitrate.isEmpty() || bitrate.getAsInt() < minBitrate.getAsInt()) {
                return false;
            }
        }
        return requiredExtensions.isEmpty() || requiredExtensions.contains(file.extension());
    }
}
