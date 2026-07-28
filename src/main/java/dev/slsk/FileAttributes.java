// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * What a peer said about a file, as typed accessors over the raw protocol map.
 *
 * <p>The raw map is kept rather than flattened into fields because clients
 * disagree about which attributes they send and what they mean by them, and a
 * fixed set of fields silently discards anything unexpected. Keeping the map
 * means a peer sending something this library does not model is not losing data,
 * and the accessors give the common cases a name.
 *
 * @param raw the attributes exactly as they arrived
 */
public record FileAttributes(Map<FileAttributeType, Integer> raw) {

    /** Validates and returns the attributes. */
    public FileAttributes {
        raw = Map.copyOf(Objects.requireNonNull(raw, "raw"));
    }

    /** Returns attributes with nothing in them. */
    public static FileAttributes none() {
        return new FileAttributes(Map.of());
    }

    /**
     * Returns the bit rate in kilobits per second.
     *
     * @return the bit rate, if the peer said
     */
    public OptionalInt bitrate() {
        return value(FileAttributeType.BIT_RATE);
    }

    /**
     * Returns how long the file plays for.
     *
     * @return the duration, if the peer said
     */
    public Optional<Duration> duration() {
        OptionalInt seconds = value(FileAttributeType.LENGTH);
        return seconds.isPresent() ? Optional.of(Duration.ofSeconds(seconds.getAsInt())) : Optional.empty();
    }

    /**
     * Returns whether the file is variable bit rate.
     *
     * @return {@code true} if the peer said it is
     */
    public boolean variableBitRate() {
        OptionalInt flag = value(FileAttributeType.VARIABLE_BIT_RATE);
        return flag.isPresent() && flag.getAsInt() != 0;
    }

    /**
     * Returns the sample rate in hertz.
     *
     * @return the sample rate, if the peer said
     */
    public OptionalInt sampleRate() {
        return value(FileAttributeType.SAMPLE_RATE);
    }

    /**
     * Returns the bit depth.
     *
     * @return the bit depth, if the peer said
     */
    public OptionalInt bitDepth() {
        return value(FileAttributeType.BIT_DEPTH);
    }

    private OptionalInt value(FileAttributeType type) {
        Integer value = raw.get(type);
        return value == null ? OptionalInt.empty() : OptionalInt.of(value);
    }
}
