// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.common;

import dev.slsk.internal.concurrent.CancellationSignal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;

/**
 * Shared internal utility operations.
 */
public final class CommonUtils {
    private CommonUtils() {}

    /**
     * Substitutes an uncancellable signal when the caller supplied none.
     *
     * <p>Every write to the network takes a signal and most callers have none
     * to give, so the substitution is made once here rather than at each of
     * them.
     *
     * @param cancellationSignal the caller's signal, possibly {@code null}
     * @return a signal, never {@code null}
     */
    public static CancellationSignal token(CancellationSignal cancellationSignal) {
        return cancellationSignal == null ? CancellationSignal.none() : cancellationSignal;
    }

    /**
     * Reports whether a value is null, empty, or consists only of whitespace.
     *
     * <p>{@code String.isBlank()} classifies fewer code points as whitespace.
     * Testing {@code Character.isWhitespace} together with {@code
     * Character.isSpaceChar} also covers non-breaking separators.
     *
     * @param value the value to test
     * @return {@code true} when the value is null, empty, or entirely whitespace
     */
    public static boolean isNullOrUnicodeWhitespace(String value) {
        return value == null
                || value.isEmpty()
                || value.codePoints()
                        .allMatch(codePoint -> Character.isWhitespace(codePoint) || Character.isSpaceChar(codePoint));
    }

    /**
     * Requires a value that is not null, empty, or entirely whitespace.
     *
     * @param value the value to check
     * @param name the argument name, for the message
     * @throws IllegalArgumentException when the value is blank
     */
    public static void requireText(String value, String name) {
        if (isNullOrUnicodeWhitespace(value)) {
            throw new IllegalArgumentException(name + " must not be null, empty, or whitespace");
        }
    }

    /**
     * Requires a value that is neither null nor empty.
     *
     * <p>Deliberately weaker than {@link #requireText}: a password of spaces is
     * a password, and a message of spaces is a message.
     *
     * @param value the value to check
     * @param name the argument name, for the message
     * @throws IllegalArgumentException when the value is null or empty
     */
    public static void requireNonEmpty(String value, String name) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be null or empty");
        }
    }

    /**
     * Removes and closes every queued value.
     *
     * @param queue the queue to drain
     * @param <T> the closeable value type
     */
    public static <T extends AutoCloseable> void dequeueAndCloseAll(Queue<T> queue) {
        Objects.requireNonNull(queue, "queue");

        T value;
        while ((value = queue.poll()) != null) {
            close(value);
        }
    }

    /**
     * Removes and closes every map value.
     *
     * @param map the map to drain
     * @param <K> the key type
     * @param <V> the closeable value type
     */
    public static <K, V extends AutoCloseable> void removeAndCloseAll(Map<K, V> map) {
        Objects.requireNonNull(map, "map");

        while (!map.isEmpty()) {
            Map.Entry<K, V> entry = map.entrySet().iterator().next();
            if (map.remove(entry.getKey(), entry.getValue())) {
                close(entry.getValue());
            }
        }
    }

    /**
     * Returns the lowercase MD5 hash of a UTF-8 string.
     *
     * @param value the value to hash
     * @return the 32-character hexadecimal hash
     */
    public static String toMd5Hash(String value) {
        Objects.requireNonNull(value, "value");

        try {
            byte[] digest = MessageDigest.getInstance("MD5").digest(value.getBytes(StandardCharsets.UTF_8));
            return java.util.HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("The Java runtime does not provide MD5", exception);
        }
    }

    private static void close(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (RuntimeException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }
}
