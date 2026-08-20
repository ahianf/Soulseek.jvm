// SPDX-FileCopyrightText: JP Dillingham
// SPDX-FileCopyrightText: 2026 Ahian Fernandez
// SPDX-License-Identifier: GPL-3.0-only

package dev.slsk.internal.transfer;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/** A snapshot of a single file transfer. */
public record Transfer(
        TransferDirection direction,
        String username,
        String filename,
        int token,
        TransferState state,
        long size,
        long startOffset,
        long bytesTransferred,
        double averageSpeed,
        Instant startTime,
        Instant endTime,
        Integer remoteToken,
        InetSocketAddress ipEndpoint,
        Throwable exception,
        long bytesRemaining,
        Duration elapsedTime,
        double percentComplete,
        Duration remainingTime) {

    public Transfer {
        direction = Objects.requireNonNull(direction, "direction");
        state = Objects.requireNonNull(state, "state");
    }

    public Transfer(
            TransferDirection direction,
            String username,
            String filename,
            int token,
            TransferState state,
            long size,
            long startOffset,
            long bytesTransferred,
            double averageSpeed,
            Instant startTime,
            Instant endTime,
            Integer remoteToken,
            InetSocketAddress ipEndpoint,
            Throwable exception) {
        this(
                direction,
                username,
                filename,
                token,
                state,
                size,
                startOffset,
                bytesTransferred,
                averageSpeed,
                startTime,
                endTime,
                remoteToken,
                ipEndpoint,
                exception,
                size - bytesTransferred,
                startTime == null ? null : Duration.between(startTime, endTime == null ? Instant.now() : endTime),
                size == 0 ? 0 : (bytesTransferred / (double) size) * 100,
                averageSpeed == 0 ? null : durationFromSeconds((size - bytesTransferred) / averageSpeed));
    }

    /** Converts a finite second count to a nanosecond-precision duration. */
    public static Duration durationFromSeconds(double seconds) {
        if (!Double.isFinite(seconds)) {
            throw new IllegalArgumentException("seconds must be finite: " + seconds);
        }
        if (seconds >= Long.MAX_VALUE || seconds <= Long.MIN_VALUE) {
            throw new ArithmeticException("seconds exceed Duration's range: " + seconds);
        }
        long wholeSeconds = (long) seconds;
        long nanoseconds = (long) ((seconds - wholeSeconds) * 1_000_000_000);
        return Duration.ofSeconds(wholeSeconds, nanoseconds);
    }
}
